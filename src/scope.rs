//! This module defines a utility for spawning non-static jobs. For more
//! information see [`crate::scope()`] or the [`Scope`] type.

use alloc::boxed::Box;
use core::any::Any;
use core::future::Future;
use core::marker::PhantomData;
use core::ptr;

use async_task::Runnable;
use async_task::Task;
use scope_ptr::ScopePtr;

use crate::ThreadPool;
use crate::job::HeapJob;
use crate::job::JobRef;
use crate::latch::Latch;
use crate::platform::*;
use crate::thread_pool::Worker;
use crate::unwind;
use crate::unwind::AbortOnDrop;

// -----------------------------------------------------------------------------
// Scope

/// A scope which can spawn a number of non-static jobs and async tasks.
pub struct Scope<'scope, 'env: 'scope> {
    /// Number of active references to the scope (including the owning
    /// allocation). This is incremented each time a new `ScopePtr` is created,
    /// and decremented when a `ScopePtr` is dropped or the owning thead is done
    /// using it.
    count: AtomicU32,
    /// A latch used to communicate when the scope has been completed.
    completed: Latch,
    /// If any job panics, we store the result here to propagate it.
    panic: AtomicPtr<Box<dyn Any + Send + 'static>>,
    /// Makes `Scope` invariant over 'scope
    _scope: PhantomData<&'scope mut &'scope ()>,
    /// Makes `Scope` invantiant over 'env
    _env: PhantomData<&'env mut &'env ()>,
}

/// Crates a new scope on a worker. [`Worker::scope`] is just an alias for this
/// function.
#[inline]
pub fn with_scope<'env, F, T>(worker: &Worker, f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    let abort_guard = AbortOnDrop;
    // SAFETY: The scope is never moved or mutably referenced. The scope is only
    // dropped at the end of this function, after the call to `complete`. The
    // abort guard above prevents the stack from being dropped early during a
    // panic unwind.
    let scope = unsafe { Scope::new(worker) };
    // Panics that occur within the closure should be caught and propagated once
    // all spawned work is complete. This is not a safety requirement, it's just
    // a nicer behavior than aborting.
    let result = match unwind::halt_unwinding(|| f(&scope)) {
        Ok(value) => Some(value),
        Err(err) => {
            scope.store_panic(err);
            None
        }
    };
    // Now that the user has (presuamably) spawnd some work onto the scope, we
    // must wait for it to complete.
    //
    // SAFETY: This is called only once, and we provide the same worker used to
    // create the scope.
    unsafe { scope.complete(worker) };
    // At this point all work on the scope is complete, so it is safe to drop
    // the scope. This also means we can relinquish our abort guard (returning
    // to the normal panic behavior).
    core::mem::forget(abort_guard);
    // If the closure or any spawned work did panic, we can now panic.
    scope.maybe_propagate_panic();
    // Otherwise return the result of evaluating the closure.
    result.unwrap()
}

impl<'scope, 'env> Scope<'scope, 'env> {
    /// Creates a new scope
    ///
    /// # Safety
    ///
    /// The caller must promise not to move or mutably reference this scope
    /// until it is dropped, and must not allow the scope to be dropped until
    /// after `Scope::complete` is run and returns.
    unsafe fn new(worker: &Worker) -> Scope<'scope, 'env> {
        Scope {
            count: AtomicU32::new(1),
            completed: worker.new_latch(),
            panic: AtomicPtr::new(ptr::null_mut()),
            _scope: PhantomData,
            _env: PhantomData,
        }
    }

    /// Spawns a scoped job onto the local worker. This job will execute
    /// sometime before the scope completes.
    ///
    /// # Returns
    ///
    /// Nothing. The spawned closures cannot pass back values to the caller
    /// directly, though they can write to local variables on the stack (if
    /// those variables outlive the scope) or communicate through shared
    /// channels.
    ///
    /// If you need to return a value, spawn a `Future` instead with
    /// [`Scope::spawn_future_on`].
    ///
    pub fn spawn_on<F>(&self, worker: &Worker, f: F)
    where
        F: FnOnce(&Worker) + Send + 'scope,
    {
        // Create a job to execute the spawned function in the scope.
        let scope_ptr = ScopePtr::new(self);
        let job = HeapJob::new(move |worker| {
            // Catch any panics and store them on the scope.
            let result = unwind::halt_unwinding(|| f(worker));
            if let Err(err) = result {
                scope_ptr.store_panic(err);
            };
            drop(scope_ptr);
        });

        // SAFETY: We must ensure that the heap job does not outlive the data it
        // closes over. In effect, this means it must not outlive `'scope`.
        //
        // This is ensured by the `scope_ptr` and the scope rules, which will
        // keep the calling stack frame alive until this job completes,
        // effectively extending the lifetime of `'scope` for as long as is
        // nessicary.
        let job_ref = unsafe { job.into_job_ref() };

        // Send the job to a queue to be executed.
        worker.enqueue(job_ref);
    }

    /// Spawns a future onto the scope. This future will be asynchronously
    /// polled to completion some time before the scope completes.
    ///
    /// # Returns
    ///
    /// This returns a task, which represents a handle to the async computation
    /// and is itself a future that can be awaited to receive the output of the
    /// future. There's four ways to interact with a task:
    ///
    /// 1. Await the task. This will eventually produce the output of the
    ///    provided future. The scope will not complete until the output is
    ///    returned to the awaiting logic.
    ///
    /// 2. Drop the task. This will stop execution of the future and potentially
    ///    allow the scope to complete immediately.
    ///
    /// 3. Cancel the task. This has the same effect as dropping the task, but
    ///    waits until the futures stops running (which in the worst-case means
    ///    waiting for the scope to complete).
    ///
    /// 4. Detach the task. This will allow the future to continue executing
    ///    even after the task itself is dropped. The scope will only complete
    ///    after the future polls to completion. Detaching a task with an
    ///    infinite loop will prevent the scope from completing, and is not
    ///    recommended.
    ///
    pub fn spawn_future_on<F, T>(&self, thread_pool: &'static ThreadPool, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'scope,
        T: Send,
    {
        // Embed the scope pointer into the future.
        let scope_ptr = ScopePtr::new(self);
        let future = async move {
            let result = future.await;
            drop(scope_ptr);
            result
        };

        // The schedule function will turn the future into a job when woken.
        let schedule = move |runnable: Runnable| {
            // Turn the runnable into a job-ref that we can send to a worker.

            // SAFETY: We provide a pointer to a non-null runnable, and we turn
            // it back into a non-null runnable. The runnable will remain valid
            // until the task is run.
            let job_ref = unsafe {
                JobRef::new_raw(runnable.into_raw(), |this, _| {
                    let runnable = Runnable::<()>::from_raw(this);
                    // Poll the task.
                    runnable.run();
                })
            };

            // Send this job off to be executed. When this schedule function is
            // called on a worker thread this re-schedules it onto the worker's
            // local queue, which will generally cause tasks to stick to the
            // same thread instead of jumping around randomly. This is also
            // faster than injecting into the global queue.
            thread_pool.with_worker(|worker| {
                worker.enqueue(job_ref);
            });
        };

        // SAFETY: We must ensure that the runnable does not outlive the data it
        // closes over. In effect, this means it must not outlive `'scope`.
        //
        // This is ensured by the `scope_ptr` and the scope rules, which will
        // keep the calling stack frame alive until the runnable is dropped,
        // effectively extending the lifetime of `'scope` for as long as is
        // nessicary.
        //
        // We have to use `spawn_unchecked` here instead of `spawn` because the
        // future is non-static.
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };

        // Call the schedule function once to create the initial job.
        runnable.schedule();

        // Return the task handle.
        task
    }

    /// Adds an additional reference to the scope's reference counter.
    ///
    /// Every call to this should have a matching call to
    /// `Scope::remove_reference`, or the scope will block forever on
    /// completion.
    fn add_reference(&self) {
        let counter = self.count.fetch_add(1, Ordering::Release);
        tracing::trace!("scope reference counter increased to {}", counter + 1);
    }

    /// Removes a reference from the scope's reference counter.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there is exactly one a matching call to
    /// `add_reference` for every call to this function, unless used within
    /// `Scope::complete`.
    unsafe fn remove_reference(&self) {
        let counter = self.count.fetch_sub(1, Ordering::Acquire);
        tracing::trace!("scope reference counter decreased to {}", counter - 1);
        if counter == 1 {
            // Alerts the owning thread that the scope has completed.
            //
            // This should never panic, because the counter can only go to zero
            // once, when the scope has been dropped and all work has been
            // completed.
            //
            // SAFETY: The signal is passed as a reference, and is live for the
            // duration of the function.
            unsafe { Latch::set(&self.completed) };
        }
    }

    /// Stores a panic so that it can be propagated when the scope is complete.
    /// If called multiple times, only the first panic is stored, and the
    /// remainder are dropped.
    fn store_panic(&self, err: Box<dyn Any + Send + 'static>) {
        if self.panic.load(Ordering::Relaxed).is_null() {
            let nil = ptr::null_mut();
            let err_ptr = Box::into_raw(Box::new(err));
            if self
                .panic
                .compare_exchange(nil, err_ptr, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                // Ownership is now transferred into the panic field.
            } else {
                // Another panic raced in ahead of us, so we need to drop this one.
                //
                // SAFETY: This was created by `Box::into_raw` just above. It is
                // possible that this will panic, because it's a `Box<dyn Any>`,
                // however in the worst case this will simply trigger the
                // scope's abort guard, causing an abort rather than UB.
                let _: Box<_> = unsafe { Box::from_raw(err_ptr) };
            }
        }
    }

    /// Propagates any panic captured while the scope was executing.
    fn maybe_propagate_panic(&self) {
        let panic = self.panic.swap(ptr::null_mut(), Ordering::Relaxed);
        if !panic.is_null() {
            // SAFETY: This was created by `Box::into_raw` in `store_panic` and,
            // because of the atomic swap just above, is only called once for
            // each box.
            let value = unsafe { Box::from_raw(panic) };
            unwind::resume_unwinding(*value);
        }
    }

    /// Waits for the scope to complete.
    ///
    /// # Safety
    ///
    /// This must be called only once. This must be called with a reference to
    /// the same worker the scope was created with.
    unsafe fn complete(&self, worker: &Worker) {
        // SAFETY: This is explicitly allowed, because every scope starts off
        // with a counter of 1. Because this is called only once, the following
        // should be the only call to `remove_reference` without a corresponding
        // call to `add_reference`.
        //
        // Only after the following call will the counter decrement to zero,
        // causing the signal to become set and allowing this function to
        // return.
        unsafe { self.remove_reference() };
        // Wait for the remaining work to complete.
        worker.wait_for(&self.completed);
    }
}

// -----------------------------------------------------------------------------
// Scope pointer

mod scope_ptr {
    //! Defines a "lifetime-erased" reference-counting pointer to a scope.

    use alloc::boxed::Box;
    use core::any::Any;

    use super::Scope;

    /// A reference-counted pointer to a scope. Used to capture a scope pointer
    /// in jobs without faking a lifetime. Holding a `ScopePtr` keeps the
    /// reference scope from being deallocated.
    pub struct ScopePtr<'scope, 'env>(*const Scope<'scope, 'env>);

    // SAFETY: !Send for raw pointers is not for safety, just as a lint.
    unsafe impl Send for ScopePtr<'_, '_> {}

    // SAFETY: !Sync for raw pointers is not for safety, just as a lint.
    unsafe impl Sync for ScopePtr<'_, '_> {}

    impl<'scope, 'env> ScopePtr<'scope, 'env> {
        /// Creates a new reference-counted scope pointer which can be sent to other
        /// threads.
        pub fn new(scope: &Scope<'scope, 'env>) -> ScopePtr<'scope, 'env> {
            // Add a reference to ensure the scope will stay alive at least
            // until this is dropped (which we will decrement the counter).
            scope.add_reference();
            ScopePtr(scope)
        }

        /// Stores a panic in the scope that can be resumed later.
        pub fn store_panic(&self, err: Box<dyn Any + Send + 'static>) {
            // SAFETY: This was created using an immutable scope reference, and
            // by the scope rules there can be no mutable references to this
            // scope, nor can the scope have been moved or deallocated while the
            // scope's counter remains incremented.
            let scope_ref = unsafe { &*self.0 };
            scope_ref.store_panic(err);
        }
    }

    impl Drop for ScopePtr<'_, '_> {
        fn drop(&mut self) {
            // SAFETY: This was created using an immutable scope reference, and
            // by the scope rules there can be no mutable references to this
            // scope, nor can the scope have been moved or deallocated while the
            // scope's counter remains incremented.
            let scope_ref = unsafe { &*self.0 };

            // Decrement the reference counter, possibly allowing
            // `Scope::complete` to return and the scope itself to be freed.
            //
            // SAFETY: We call `add_reference` in `ScopePtr::new`.
            unsafe { scope_ref.remove_reference() };
        }
    }
}

// -----------------------------------------------------------------------------
// Tests

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use core::sync::atomic::AtomicU8;
    use core::sync::atomic::Ordering;

    use crate::ThreadPool;
    use crate::scope;

    #[test]
    fn scoped_borrow() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.populate();

        let mut string = "a";
        THREAD_POOL.with_worker(|worker| {
            scope(|scope| {
                scope.spawn_on(worker, |_| {
                    string = "b";
                })
            });
        });
        assert_eq!(string, "b");
    }

    #[test]
    fn scoped_borrow_twice() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.populate();

        let mut string = "a";
        THREAD_POOL.with_worker(|worker| {
            scope(|scope| {
                scope.spawn_on(worker, |worker| {
                    string = "b";
                    scope.spawn_on(worker, |_| {
                        string = "c";
                    })
                })
            });
        });
        assert_eq!(string, "c");
    }

    #[test]
    fn scoped_concurrency() {
        const NUM_JOBS: u8 = 128;

        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to(4);

        let a = AtomicU8::new(0);
        let b = AtomicU8::new(0);

        THREAD_POOL.with_worker(|worker| {
            scope(|scope| {
                for _ in 0..NUM_JOBS {
                    scope.spawn_on(worker, |_| {
                        THREAD_POOL.join(
                            |_| a.fetch_add(1, Ordering::Relaxed),
                            |_| b.fetch_add(1, Ordering::Relaxed),
                        );
                    });
                }
            });
        });

        assert_eq!(a.load(Ordering::Relaxed), NUM_JOBS);
        assert_eq!(b.load(Ordering::Relaxed), NUM_JOBS);

        THREAD_POOL.depopulate();
    }
}
