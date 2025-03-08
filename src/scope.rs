//! This module defines an utility for spawning non-static jobs. For more
//! information see [`crate::scope`] or the [`Scope`] type.

use alloc::boxed::Box;
use core::future::Future;
use core::marker::PhantomData;
use core::marker::PhantomPinned;
use core::pin::Pin;

use async_task::Runnable;
use async_task::Task;
use scope_ptr::ScopePtr;

use crate::job::HeapJob;
use crate::job::JobRef;
use crate::platform::*;
use crate::signal::Signal;
use crate::thread_pool::Worker;

// -----------------------------------------------------------------------------
// Scope

/// A scope which can spawn a number of non-static jobs and async tasks. See
/// [`ThreadPool::scope`] for more information.
pub struct Scope<'scope> {
    /// Number of active references to the scope (including the owning
    /// allocation). This is incremented each time a new `ScopePtr` is created,
    /// and decremented when a `ScopePtr` or the `Scope` itself is dropped.
    count: AtomicU32,
    /// A signal used to communicate when the scope has been completed.
    signal: Signal,
    /// A marker that makes the scope behave as if it contained a vector of
    /// closures to execute, all of which outlive `'scope`. We pretend they are
    /// `Send + Sync` even though they're not actually required to be `Sync`.
    /// It's still safe to let the `Scope` implement `Sync` because the closures
    /// are only *moved* across threads to be executed.
    #[allow(clippy::type_complexity)]
    marker: PhantomData<Box<dyn FnOnce(&Scope<'scope>) + Send + Sync + 'scope>>,
    /// Opt out of Unpin behavior; this type requires strong pinning guaranties.
    _phantom: PhantomPinned,
}

impl<'scope> Scope<'scope> {
    /// Creates a new scope owned by the given worker thread. For a safe
    /// equivalent, use [`ThreadPool::scope`].
    ///
    /// Two important lifetimes effect scope: the external lifetime of the scope
    /// object itself (which we will call `'ext`) and the internal lifetime
    /// `'scope`.
    ///
    /// Before a scope can be used, it must be pinned. The recommended approach
    /// is to use the `pin!` macro to get a `Pin<&mut Scope<'scope>>` which you
    /// then convert into a `Pin<&Scope<'scope>>` via `into_ref`.
    pub(crate) fn new() -> Scope<'scope> {
        Scope {
            count: AtomicU32::new(1),
            signal: Signal::new(),
            marker: PhantomData,
            _phantom: PhantomPinned,
        }
    }

    /// Consumes the scope and blocks until all jobs spawned on it are complete.
    /// This is equivalent to dropping the scope.
    pub fn complete(self) {
        drop(self);
    }

    /// Spawns a job into the scope. This job will execute sometime before the
    /// scope completes. The job is specified as a closure, and this closure
    /// receives its own reference to the scope `self` as argument. This can be
    /// used to inject new jobs into `self`.
    ///
    /// # Returns
    ///
    /// Nothing. The spawned closures cannot pass back values to the caller
    /// directly, though they can write to local variables on the stack (if
    /// those variables outlive the scope) or communicate through shared
    /// channels.
    ///
    /// If you need to return a value, spawn a `Future` instead with
    /// [`Scope::spawn_future`].
    ///
    /// # See also
    ///
    /// The [`ThreadPool::scope`] function has more extensive documentation about
    /// task spawning.
    pub fn spawn<F>(self: Pin<&Self>, f: F)
    where
        F: FnOnce(Pin<&Scope<'scope>>) + Send + 'scope,
    {
        // Create a job to execute the spawned function in the scope.
        let scope_ptr = ScopePtr::new(self);
        let job = HeapJob::new(move |_| {
            scope_ptr.run(f);
        });

        // SAFETY: We must ensure that the heap job does not outlive the data is
        // closes over. In effect, this means it must not outlive `'scope`.
        //
        // The `'scope` will last until the scope is deallocated, which (due to
        // reference counting) will not be until after `scope_ptr` within the
        // heap job is dropped. So `'scope` should last at least until the heap
        // job is dropped.
        let job_ref = unsafe { job.into_job_ref() };

        // Send the job to a queue to be executed.
        Worker::with_current(|worker| {
            let worker = worker.unwrap();
            worker.queue.push_back(job_ref);
        });
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
    pub fn spawn_future<F, T>(self: Pin<&Self>, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'scope,
        T: Send + 'scope,
    {
        self.spawn_async(|_| future)
    }

    /// Spawns an async closure onto the scope. This future will be
    /// asynchronously polled to completion some time before the scope
    /// completes.
    ///
    /// Internally the closure is wrapped into a future and passed along to
    /// [`Scope::spawn_future`]. See the docs on that function for more
    /// information.
    pub fn spawn_async<Fn, Fut, T>(self: Pin<&Self>, f: Fn) -> Task<T>
    where
        Fn: FnOnce(Pin<&Scope<'scope>>) -> Fut + Send + 'scope,
        Fut: Future<Output = T> + Send + 'scope,
        T: Send + 'scope,
    {
        // Wrap the function into a future using an async block.
        let scope_ptr = ScopePtr::new(self);
        let future = async move { scope_ptr.run(f).await };

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
            Worker::with_current(|worker| {
                let worker = worker.unwrap();
                worker.queue.push_back(job_ref);
            });
        };

        // SAFETY: We must ensure that the runnable does not outlive the data is
        // closes over. In effect, this means it must not outlive `'scope`.
        //
        // The `'scope` will last until the scope is deallocated, which (due to
        // reference counting) will not be until after `scope_ptr` within the
        // future is dropped. The future will not be dropped until after the
        // runnable is dropped, so `'scope` should last at least until the
        // runnable is dropped.
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
    fn add_reference(&self) {
        let counter = self.count.fetch_add(1, Ordering::SeqCst);
        tracing::trace!("scope reference counter increased to {}", counter + 1);
    }

    /// Removes a reference from the scope's reference counter.
    fn remove_reference(&self) {
        let counter = self.count.fetch_sub(1, Ordering::SeqCst);
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
            unsafe { Signal::send(&self.signal, ()) };
        }
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        // When the scope is dropped, block to prevent deallocation until the
        // reference counter allows the scope to complete.
        tracing::trace!("completing scope");
        self.remove_reference();
        Worker::with_current(|worker| {
            let worker = worker.unwrap();
            worker.wait_for_signal(&self.signal);
        });
    }
}

// -----------------------------------------------------------------------------
// Scope pointer

mod scope_ptr {
    //! Defines a "lifetime-erased" reference-counting pointer to a scope.

    use core::pin::Pin;

    use super::Scope;

    /// A reference-counted pointer to a scope. Used to capture a scope pointer
    /// in jobs without faking a lifetime. Holding a `ScopePtr` keeps the
    /// reference scope from being deallocated.
    pub struct ScopePtr<'scope>(*const Scope<'scope>);

    // SAFETY: !Send for raw pointers is not for safety, just as a lint.
    unsafe impl Send for ScopePtr<'_> {}

    // SAFETY: !Sync for raw pointers is not for safety, just as a lint.
    unsafe impl Sync for ScopePtr<'_> {}

    impl<'scope> ScopePtr<'scope> {
        /// Creates a new reference-counted scope pointer which can be sent to other
        /// threads.
        pub fn new(scope: Pin<&Scope<'scope>>) -> ScopePtr<'scope> {
            scope.add_reference();
            ScopePtr(scope.get_ref())
        }

        /// Passes the scope referred to by this pointer into a closure.
        pub fn run<F, T>(&self, f: F) -> T
        where
            F: FnOnce(Pin<&Scope<'scope>>) -> T + 'scope,
        {
            // SAFETY: This pointer is convertible to a shared reference.
            //
            // + It was created from an immutable reference, and we never change
            //   it so it must be non-null and dereferenceable.
            //
            // + We incremented the scopes reference counter and will not
            //   decrement it until this pointer is dropped. Since the scope
            //   will remain valid as long as the reference counter is above
            //   zero, we know it is valid.
            //
            // + The scope is never accessed mutably, so creating shared
            //   references is allowed.
            //
            let inner_ref = unsafe { &*self.0 };

            // SAFETY: The scope was pinned when passed to `ScopePtr::new` so
            // the pinning rules must already be satisfied.
            let pinned_ref = unsafe { Pin::new_unchecked(inner_ref) };

            // Execute the closure on the shared reference.
            f(pinned_ref)
        }
    }

    impl Drop for ScopePtr<'_> {
        fn drop(&mut self) {
            // SAFETY: This pointer is convertible to a shared reference.
            //
            // + It was created from an immutable reference, and we never change
            //   it so it must be non-null and dereferenceable.
            //
            // + We incremented the scopes reference counter and we have not yet
            //   decremented it (although we are about to). Since the scope will
            //   remain valid as long as the reference counter is above zero, we
            //   know it is valid.
            //
            // + The scope is never accessed mutably, so creating shared
            //   references is allowed.
            //
            let scope_ref = unsafe { &*self.0 };

            // Decrement the reference counter, possibly allowing the scope to
            // complete.
            scope_ref.remove_reference();
        }
    }
}
