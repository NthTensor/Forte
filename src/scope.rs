use std::{future::Future, marker::PhantomData, ptr::NonNull};

use async_task::{Runnable, Task};

use crate::{
    job::{HeapJob, JobRef},
    latch::{CountLatch, Latch},
    thread_pool::{ThreadPool, WorkerThread},
    util::CallOnDrop,
};

// -----------------------------------------------------------------------------
// Scope

/// A scope which can spawn a number of non-static jobs and async tasks. See
/// [`Registry::scope`] for more information.
pub struct Scope<'scope> {
    /// The registry the scope operates on.
    thread_pool: &'static ThreadPool,
    /// A counting latch that opens when all jobs spawned in this scope are complete.
    job_completed_latch: CountLatch,
    /// A marker that makes the scope behave as if it contained a vector of
    /// closures to execute, all of which outlive `'scope`. We pretend they are
    /// `Send + Sync` even though they're not actually required to be `Sync`.
    /// It's still safe to let the `Scope` implement `Sync` because the closures
    /// are only *moved* across threads to be executed.
    #[allow(clippy::type_complexity)]
    marker: PhantomData<Box<dyn FnOnce(&Scope<'scope>) + Send + Sync + 'scope>>,
}

impl<'scope> Scope<'scope> {
    /// Creates a new scope owned by the given worker thread. For a safe
    /// equivalent, use [`Registry::scope`].
    ///
    /// Two important lifetimes effect scope: the external lifetime of the scope
    /// object itself (which we will call `'ext`) and the internal lifetime
    /// `'scope`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the scope is completed with a call to
    /// [`Scope::complete`], passing in a reference the same owning worker
    /// thread both times. 
    ///
    /// If the scope is not completed, jobs spawned onto this scope may outlive
    /// the data they close over.
    pub unsafe fn new(owner: &WorkerThread) -> Scope<'scope> {
        Scope {
            thread_pool: owner.thread_pool(),
            job_completed_latch: CountLatch::with_count(1, owner),
            marker: PhantomData,
        }
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
    /// The [`Registry::scope`] function has more extensive documentation about
    /// task spawning.
    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce(&Scope<'scope>) + Send + 'scope,
    {
        // We increment the scope counter; this will prevent the scope from
        // ending until after a corresponding `Latch::set` call.
        self.job_completed_latch.increment();

        // Create a job to execute the spawned function in the scope.
        let scope_ptr = ScopePtr(self);
        let job = HeapJob::new(move || {
            // SAFETY: Because we called `increment` and the owner is required
            // to call `complete`, this scope will remain valid at-least until
            // `Latch::set` is called.
            unsafe {
                let scope = scope_ptr.as_ref();
                f(scope);
                Latch::set(&self.job_completed_latch);
            }
        });

        // SAFETY: The heap job does not outlive `'scope`. This is ensured
        // because the owner of this scope is required to call
        // `Scope::complete`, and that function keeps the scope alive until the
        // latch is opened. The latch will not open until after this job is
        // executed, because the call to `increment` above is matched by the
        // call to `Latch::set` after execution.
        let job_ref = unsafe { job.into_job_ref() };

        // Send the job to a queue to be executed.
        self.thread_pool.inject_or_push(job_ref);
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
    pub fn spawn_future<F, T>(&self, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'scope,
        T: Send + 'scope,
    {
        // We increment the scope counter; this will prevent the scope from
        // ending until after a corresponding `Latch::set` call.
        self.job_completed_latch.increment();

        // The future is dropped when the task is completed or canceled. In
        // either case we have to decrement the scope job count. We inject this
        // logic into the future itself at the onset.
        //
        // A useful consequence of this approach is that the scope (and
        // therefore the latch) will remain valid at least until the last future
        // is dropped.
        let scope_ptr = ScopePtr(self);
        let future = async move {
            let _guard = CallOnDrop(move || {
                // SAFETY: Because we called `increment` and the owner is required
                // to call `complete`, this scope will remain valid at-least until
                // `Latch::set` is called.
                unsafe {
                    let scope = scope_ptr.as_ref();
                    Latch::set(&scope.job_completed_latch);
                }
            });
            future.await
        };

        // The schedule function will turn the future into a job when woken.
        let scope_ptr = ScopePtr(self);
        let schedule = move |runnable: Runnable| {
            // SAFETY: Because we called `increment` and the owner is required
            // to call `complete`, this scope will remain valid at-least until
            // `Latch::set` is called when the future is dropped.
            //
            // The future will not be dropped until after the runnable is
            // dropped, so the scope pointer must still be valid.
            let scope = unsafe { scope_ptr.as_ref() };

            // Now we turn the runnable into a job-ref that we can send to a
            // worker.
            
            // SAFETY: We provide a pointer to a non-null runnable, and we turn
            // it back into a non-null runnable. The runnable will remain valid
            // until the task is run.
            let job_ref = unsafe {
                JobRef::new_raw(
                    runnable.into_raw().as_ptr(),
                    |this| {
                        let this = NonNull::new_unchecked(this as *mut ());
                        let runnable = Runnable::<()>::from_raw(this);
                        // Poll the task.
                        runnable.run();
                    }
                )
            };
            
            // Send this job off to be executed. When this schedule function is
            // called on a worker thread this re-schedules it onto the worker's
            // local queue, which will generally cause tasks to stick to the
            // same thread instead of jumping around randomly. This is also
            // faster than injecting into the global queue.
            scope.thread_pool.inject_or_push(job_ref)
        };

        // SAFETY: We must ensure that the runnable and the waker do not outlive
        // `'scope`. This is ensured because the owner of this scope is
        // required to call `Scope::complete`. That function keeps the scope
        // alive until the latch is opened, and the latch will not open until
        // after the future is dropped, which can happen only after the runnable
        // and waker are dropped.
        //
        // We have to use `spawn_unchecked` here instead of `spawn` because the
        // future is non-static.
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) };
        // Call the schedule function once to create the initial job.
        runnable.schedule();
        task
    }

    /// Spawns an async closure onto the scope. This future will be
    /// asynchronously polled to completion some time before the scope
    /// completes.
    ///
    /// Internally the closure is wrapped into a future and passed along to
    /// [`Scope::spawn_future`]. See the docs on that function for more
    /// information.
    pub fn spawn_async<Fn, Fut, T>(&self, f: Fn) -> Task<T>
    where Fn: FnOnce(&Scope<'scope>) -> Fut + Send + 'static,
          Fut: Future<Output = T> + Send + 'static,
          T: Send + 'static
    {
        // Wrap the function into a future using an async block.
        let scope_ptr = ScopePtr(self);
        let future = async move {
            // SAFETY: The scope will be valid at least until this future is
            // dropped because of the drop guard in `spawn_future`.
            let scope = unsafe { scope_ptr.as_ref() };
            f(scope).await
        };
        // We just pass this future to `spawn_future`.
        self.spawn_future(future)
    }

    /// Consumes the scope and blocks until all jobs spawned on it are complete.
    pub fn complete(self, owner: &WorkerThread) {
        // SAFETY: The latch is valid until the scope is dropped at the end of
        // this function.
        unsafe { Latch::set(&self.job_completed_latch) };
        // Run the thread until the jobs are complete, then return.
        owner.run_until(&self.job_completed_latch);
    }
}

// -----------------------------------------------------------------------------
// Scope pointer

/// Used to capture a scope `&Self` pointer in jobs, without faking a lifetime.
///
/// Unsafe code is still required to dereference the pointer, but that's fine in
/// scope jobs that are guaranteed to execute before the scope ends.
struct ScopePtr<T>(*const T);

// SAFETY: !Send for raw pointers is not for safety, just as a lint
unsafe impl<T: Sync> Send for ScopePtr<T> {}

// SAFETY: !Sync for raw pointers is not for safety, just as a lint
unsafe impl<T: Sync> Sync for ScopePtr<T> {}

impl<T> ScopePtr<T> {
    // Helper to avoid disjoint captures of `scope_ptr.0`
    //
    // # Saftey
    //
    // Callers must ensure the scope pointer is still valid.
    unsafe fn as_ref(&self) -> &T {
        &*self.0
    }
}
