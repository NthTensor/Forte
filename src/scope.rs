//! This module defines a utility for spawning non-static jobs. For more
//! information see [`crate::scope()`] or the [`Scope`] type.

use alloc::boxed::Box;
use core::any::Any;
use core::cell::UnsafeCell;
use core::future::Future;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::ptr;
use core::ptr::NonNull;
use core::sync::atomic::fence;
use core::task::Context;
use core::task::Poll;
use core::task::RawWaker;
use core::task::RawWakerVTable;
use core::task::Waker;
use std::process::abort;

use scope_ptr::ScopePtr;

use crate::FnOnceMarker;
use crate::FutureMarker;
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

/// A scope which can spawn a number of non-static jobs and async tasks. Refer
/// to [`scope`](crate::scope()) for more extensive documentation.
///
/// # Lifetimes
///
/// A scope has two lifetimes: `'scope` and `'env`.
///
/// The `'scope` lifetime represents the lifetime of the scope itself. That is:
/// the time during which new scoped jobs may be spawned, and also the time
/// during which they might still be running. This lifetime starts within the
/// `scope()` function, before the closure `f` (the argument to `scope()`) is
/// executed. It ends after the closure `f` returns and after all scoped work is
/// complete, but before `scope()` returns.
///
/// The `'env` lifetime represents the lifetime of whatever is borrowed by the
/// scoped jobs. This lifetime must outlast the call to `scope()`, and thus
/// cannot be smaller than `'scope`. It can be as small as the call to
/// `scope()`, meaning that anything that outlives this call, such as local
/// variables defined right before the scope, can be borrowed by the scoped
/// jobs.
///
/// The `'env: 'scope` bound is part of the definition of the `Scope` type. The
/// requirement that scoped work outlive `'scope` is part of the definition of
/// the [`ScopedSpawn`] trait.
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
    /// This adds invariance over 'scope, to make sure 'scope cannot shrink,
    /// which is necessary for soundness.
    ///
    /// Without invariance, this would compile fine but be unsound:
    ///
    /// ```compile_fail
    /// # use forte::ThreadPool;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.resize_to_available();
    /// # THREAD_POOL.with_worker(|worker| {
    /// worker.scope(|scope| {
    ///     scope.spawn_on(worker, |worker: &Worker| {
    ///         let a = String::from("abcd");
    ///         scope.spawn_on(worker, |_: &Worker| println!("{a:?}")); // might run after `a` is dropped
    ///     });
    /// });
    /// # });
    /// ```
    _scope: PhantomData<&'scope mut &'scope ()>,
    /// This adds covariance over 'env.
    _env: PhantomData<&'env mut &'env ()>,
}

/// Executes a new scope on a worker. [`Worker::scope`],
/// [`ThreadPool::scope`][crate::ThreadPool::scope] and [`scope`][crate::scope()] are all just
/// an aliases for this function.
///
/// For details about the `'scope` and `'env` lifetimes see [`Scope`].
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
        Ok(result) => Some(result),
        Err(err) => {
            scope.store_panic(err);
            None
        }
    };
    // Now that the user has (presumably) spawned some work onto the scope, we
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
    // Return the result.
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

    /// Runs a closure or future sometime before the scope completes. Valid
    /// inputs to this method are:
    ///
    /// + A `for<'worker> FnOnce(&'worker Worker)` closure, with no return type.
    ///
    /// + A `Future<Output = ()>` future, with no return type.
    ///
    /// # Panics
    ///
    /// If not in a worker, this panics.
    pub fn spawn<M, S: ScopedSpawn<'scope, M>>(&'scope self, scoped_work: S) {
        Worker::with_current(|worker| scoped_work.spawn_on(worker.unwrap(), self));
    }

    /// Runs a closure or future sometime before the scope completes. Valid
    /// inputs to this method are:
    ///
    /// + A `for<'worker> FnOnce(&'worker Worker)` closure, with no return type.
    ///
    /// + A `Future<Output = ()>` future, with no return type.
    ///
    /// Unlike [`Scope::spawn`], this accepts the current worker as a parameter.
    pub fn spawn_on<M, S: ScopedSpawn<'scope, M>>(&'scope self, worker: &Worker, scoped_work: S) {
        scoped_work.spawn_on(worker, self);
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
            // SAFETY: The latch is passed as a reference, and is live for the
            // duration of the function.
            unsafe { Latch::set(&self.completed) };
        }
    }

    /// Stores a panic so that it can be propagated when the scope is complete.
    /// If called multiple times, only the first panic is stored, and the
    /// remainder are dropped.
    #[cold]
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
        // causing the latch to become set and allowing this function to
        // return.
        unsafe { self.remove_reference() };
        // Wait for the remaining work to complete.
        worker.wait_for(&self.completed);
    }
}

// -----------------------------------------------------------------------------
// Generalized scoped spawn trait

/// A trait for types that can be spawned onto a [`Scope`]. It is implemented for:
///
/// + Closures that satisfy `for<'worker> FnOnce(&'worker Worker) + Send + 'scope`.
///
/// + Futures that satisfy `Future<Output = ()> + Send + 'scope`.
///
/// Due to a bug in rustc, you may be given errors when using closures
/// with inferred types. If you encounter the following:
///
/// ```compile_fail
/// # use forte::ThreadPool;
/// # use forte::Worker;
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// THREAD_POOL.scope(|scope| {
///     scope.spawn(|_| { });
/// //              ^^^^^^^ the trait `ScopedSpawn<'_, _>` is not implemented for closure ...
/// });
/// ```
/// Try adding a type hint to the closure's parameters, like so:
/// ```
/// # use forte::ThreadPool;
/// # use forte::Worker;
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// THREAD_POOL.scope(|scope| {
///     scope.spawn(|_: &Worker| { });
/// });
/// ```
/// Hopefully rustc will fix this type inference failure eventually.
pub trait ScopedSpawn<'scope, M>: Send + 'scope {
    /// Spawns the value of self as scoped work on the provided worker.
    fn spawn_on<'env>(self, worker: &Worker, scope: &'scope Scope<'scope, 'env>);
}

impl<'scope, F> ScopedSpawn<'scope, FnOnceMarker> for F
where
    F: FnOnce(&Worker) + Send + 'scope,
{
    #[inline]
    fn spawn_on<'env>(self, worker: &Worker, scope: &'scope Scope<'scope, 'env>) {
        // Create a job to execute the spawned function in the scope.
        let scope_ptr = ScopePtr::new(scope);
        let job = HeapJob::new(move |worker| {
            // Catch any panics and store them on the scope.
            let result = unwind::halt_unwinding(|| self(worker));
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
        // necessary.
        let job_ref = unsafe { job.into_job_ref() };

        // Send the job to a queue to be executed.
        worker.enqueue(job_ref);
    }
}

impl<'scope, Fut> ScopedSpawn<'scope, FutureMarker> for Fut
where
    Fut: Future<Output = ()> + Send + 'scope,
{
    #[inline]
    fn spawn_on<'env, 'worker>(self, worker: &'worker Worker, scope: &'scope Scope<'scope, 'env>) {
        let poll_job = ScopeFutureJob::new(worker.thread_pool(), scope, self);
        let job_ref = poll_job.into_job_ref();
        worker.enqueue(job_ref);
    }
}

// The following is a small async executor built specifically to execute futures
// spawned onto scopes. It is one of the more complex (and unsafe) areas of
// Forte's code. Please take care when making changes.

/// This value is used for the state of future-jobs that are not currently being
/// polled or queued onto the thread pool. They may switch to the WOKEN state at
/// any time, after which they are queued.
const READY: u32 = 0;

/// This value is used for the state of future-jobs that have already been
/// woken. Jobs in this state may be in one of the three following categories:
///
/// + A pending job that has been (or is about to be) pushed to the queue
///   so that it can be polled.
///
/// + A pending job that is currently being polled (or has just finished) and
///   which was *not* queued after it was woken, because it was woken while
///   running.
///
/// + A job that was woken after it completed or panicked. These jobs will stay
///   in the WOKEN state forever, and will never be queued or polled again.
///
/// When a WOKEN future-job is executed by a worker, it switches into the LOCKED
/// state. When a job finished executing in the WOKEN state (indicating it was
/// woken while executing), and has not been completed or panicked, it is
/// queued, and remains in the WOKEN state.
///
/// Future-jobs should never move directly from WOKEN to READY.
const WOKEN: u32 = 1;

/// This value is used for the state of future-jobs that have not been woken and
/// are either executing, completed, or have been canceled due to a panic. They
/// may switch to the WOKEN state at any time, but are not queued to the pool
/// when this happens (they are instead queued when the future is done being
/// polled, assuming it has not pancaked or been completed).
///
/// When a job finished executing and has not been WOKEN, it switches back to
/// the READY state.
const LOCKED: u32 = 2;

/// This is a job that polls a future each time it is executed. The 'scope and
/// 'env lifetimes are the same as the ones from scope (see [`Scope`] for a
/// detailed explanation).
///
/// This struct is designed to live within an `Arc<T>` and be fully
/// reference-counted.
///
/// This type serves a dual purpose:
///
/// 1. It be converted into a job ref that can be queued onto the pool. When the
///    job executes, it polls the future.
///
/// 2. It can be converted into a `Waker`, which is provided to the future while
///    it is executing and which can be used to schedule a poll of the future
///    by queuing itself back on the pool.
///
/// The future is always queued on the thread on which it was woken. This allows
/// the waker to be converted directly into a job-ref, and may mean that the
/// cache is already primed.
struct ScopeFutureJob<'scope, 'env, Fut> {
    /// The future that the job exists to poll. This is stored in an unsafe cell
    /// to allow mutable access within an `Arc<T>`. The `state` field acts as a
    /// kind of mutex that ensures exclusive access by preventing the job from
    /// being queued or executed multiple times simultaneously.
    future: UnsafeCell<Fut>,
    /// A scope pointer. This allows the job to interact with the scope, and
    /// also keeps the scope alive until the job is dropped.
    scope_ptr: ScopePtr<'scope, 'env>,
    /// The thread pool this job is attached to.
    thread_pool: &'static ThreadPool,
    /// The state of the job, which is either READY, WOKEN, or LOCKED.
    state: AtomicU32,
}

impl<'scope, 'env, Fut> ScopeFutureJob<'scope, 'env, Fut>
where
    Fut: Future<Output = ()> + Send + 'scope,
{
    /// This vtable is part of what allows a `ScopedFutureJob` to act as an
    /// async task waker.
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_as_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_as_waker,
    );

    /// Creates a new `ScopedFutureJob` in an `Arc`. The caller is expected to
    /// immediately call `into_job_ref` and queue it on a worker to be polled.
    fn new(
        thread_pool: &'static ThreadPool,
        scope: &Scope<'scope, 'env>,
        future: Fut,
    ) -> Arc<Self> {
        let scope_ptr = ScopePtr::new(scope);
        Arc::new(Self {
            future: UnsafeCell::new(future),
            scope_ptr,
            thread_pool,
            // The job starts in the WOKEN state because we always queue it
            // after creating it.
            state: AtomicU32::new(WOKEN),
        })
    }

    /// Converts an `Arc<ScpedFutureJob>` into a job ref that can be queued on a
    /// thread pool. The ref-count is not decremented, ensuring that the job
    /// remains alive while this job ref exists.
    ///
    /// Forgetting this job ref will cause a memory leak.
    fn into_job_ref(self: Arc<Self>) -> JobRef {
        // SAFETY: Pointers created by `Arc::into_raw` are never null.
        let job_pointer = unsafe { NonNull::new_unchecked(Arc::into_raw(self).cast_mut().cast()) };

        // SAFETY: This pointer is an erased `Arc<Self>` which is what
        // `Self::poll` expects to receive.
        unsafe { JobRef::new_raw(job_pointer, Self::poll) }
    }

    /// This is what happens when the job is executed. It is this function that
    /// is in charge of actually polling the future, and it is therefore an
    /// extremely hot and performance sensitive function.
    fn poll(this: NonNull<()>, worker: &Worker) {
        // While we still have a raw pointer to the job, create a raw task waker
        // using our vtable.
        let raw_waker = RawWaker::new(this.as_ptr().cast_const(), &Self::VTABLE);

        // Create a new waker from the raw waker. This is *non-owning* and
        // functions like a `&Arc<Self>` rather than an `Arc<Self>`. We wrap it
        // in a `ManuallyDrop` to prevent the waker from calling `drop_as_waker`
        // through the vtable, which would cause the reference-count to
        // decrement (incorrectly).
        //
        // SAFETY: The api contract of RawWaker and RawWakerVTable is upheld by
        // the `Self::VTABLE` const.
        //
        // * The functions are all thread safe.
        //
        // * `clone` increments the reference count, ensuring that all resources
        //   needed to schedule the task are retained, and returns a waker that
        //   wakes the same task as the input.
        //
        // * `wake` converts the waker into a job ref that it queues for
        //   execution (and will be freed when completed unless closed again as a
        //   waker).
        //
        // * `wake_by_ref` uses a clone to create job ref, which will prevent
        //   resources from being freed.
        //
        // * `drop` decrements the reference count, potentially allowing the job
        //    to be freed.
        //
        let waker = unsafe { ManuallyDrop::new(Waker::from_raw(raw_waker)) };

        // SAFETY: This was created from an `Arc<Self>` in `into_job_ref`.
        //
        // Within this function, this pointer is only turned into an arc once,
        // so it can only be dropped once. Outside of this function, we are
        // forced to assume that the reference count is maintained correctly.
        let this = unsafe { Arc::from_raw(this.cast::<Self>().as_ptr()) };

        // If our implementation is correct, we should always be in the WOKEN
        // state at this point. To avoid potential UB, we double check that this
        // is the case, and abort the program if it is not.
        //
        // We use Acquire ordering here to ensure we have the current state of
        // the future. This synchronizes with the fence in the Poll::Pending
        // branch.
        if this.state.swap(LOCKED, Ordering::Acquire) != WOKEN {
            // We abort because this function needs to be panic safe.
            abort();
        }

        // At this point, we have acquired exclusive ownership of the future.

        // SAFETY: The arc never moves, and the future cannot be aliased mutably
        // elsewhere because this is the only place we access it, and no other
        // threads can have gotten past the memory swap above without causing an
        // abort.
        let future = unsafe { Pin::new_unchecked(&mut *this.future.get()) };

        // Create a new context from the waker, and poll the future.
        let mut cx = Context::from_waker(&waker);
        let result = unwind::halt_unwinding(|| future.poll(&mut cx));

        // Update the job state depending on the outcome of polling the future.
        match result {
            // The job completed without panicking.
            Ok(Poll::Ready(())) => {
                // Drop the job without rescheduling it, leaving it in the
                // LOCKED or WOKEN state so that it cannot be rescheduled.
            }
            // The job is still pending, and has not yet panicked.
            Ok(Poll::Pending) => {
                // The fence here ensures that our changes to the future become
                // visible to the next thread to execute the job and poll the
                // future.
                fence(Ordering::Release);
                // Try to set the state back back idle so other threads can
                // schedule it again. This will only fail if the job was woken
                // while running, and is already in the WOKEN state.
                //
                // If successful, this effectively releases our exclusive
                // ownership of the future.
                let rescheduled = this
                    .state
                    .compare_exchange(LOCKED, READY, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err();
                // If the job was woken while running, it should be queued
                // immediately. Conveniently, we know the state will already be
                // QUEUED, so we can leave it as it is.
                if rescheduled {
                    // This converts the local `Arc<Self>` into a job ref,
                    // preventing it from being dropped and potentially
                    // extending the job's lifetime.
                    let job_ref = this.into_job_ref();
                    worker.enqueue(job_ref);
                }
            }
            // The job panicked. Store the panic in the scope so it can be
            // resumed later.
            Err(err) => this.scope_ptr.store_panic(err),
        }

        // At this point, if we have not converted `this` into a job-ref and
        // queued it, it will be dropped. If no wakers for this task are being
        // held, then this will cause the reference counter to decrement to zero
        // and free the task.
        //
        // The alternative, if there are no wakers being held for the task, is
        // that the task will never wake and the scope will deadlock.
    }

    /// Creates a new `RawWaker` from the provided pointer.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    unsafe fn clone_as_waker(this: *const ()) -> RawWaker {
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance on of `Arc<Self`.
        unsafe { Arc::increment_strong_count(this.cast::<Self>()) };
        RawWaker::new(this, &Self::VTABLE)
    }

    /// Queues self to be executed and consumes the waker.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    unsafe fn wake(this: *const ()) {
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance on of `Arc<Self`.
        let this = unsafe { Arc::from_raw(this.cast::<Self>()) };

        if this.state.swap(WOKEN, Ordering::Relaxed) == READY {
            this.thread_pool.with_worker(|worker| {
                // Convert the waker into a job ref and queue it.
                let job_ref = this.into_job_ref();
                worker.enqueue(job_ref);
            });
        }
    }

    /// Queues self to be executed without consuming the waker.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    fn wake_by_ref(this: *const ()) {
        // We use manually drop here to prevent us from consuming the arc on
        // drop. This functions like an `&Arc<Self>` rather than an `Arc<Self>`.
        //
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance on of `Arc<Self`.
        let this = unsafe { ManuallyDrop::new(Arc::from_raw(this.cast::<Self>())) };

        if this.state.swap(WOKEN, Ordering::Relaxed) == READY {
            this.thread_pool.with_worker(|worker| {
                // Clone the waker, convert it into a job-ref and queue it.
                let this = ManuallyDrop::into_inner(this.clone());
                let job_ref = this.into_job_ref();
                worker.enqueue(job_ref);
            });
        }
    }

    /// Frees the waker.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    fn drop_as_waker(this: *const ()) {
        // Rather than converting back into an arc, we can just decrement the
        // counter here.
        //
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance on of `Arc<Self`.
        unsafe { Arc::decrement_strong_count(this.cast::<Self>()) };
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
    use core::iter::once;
    use core::pin::Pin;
    use core::sync::atomic::AtomicU8;
    use core::sync::atomic::AtomicUsize;
    use core::sync::atomic::Ordering;
    use core::task::Context;
    use core::task::Poll;
    use std::sync::Mutex;
    use std::vec;
    use std::vec::Vec;

    use crate::ThreadPool;
    use crate::Worker;
    use crate::scope;
    use crate::unwind;
    use crate::util::XorShift64Star;

    use super::Scope;

    /// Tests that empty scopes return properly.
    #[test]
    fn scope_empty() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|_: &Scope| {});

        THREAD_POOL.depopulate();
    }

    /// Tests that scopes return the output of the closure.
    #[test]
    fn scope_result() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut x = 0;
        THREAD_POOL.scope(|_: &Scope| {
            x = 22;
        });
        assert_eq!(x, 22);

        THREAD_POOL.depopulate();
    }

    /// Tests that multiple tasks in a scope run to completion.
    #[test]
    fn scope_two() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let counter = &AtomicUsize::new(0);
        THREAD_POOL.scope(|scope| {
            scope.spawn(move |_: &Worker| {
                counter.fetch_add(1, Ordering::SeqCst);
            });
            scope.spawn(move |_: &Worker| {
                counter.fetch_add(10, Ordering::SeqCst);
            });
        });

        let v = counter.load(Ordering::SeqCst);
        assert_eq!(v, 11);

        THREAD_POOL.depopulate();
    }

    /// Test that it is possible to borrow local data within a scope, modify it,
    /// and then read it later. This is mostly here to ensure stuff like this
    /// compiles.
    #[test]
    fn scope_borrow() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut string = "a";
        THREAD_POOL.scope(|scope| {
            scope.spawn(|_: &Worker| {
                string = "b";
            });
        });
        assert_eq!(string, "b");

        THREAD_POOL.depopulate();
    }

    /// Test that it is possible to borrow local data immutably within deeply
    /// nested scopes. This is also mostly here to ensure stuff like this
    /// compiles.
    #[test]
    fn scope_borrow_twice() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let counter = AtomicU8::new(0);
        THREAD_POOL.scope(|scope| {
            scope.spawn(|_: &Worker| {
                counter.fetch_add(1, Ordering::Relaxed);
                scope.spawn(|_: &Worker| {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            });
            scope.spawn(|worker: &Worker| {
                counter.fetch_add(1, Ordering::Relaxed);
                scope.spawn_on(worker, |_: &Worker| {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            });
        });
        assert_eq!(counter.load(Ordering::Relaxed), 4);

        THREAD_POOL.depopulate();
    }

    /// Tests that we can spawn futures onto the thraed pool and that they can
    /// borrow data as expected.
    #[test]
    fn scope_future() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut value = 0;
        THREAD_POOL.scope(|scope| {
            scope.spawn(async {
                value = 42;
            });
        });
        assert_eq!(value, 42);

        THREAD_POOL.depopulate();
    }

    /// This is a handy future that needs to be polled repeatedly before
    /// resolving.
    ///
    /// Each time it is polled, it wakes itself (so it will be polled again) and
    /// yields. It does this until it has been polled 128 times.
    ///
    /// This lets us test the behavior of scopes for sleeping tasks, to ensure
    /// we do not return from the scope while tasks are still pending.
    #[derive(Default)]
    struct CountFuture {
        /// The number of times the future has been polled.
        count: usize,
    }

    impl Future for CountFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.count == 128 {
                Poll::Ready(())
            } else {
                self.count += 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// Tests that we can spawn futures onto a scope, and that the scope really
    /// does poll wait for the future to complete before returning.
    #[test]
    fn scope_pending_future() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|scope| scope.spawn(CountFuture::default()));

        THREAD_POOL.depopulate();
    }

    /// Tests that a future that sleeps for a nontrivial amount of time does not
    /// cause the scope to exit early.
    #[test]
    fn scope_sleeping_future() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut complete = false;
        THREAD_POOL.scope(|scope| {
            scope.spawn(async {
                // Spawn a count future onto the thread pool, then have this
                // future sleep until that future is done.
                THREAD_POOL.spawn(CountFuture::default()).await;
                complete = true;
            })
        });

        assert!(complete);

        THREAD_POOL.depopulate();
    }

    /// Tests that blocking functions like `join` can be nested within scopes.
    #[test]
    fn scope_concurrency() {
        const NUM_JOBS: u8 = 128;

        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let a = AtomicU8::new(0);
        let b = AtomicU8::new(0);

        THREAD_POOL.with_worker(|worker| {
            scope(|scope| {
                for _ in 0..NUM_JOBS {
                    scope.spawn_on(worker, |_: &Worker| {
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

    #[test]
    fn scope_nesting() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut completed = false;

        THREAD_POOL.scope(|scope| {
            scope.spawn(|worker: &Worker| {
                worker.scope(|scope| {
                    scope.spawn(|_: &Worker| {
                        completed = true;
                    })
                })
            })
        });

        assert!(completed);

        THREAD_POOL.depopulate();
    }

    /// Tests that nesting two scopes on different workers will not deadlock.
    #[test]
    fn scope_nesting_new_worker() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut completed = false;

        THREAD_POOL.with_worker(|worker| {
            worker.scope(|scope| {
                scope.spawn_on(worker, |_: &Worker| {
                    // Creating a new worker instead of reusing the old one is
                    // bad form, but we may as well test it.
                    THREAD_POOL.with_worker(|worker| {
                        worker.scope(|scope| {
                            scope.spawn_on(worker, |_: &Worker| {
                                completed = true;
                            });
                        });
                    });
                });
            });
        });

        assert!(completed);

        THREAD_POOL.depopulate();
    }

    /// Tests that a serial and parallel version of a binary tree traversal
    /// produces the same output.
    #[test]
    fn scope_divide_and_conquer() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let counter_p = &AtomicUsize::new(0);
        THREAD_POOL.with_worker(|worker| {
            worker.scope(|scope| {
                scope.spawn(move |worker: &Worker| {
                    divide_and_conquer(worker, scope, counter_p, 1024)
                })
            });
        });

        let counter_s = &AtomicUsize::new(0);
        divide_and_conquer_seq(counter_s, 1024);

        let p = counter_p.load(Ordering::SeqCst);
        let s = counter_s.load(Ordering::SeqCst);
        assert_eq!(p, s);

        THREAD_POOL.depopulate();
    }

    fn divide_and_conquer<'scope, 'env>(
        worker: &Worker,
        scope: &'scope Scope<'scope, 'env>,
        counter: &'scope AtomicUsize,
        size: usize,
    ) {
        if size > 1 {
            scope.spawn_on(worker, move |worker: &Worker| {
                divide_and_conquer(worker, scope, counter, size / 2)
            });
            scope.spawn_on(worker, move |worker: &Worker| {
                divide_and_conquer(worker, scope, counter, size / 2)
            });
        } else {
            // count the leaves
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn divide_and_conquer_seq(counter: &AtomicUsize, size: usize) {
        if size > 1 {
            divide_and_conquer_seq(counter, size / 2);
            divide_and_conquer_seq(counter, size / 2);
        } else {
            // count the leaves
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Tests for correct scope completion on a deeply nested semi-random tree.
    #[test]
    fn scope_update_tree() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.with_worker(|_| {
            let mut tree = random_tree(10, 1337);
            let values: Vec<_> = tree.iter().cloned().collect();
            tree.update(|v| *v += 1);
            let new_values: Vec<usize> = tree.iter().cloned().collect();
            assert_eq!(values.len(), new_values.len());
            for (&i, &j) in values.iter().zip(&new_values) {
                assert_eq!(i + 1, j);
            }
        });

        THREAD_POOL.depopulate();
    }

    struct Tree<T: Send> {
        value: T,
        children: Vec<Tree<T>>,
    }

    impl<T: Send> Tree<T> {
        fn iter(&self) -> vec::IntoIter<&T> {
            once(&self.value)
                .chain(self.children.iter().flat_map(Tree::iter))
                .collect::<Vec<_>>() // seems like it shouldn't be needed... but prevents overflow
                .into_iter()
        }

        fn update<OP>(&mut self, op: OP)
        where
            OP: Fn(&mut T) + Sync,
            T: Send,
        {
            scope(|scope| self.update_in_scope(scope, &op));
        }

        fn update_in_scope<'scope, 'env, OP>(
            &'env mut self,
            scope: &'scope Scope<'scope, 'env>,
            op: &'scope OP,
        ) where
            OP: Fn(&mut T) + Sync,
        {
            let Tree { value, children } = self;
            scope.spawn(move |worker: &Worker| {
                for child in children {
                    scope.spawn_on(worker, move |_: &Worker| {
                        let child = child;
                        child.update_in_scope(scope, op)
                    });
                }
            });

            op(value);
        }
    }

    fn random_tree(depth: usize, seed: u64) -> Tree<usize> {
        assert!(depth > 0);
        let mut rng = XorShift64Star::from_seed(seed);
        random_tree_inner(depth, &mut rng)
    }

    fn random_tree_inner(depth: usize, rng: &mut XorShift64Star) -> Tree<usize> {
        let children = if depth == 0 {
            vec![]
        } else {
            (0..rng.next_usize(4)) // somewhere between 0 and 3 children at each level
                .map(|_| random_tree_inner(depth - 1, rng))
                .collect()
        };

        Tree {
            value: rng.next_usize(1_000_000),
            children,
        }
    }

    /// Check that if you have a chain of scoped tasks where T0 spawns T1
    /// spawns T2 and so forth down to Tn, the stack space should not grow
    /// linearly with N. We test this by some unsafe hackery and
    /// permitting an approx 10% change with a 10x input change.
    ///
    /// Ported from rayon.
    #[test]
    fn scope_linear_stack_growth() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.with_worker(|_| {
            let mut max_diff = Mutex::new(0);
            let bottom_of_stack = 0;
            scope(|s| the_final_countdown(s, &bottom_of_stack, &max_diff, 5));
            let diff_when_5 = *max_diff.get_mut().unwrap() as f64;

            scope(|s| the_final_countdown(s, &bottom_of_stack, &max_diff, 500));
            let diff_when_500 = *max_diff.get_mut().unwrap() as f64;

            let ratio = diff_when_5 / diff_when_500;
            assert!(
                ratio > 0.9 && ratio < 1.1,
                "stack usage ratio out of bounds: {ratio}"
            );
        });

        THREAD_POOL.depopulate();
    }

    fn the_final_countdown<'scope, 'env>(
        scope: &'scope Scope<'scope, 'env>,
        bottom_of_stack: &'scope i32,
        max: &'scope Mutex<usize>,
        n: usize,
    ) {
        let top_of_stack = 0;
        let p = bottom_of_stack as *const i32 as usize;
        let q = &top_of_stack as *const i32 as usize;
        let diff = p.abs_diff(q);

        let mut data = max.lock().unwrap();
        *data = Ord::max(diff, *data);

        if n > 0 {
            scope.spawn(move |_: &Worker| the_final_countdown(scope, bottom_of_stack, max, n - 1));
        }
    }

    /// Tests that panics within the scope closure are propagated.
    #[test]
    #[should_panic(expected = "Hello, world!")]
    fn scope_panic_propagate_from_closure() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|_: &Scope| panic!("Hello, world!"));

        THREAD_POOL.depopulate();
    }

    /// Tests that panics within closures spawned onto a scope are propagated.
    #[test]
    #[should_panic(expected = "Hello, world!")]
    fn scope_panic_propagate_from_spawn() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|scope| scope.spawn(|_: &Worker| panic!("Hello, world!")));

        THREAD_POOL.depopulate();
    }

    /// Tests that panics within nested scoped spawns are propagated.
    #[test]
    #[should_panic(expected = "Hello, world!")]
    fn scope_panic_propagate_from_nested_spawn() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|scope| {
            scope.spawn(|_: &Worker| {
                scope.spawn(|_: &Worker| scope.spawn(|_: &Worker| panic!("Hello, world!")))
            })
        });

        THREAD_POOL.depopulate();
    }

    /// Tests that panics within nested scopes are propagated.
    #[test]
    #[should_panic(expected = "Hello, world!")]
    fn scope_panic_propagate_from_nested_scope_spawn() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.scope(|scope_1| {
            scope_1.spawn(|worker: &Worker| {
                worker.scope(|scope_2| scope_2.spawn(|_: &Worker| panic!("Hello, world!")))
            })
        });

        THREAD_POOL.depopulate();
    }

    /// Tests that work spawned after a panicking job still completes.
    #[test]
    #[cfg_attr(not(panic = "unwind"), ignore)]
    fn scope_panic_propagate_still_execute_after() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut x = false;
        let result = unwind::halt_unwinding(|| {
            THREAD_POOL.scope(|scope| {
                scope.spawn(|_: &Worker| panic!("Hello, world!")); // job A
                scope.spawn(|_: &Worker| x = true); // job B, should still execute even though A panics
            });
        });

        match result {
            Ok(_) => panic!("failed to propagate panic"),
            Err(_) => assert!(x, "job b failed to execute"),
        }

        THREAD_POOL.depopulate();
    }

    /// Tests that work spawned before a panicking job still completes.
    #[test]
    #[cfg_attr(not(panic = "unwind"), ignore)]
    fn scope_panic_propagate_still_execute_before() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut x = false;
        let result = unwind::halt_unwinding(|| {
            THREAD_POOL.scope(|scope| {
                scope.spawn(|_: &Worker| x = true); // job B, should still execute even though A panics
                scope.spawn(|_: &Worker| panic!("Hello, world!")); // job A
            });
        });
        match result {
            Ok(_) => panic!("failed to propagate panic"),
            Err(_) => assert!(x, "job b failed to execute"),
        }

        THREAD_POOL.depopulate();
    }

    /// Tests that work spawned before the scoped closure panics still completes.
    #[test]
    #[cfg_attr(not(panic = "unwind"), ignore)]
    fn scope_panic_propagate_still_execute_before_closure() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut x = false;
        let result = unwind::halt_unwinding(|| {
            THREAD_POOL.scope(|scope| {
                scope.spawn(|_: &Worker| x = true); // spawned job should still execute despite later panic
                panic!("Hello, world!");
            });
        });
        match result {
            Ok(_) => panic!("failed to propagate panic"),
            Err(_) => assert!(x, "panic after spawn, spawn failed to execute"),
        }

        THREAD_POOL.depopulate();
    }

    /// Tests that the closure still completes if one of it's items panics.
    #[test]
    #[cfg_attr(not(panic = "unwind"), ignore)]
    fn scope_panic_propagate_still_execute_closure_after() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut x = false;
        let result = unwind::halt_unwinding(|| {
            THREAD_POOL.scope(|scope| {
                scope.spawn(|_: &Worker| panic!("Hello, world!"));
                x = true;
            });
        });
        match result {
            Ok(_) => panic!("failed to propagate panic"),
            Err(_) => assert!(x, "panic in spawn tainted scope"),
        }

        THREAD_POOL.depopulate();
    }

    /// Tests that we can create scopes that capture a static env.
    #[test]
    fn scope_static() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        static COUNTER: AtomicUsize = AtomicUsize::new(0);

        let mut range = 0..100;
        let sum = range.clone().sum();
        let iter = &mut range;

        COUNTER.store(0, Ordering::Relaxed);
        THREAD_POOL.scope::<'static>(|scope: &Scope| {
            for i in iter {
                scope.spawn(move |_: &Worker| {
                    COUNTER.fetch_add(i, Ordering::Relaxed);
                });
            }
        });

        assert_eq!(COUNTER.load(Ordering::Relaxed), sum);

        THREAD_POOL.depopulate();
    }

    /// Tests that scopes can deal with multiple lifetimes being captured.
    #[test]
    fn scope_mixed_lifetimes() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        fn increment<'slice, 'counter>(counters: &'slice [&'counter AtomicUsize]) {
            THREAD_POOL.scope::<'counter>(move |scope| {
                // We can borrow 'slice here, but the spawns can only borrow 'counter.
                for &c in counters {
                    scope.spawn(move |_: &Worker| {
                        c.fetch_add(1, Ordering::Relaxed);
                    });
                }
            });
        }

        let counter = AtomicUsize::new(0);
        increment(&[&counter; 100]);
        assert_eq!(counter.into_inner(), 100);

        THREAD_POOL.depopulate();
    }
}
