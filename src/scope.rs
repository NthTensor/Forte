//! This module defines a utility for spawning non-static jobs. For more
//! information see [`crate::scope()`] or the [`Scope`] type.

use alloc::boxed::Box;
use core::any::Any;
use core::cell::UnsafeCell;
use core::future::Future;
use core::hint::cold_path;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::ptr;
use core::ptr::NonNull;
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
use crate::thread_pool::Broadcast;
use crate::thread_pool::Scheduler;
use crate::thread_pool::Worker;
use crate::unwind;
use crate::unwind::AbortOnDrop;
use crate::util::IterBits;

// -----------------------------------------------------------------------------
// Scope

/// A scope which can spawn a number of non-static jobs and async tasks.
///
/// Refer to [`scope`](crate::scope()) for more extensive documentation.
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
/// the [`SpawnScoped`] trait.
pub struct Scope<'scope, 'env: 'scope> {
    /// The thread-pool this scope is attached to.
    thread_pool: &'static ThreadPool,
    /// Number of active references to the scope (including the owning
    /// allocation). This is incremented each time a new `ScopePtr` is created,
    /// and decremented when a `ScopePtr` is dropped or the owning thread is
    /// done using it.
    count: AtomicU32,
    /// A latch used to communicate when the scope has been completed.
    completed: Latch,
    /// If any job panics, we store the result here to propagate it.
    panic: AtomicPtr<Box<dyn Any + Send + 'static>>,
    /// This adds invariance over 'scope. In other words, it ensures 'scope
    /// cannot shrink or grow. This keeps the lifetime properly bound to the
    /// closure.
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
    /// This adds invariance over 'env. In other words, it ensures 'env cannot
    /// shrink or grow.
    ///
    /// This is not strictly necessary for correctness, and could probably be
    /// covariant instead. Invariance was chosen to follow the precedent set by
    /// `std::thread::scope`.
    _env: PhantomData<&'env mut &'env ()>,
}

/// Executes a new scope on a worker. [`Worker::scope`],
/// [`ThreadPool::scope`][crate::ThreadPool::scope] and
/// [`scope`][crate::scope()] are all just aliases for this function.
///
/// For details about the `'scope` and `'env` lifetimes see [`Scope`].
#[inline]
pub fn with_scope<'env, F, T>(worker: &Worker, f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    let abort_guard = AbortOnDrop;
    // Create a new scope object on the stack.
    let scope = Scope {
        thread_pool: worker.thread_pool(),
        count: AtomicU32::new(1),
        completed: worker.new_latch(),
        panic: AtomicPtr::new(ptr::null_mut()),
        _scope: PhantomData,
        _env: PhantomData,
    };
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
    // SAFETY: This is called only once within this function, and then the scope
    // is dropped.
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
    /// Runs a closure or future sometime before the scope completes.
    ///
    /// This is like [`Worker::spawn`], allows the work to borrow local data.
    /// Vald inputs to this method are:
    ///
    /// * A `for<'worker> FnOnce(&'worker Worker)` closure, with no return type.
    ///
    /// * A `Future<Output = ()>` future, with no return type.
    pub fn spawn<M, S>(&'scope self, work: S)
    where
        S: SpawnScoped<'scope, M> + Send,
    {
        let thread_pool = self.thread_pool;
        let scheduler = |job_ref, worker: Option<&Worker>| match worker {
            Some(worker) => worker.fifo_queue.push_new(job_ref),
            None => thread_pool.push_shared_job(job_ref),
        };
        // SAFETY: `spawn_scoped` requires that `!Send` work only be scheduled
        // to run on the current thread. Here `S: Send`, so that obligation is
        // vacuous.
        unsafe { work.spawn_scoped(self, scheduler) };
    }

    /// Like [`spawn`](Self::spawn), but routes the work to a specific pool
    /// member so it runs on that member's thread.
    pub fn spawn_on<M, S>(&'scope self, member_index: usize, work: S)
    where
        S: SpawnScoped<'scope, M> + Send,
    {
        let member_data = self.thread_pool.get_member_data();
        let scheduler = |job_ref, _: Option<&Worker>| {
            member_data.broadcasts[member_index].push(job_ref);
            member_data.semaphores[member_index].signal();
        };
        // SAFETY: `spawn_scoped` requires that `!Send` work only be scheduled
        // to run on the current thread. Here `S: Send`, so that obligation is
        // vacuous.
        unsafe { work.spawn_scoped(self, scheduler) }
    }

    /// Like [`spawn`](Self::spawn), but accepts `!Send` work, which is confined
    /// to (and only ever polled on) the current worker's thread.
    pub fn spawn_local<M, S>(&'scope self, worker: &Worker, work: S)
    where
        S: SpawnScoped<'scope, M>,
    {
        let queue = worker.nonsend_fifo_queue.clone();
        let semaphore = &worker.member_data.semaphores[worker.member_index];
        let scheduler = move |job_ref, _: Option<&Worker>| {
            queue.push(job_ref);
            semaphore.signal();
        };
        // SAFETY: `spawn_scoped` requires that `!Send` work only be scheduled
        // to run on the current thread.
        //
        // The scheduler pushes the job onto this worker's `nonsend_fifo_queue`.
        // That queue is only ever drained by this worker on this thread (see
        // `Worker::find_local_work`). For a future, every reschedule re-pushes
        // onto the same queue regardless of which thread issued the wakeup, so
        // the work is confined to this thread.
        unsafe { work.spawn_scoped(self, scheduler) }
    }

    /// Runs an operation across multiple threads.
    ///
    /// This is like [`Worker::spawn_broadcast`], but allows the work to borrow
    /// local data.
    pub fn spawn_broadcast<F>(&'scope self, f: F)
    where
        F: for<'worker> Fn(Broadcast<'worker>) + Send + Sync + 'scope,
    {
        // Prevent workers from leaving the pool, and read the membership bitset
        // once it's frozen.
        let members = self.thread_pool.freeze_membership();
        let participants = members.count_ones() as usize;

        // We are going to spawn a job for every participant, and need to keep
        // the scope alove while that completes. For the sake of efficiency,
        // we'll increment the counter for all of these jobs in one single
        // operation.
        self.count.fetch_add(participants as u32, Ordering::Relaxed); // (*)

        // The jobs can outlive this stack frame, so they cannot borrow `f`;
        // they share ownership of it through an `Arc` instead.
        let f = Arc::new(f);

        // Create a new job for each member
        let member_data = self.thread_pool.get_member_data();
        for (i, member_index) in members.iter_bits().enumerate() {
            let func = Arc::clone(&f);
            let scope = self;
            let op = move |worker: &Worker| {
                // Run an instance of the job job.
                let result = unwind::halt_unwinding(|| {
                    func(Broadcast {
                        worker,
                        index: i,
                        participants,
                    });
                });
                // If the operation panics on any thread, write the panic out to
                // the scope panic slot.
                if let Err(err) = result {
                    scope.store_panic(err);
                };
                // Dropping the last handle drops `f`, whose drop glue may read
                // `'scope` data. That data is only guaranteed to live until the
                // scope's counter reaches zero, so the handle must be dropped
                // before this job's count is removed.
                //
                // Because `f` is user code its drop glue may also panic. The
                // panic is caught and stored on the scope so that it cannot
                // unwind out of this job.
                if let Err(err) = unwind::halt_unwinding(|| drop(func)) {
                    scope.store_panic(err);
                }
                // SAFETY: This corresponds to one of the increments performed in
                // a batch with the `fetch_add` at the start of this function.
                // It was incremented `p` times, and this will be called `p`
                // times, as the workers complete the `p` broadcast jobs.
                unsafe { scope.remove_reference() };
            };

            // SAFETY: `HeapJob::new` may only be passed functions that do not
            // unwind. The function `op` catches panics from the broadcast
            // operation `func`, and makes no other calls that can panic.
            let job = unsafe { HeapJob::new(op) };

            // SAFETY: `HeapJob::into_job_ref` requires:
            //
            // * The `JobRef` will not outlive any of the items closed over by
            //   the `op`.
            //
            //   The captures of `op` are `i` and `participants` (both `Copy`),
            //   `scope: &'scope Scope`, and `func: Arc<F>`.
            //
            //   The job owns `func`, so the `JobRef` cannot outlive it. The
            //   `Arc` keeps the value `f` alive until the last handle is
            //   dropped; each handle is dropped inside `op` when a job runs,
            //   when this function's own handle goes out of scope, or leaks
            //   along with an unexecuted `JobRef`.
            //
            //   The capture `scope`, and any `'scope` data borrowed by `f`,
            //   are kept alive by the scope's lifetime extension logic: we
            //   incremented the scope's counter once per job on the line
            //   marked with (*), and the scope will not complete (extending
            //   the lifetime of `'scope`) until there is a corresponding call
            //   to `remove_reference()`. Each job holds its increment until
            //   after it has called `func` and dropped its `Arc` handle, so
            //   neither the `Scope` nor anything `f` borrows can be freed
            //   while a job might still use (or drop) them.
            //
            // * If `op` is `!Send` then `JobRef::execute` will only be called
            //   on this thread.
            //
            //   `op` is unconditionally `Send`: `F: Send + Sync` makes
            //   `Arc<F>: Send`, and `Scope: Sync` makes `&Scope: Send`.
            let job_ref = unsafe { job.into_job_ref() };
            member_data.broadcasts[member_index].push(job_ref);
            member_data.semaphores[member_index].signal();
        }

        // Once we have finished pushing jobs out to workers who we know are not
        // in the middle of resginging, we can allow resignations again.
        self.thread_pool.unfreeze_membership();
    }

    /// Adds an additional reference to the scope's reference counter.
    ///
    /// Every call to this should have a matching call to
    /// `Scope::remove_reference`, or the scope will block forever on
    /// completion.
    fn add_reference(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes a reference from the scope's reference counter.
    ///
    /// # Safety
    ///
    /// The caller must be able to point to a corresponding place where the scope
    /// counter was incremented by one. This could be through a call to
    /// `add_reference`, a direct `fetch_add` on the underlying counter, or the
    /// implicit initial increment the scope starts with.
    unsafe fn remove_reference(&self) {
        let counter = self.count.fetch_sub(1, Ordering::Relaxed);
        if counter == 1 {
            // Alerts the owning thread that the scope has completed.
            //
            // This should never panic, because the counter can only go to zero
            // once, when the scope has been dropped and all work has been
            // completed.
            //
            // SAFETY: The owning thread must call `Scope::complete` before
            // dropping any `Scope`, and `Scope::complete` does not return until
            // the latch is set, which happens only here, after the count
            // reaches zero. Therefore, the `completed` field of this `Scope`
            // must still be a live latch.
            unsafe { Latch::set(&self.completed, false) };
        }
    }

    /// Stores a panic so that it can be propagated when the scope is complete.
    /// If called multiple times, only the first panic is stored, and the
    /// remainder are dropped.
    ///
    /// This function never unwinds, but may abort in circumstances.
    #[cold]
    fn store_panic(&self, err: Box<dyn Any + Send + 'static>) {
        // Create an abort guard that will prevent this function from unwinding.
        let abort_guard = AbortOnDrop;
        // Check if the panic pointer has already been set. This lets us avoid
        // allocating a second time, and means we can immediately drop the panic
        // we have just been passed.
        if self.panic.load(Ordering::Relaxed).is_null() {
            let nil = ptr::null_mut();
            let err_ptr = Box::into_raw(Box::new(err));
            // Try to atomically swap the panic pointer from null to the newly
            // allocated error slot. If this succeeds, the write occurs with
            // `Release` ordering, which establishes a happens-before
            // relationship with the fence in `maybe_propagate_panic`, so that
            // the heap-allocated error will be visible to the reader.
            //
            // If the write fails, another panic must have already occurred, and
            // we don't need to synchronize memory (the previous call to
            // `store_panic` handles the synchronization for it's panic data).
            if !self
                .panic
                .compare_exchange(
                    nil,
                    err_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Another panic raced in ahead of us, so we need to drop this
                // error. Dropping the payload may itself panic, but this will
                // trigger the abort guard instead of unwinding.
                //
                // SAFETY: This was created by `Box::into_raw` just above.
                let err = unsafe { Box::from_raw(err_ptr) };
                drop(err);
            }
        } else {
            // A panic is already stored, so this payload is dropped. Dropping
            // it may itself panic, but this will trigger the abort guard
            // instead of unwinding.
            drop(err);
        }
        // At this point we can allow panics to unwind again.
        core::mem::forget(abort_guard);
    }

    /// Propagates any panic captured while the scope was executing.
    fn maybe_propagate_panic(&self) {
        // Swap out the panic pointer. This gives us exclusive read access to
        // whatever it points to.
        let panic = self.panic.swap(ptr::null_mut(), Ordering::Relaxed);
        if !panic.is_null() {
            // We generally don't expect panics to happen.
            cold_path();
            // If the panic pointer is not null, emit an `Acquire` fence to
            // establish a happens-after relationship with the `Release` branch
            // of the `compare_exchange` call in `store_panic`, so that the
            // error stored at the memory location pointed to by the atomic
            // pointer will be visible on the following line.
            fence(Ordering::Acquire);
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
    /// The caller must ensure that this is called at most once.
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

/// A trait for types that can be spawned onto a [`Scope`].
///
/// It is implemented for:
///
/// * Closures that satisfy `for<'worker> FnOnce(&'worker Worker) + Send + 'scope`.
///
/// * Futures that satisfy `Future<Output = ()> + Send + 'scope`.
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
/// //              ^^^^^^^ the trait `SpawnScoped<'_, _>` is not implemented for closure ...
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
pub trait SpawnScoped<'scope, M>: 'scope {
    /// Similar to [`spawn`][crate::Worker::spawn] but adds the work to a
    /// [`Scope`]. This work will be polled to completion some-time before the
    /// scome completes, and may borrow data that outlives the scope.
    ///
    /// # Safety
    ///
    /// If `Self` is not `Send`, the `scheduler` must only ever schedule the
    /// work to run on the current thread. For a future, this includes every
    /// reschedule triggered by a wakeup, no matter which thread the wakeup
    /// occurs on.
    unsafe fn spawn_scoped<'env, S: for<'w> Scheduler<'w>>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        scheduler: S,
    );
}

impl<'scope, F> SpawnScoped<'scope, FnOnceMarker> for F
where
    F: FnOnce(&Worker) + 'scope,
{
    #[inline]
    unsafe fn spawn_scoped<'env, S: for<'w> Scheduler<'w>>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        scheduler: S,
    ) {
        // Create a job to execute the spawned function in the scope.
        let scope_ptr = ScopePtr::new(scope);

        // Create the spawned operation
        let op = move |worker: &Worker| {
            // Catch any panics and store them on the scope.
            let result = unwind::halt_unwinding(|| self(worker));
            if let Err(err) = result {
                scope_ptr.store_panic(err);
            };
            drop(scope_ptr);
        };

        // SAFETY: `HeapJob::new` may only be called with functions that do not
        // unwind. The function `op` uses `halt_unwinding` to ensure this is the
        // case.
        let job = unsafe { HeapJob::new(op) };
        // SAFETY: `HeapJob::into_job_ref` requires:
        //
        // * The `JobRef` will not outlive any of the items closed over by
        //   the `op`.
        //
        //   The only non-copy captures are `self` and the scope pointer, both
        //   of which have lifetime `'scope`. So we must show that the `JobRef`
        //   will not outlive `'scope`.
        //
        //   This is ensured via the scope's lifetime extension logic: the scope
        //   will not complete so long as the `scope_ptr` is held, extending the
        //   lifetime of `'scope` until after `self` the job executes and is
        //   dropped.
        //
        // * If `op` is `!Send` then `JobRef::execute` will only be called
        //   on this thread.
        //
        //   `op` is unconditionally `Send`.
        let job_ref = unsafe { job.into_job_ref() };

        // Send the job to a queue to be executed.
        Worker::with_current(|worker| scheduler.schedule(job_ref, worker));
    }
}

impl<'scope, Fut> SpawnScoped<'scope, FutureMarker> for Fut
where
    Fut: Future<Output = ()> + 'scope,
{
    #[inline]
    unsafe fn spawn_scoped<'env, S: for<'w> Scheduler<'w>>(
        self,
        scope: &'scope Scope<'scope, 'env>,
        scheduler: S,
    ) {
        let job = ScopeFutureJob::new(scope, scheduler, self);
        Worker::with_current(|worker| job.schedule(worker));
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
/// * A pending job that has been (or is about to be) pushed to the queue
///   so that it can be polled.
///
/// * A pending job that is currently being polled (or has just finished) and
///   which was *not* queued after it was woken, because it was woken while
///   running.
///
/// * A job that was woken after it completed or panicked. These jobs will stay
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
/// polled, assuming it has not panicked or been completed).
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
struct ScopeFutureJob<'scope, 'env, S: for<'w> Scheduler<'w>, Fut> {
    /// The future that the job exists to poll. This is stored in an unsafe cell
    /// to allow mutable access within an `Arc<T>`. The `state` field acts as a
    /// kind of mutex that ensures exclusive access by preventing the job from
    /// being queued or executed multiple times simultaneously.
    ///
    /// The future is wrapped in `ManuallyDrop` so the `Arc`'s drop glue never
    /// runs its destructor. That lets a waker free the allocation on any thread
    /// while `poll` remains the sole place the future is dropped, on its
    /// confined thread.
    future: UnsafeCell<ManuallyDrop<Fut>>,
    /// A scope pointer. This allows the job to interact with the scope, and
    /// also keeps the scope alive until the job is dropped.
    scope_ptr: ScopePtr<'scope, 'env>,
    /// The state of the job, which is either READY, WOKEN, or LOCKED.
    state: AtomicU32,
    /// The scheduler used to queue this job whenever it needs to be polled.
    scheduler: S,
}

impl<'scope, 'env, S, Fut> ScopeFutureJob<'scope, 'env, S, Fut>
where
    S: for<'w> Scheduler<'w>,
    Fut: Future<Output = ()> + 'scope,
{
    /// This vtable is part of what allows a `ScopeFutureJob` to act as an
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
        scope: &Scope<'scope, 'env>,
        scheduler: S,
        future: Fut,
    ) -> Arc<Self> {
        let scope_ptr = ScopePtr::new(scope);
        Arc::new(Self {
            future: UnsafeCell::new(ManuallyDrop::new(future)),
            scope_ptr,
            // The job starts in the WOKEN state because we always queue it
            // after creating it.
            state: AtomicU32::new(WOKEN),
            scheduler,
        })
    }

    /// Converts an `Arc<ScopeFutureJob>` into a job ref that can be queued on a
    /// thread pool. The ref-count is not decremented, ensuring that the job
    /// remains alive while this job ref exists.
    ///
    /// Forgetting this job ref will cause a memory leak.
    fn into_job_ref(self: Arc<Self>) -> JobRef {
        // SAFETY: Pointers created by `Arc::into_raw` are never null.
        let job_pointer = unsafe {
            NonNull::new_unchecked(Arc::into_raw(self).cast_mut().cast())
        };

        // SAFETY: `JobRef::new` requires us to show that `execute` will only be
        // called on the resulting `JobRef` where it is sound to call `poll` on
        // `job_pointer`.
        //
        // `Poll` has two obligations:
        //
        // * `job_pointer` must have been produced by `Arc::into_raw` on an
        //   `Arc<Self>`.
        //
        //   We produced `job_pointer` this way just above.
        //
        // * We must hold ownership of exactly one strong reference count for
        //   the allocation.
        //
        //   We start with an `Arc<Self>`, so we must own a strong reference.
        //   Calling `Arc::into_raw` transfers the strong count of `self` onto
        //   `job_pointer` without decrementing it. Therefore, when `execute`
        //   is called, there will still be a strong reference for it to
        //   consume.
        //
        // `JobRef::new` also requires that `poll` not unwind. See the
        // doc-comment for `poll` for an argument as to why this is the case.
        unsafe { JobRef::new(job_pointer, Self::poll) }
    }

    /// Schedules this job to be polled, by converting it into a `JobRef` and
    /// handing that to the job's own `scheduler`. The `worker` param should be
    /// the same as calling `Worker::with_current`.
    fn schedule(self: Arc<Self>, worker: Option<&Worker>) {
        // Clone a strong reference so the allocation — and therefore the
        // `scheduler` field — stays alive for the entire `schedule` call.
        //
        // `into_job_ref` consumes `self` via `Arc::into_raw`, moving our strong
        // reference into the `JobRef` *without* decrementing the count. The act
        // of calling `schedule` enqueues that `JobRef`, after which another
        // worker may execute and drop it before `schedule` returns. Holding
        // `this` keeps an independent strong reference alive until the end of
        // this function, so borrowing `this.scheduler` across the call cannot
        // dangle.
        let this = Arc::clone(&self);
        let job_ref = self.into_job_ref();
        this.scheduler.schedule(job_ref, worker);
    }

    /// Polls the future.
    ///
    /// # Panics
    ///
    /// This function does not unwind. Panics that occur while running the job
    /// are caught with `catch_unwind` and stored on the scope. Other panics,
    /// such as those emited by drop-glue, may cause aborts.
    ///
    /// # Safety
    ///
    /// `this` must be a pointer produced by `Arc::into_raw` on an `Arc<Self>`.
    ///
    /// This call takes ownership of exactly one strong reference count for that
    /// allocation, consuming it via `Arc::from_raw` internally. The caller must
    /// hold "ownership" of one such strong reference.
    unsafe fn poll(this: NonNull<()>, worker: &Worker) {
        // While we still have a raw pointer to the job, create a raw task waker
        // using our vtable.
        let raw_waker =
            RawWaker::new(this.as_ptr().cast_const(), &Self::VTABLE);

        // Create a new waker from the raw waker. This is *non-owning* and
        // functions like a `&Arc<Self>` rather than an `Arc<Self>`. We wrap it
        // in a `ManuallyDrop` to prevent the waker from calling `drop_as_waker`
        // through the vtable, which would cause the reference-count to
        // decrement (incorrectly).
        //
        // SAFETY: The API contract of `RawWaker` and `RawWakerVTable` is upheld
        // by the `Self::VTABLE` const.
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
        //    to be freed. A waker is `Send`, so this may run on any thread, but
        //    freeing the job does not run the future's destructor (it is
        //    `ManuallyDrop`, dropped only by `poll` on the confined thread); the
        //    remaining fields are `Send`, so freeing anywhere is sound.
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

        // SAFETY: The following line requires that:
        //
        // * No other mutable references to the future exist.
        //
        //   Access to the future is protected by the `state` field, which acts
        //   as a mutex. Just above, we executed
        //
        //       state.swap(LOCKED, Ordering::Acquire)
        //
        //   which transitions us from the `WOKEN` into the `LOCKED` state. Any
        //   concurrent caller that also tries to execute `poll` will fail this
        //   swap, and cause an abort. Exclusive access is therefore
        //   guaranteed.
        //
        //   In the event that `poll` has been called previously, the `Acquire`
        //   ordering synchronizes with the call to
        //
        //       fence(Ordering::Release)
        //
        //   later in this function. This ensures that we are not racing with
        //   another mutable reference to the same value.
        //
        // * The future will not move.
        //
        //   The future does not move, because it is stored in a field within an
        //   `Arc`, which has a stable heap-allocated address.
        let future = unsafe { Pin::new_unchecked(&mut **this.future.get()) };

        // Create a new context from the waker, and poll the future.
        let mut cx = Context::from_waker(&waker);
        let result = unwind::halt_unwinding(|| future.poll(&mut cx));

        // Update the job state depending on the outcome of polling the future.
        //
        // Out of an abundance of causion, we add an abort guard here.
        let abort_guard = AbortOnDrop;
        match result {
            // The job completed without panicking.
            Ok(Poll::Ready(())) => {
                // Drop the future here, on its confined thread. State stays
                // LOCKED, so it is never re-polled and this runs once.
                //
                // SAFETY: The LOCKED state gives us exclusive access.
                unsafe { ManuallyDrop::drop(&mut *this.future.get()) };
                drop(this);
            }
            // The job is still pending, and has not yet panicked.
            Ok(Poll::Pending) => {
                // Try to set the state back back idle so other threads can
                // schedule it again. This will only fail if the job was woken
                // while running, and is already in the WOKEN state.
                //
                // If successful, this releases our exclusive ownership of the
                // future and synchronizes with the `Acquire` swap at the start
                // of this function. This establishes a "happens before"
                // relaationship with each poll of the future.
                let rescheduled = this
                    .state
                    .compare_exchange(
                        LOCKED,
                        READY,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_err();
                // If the job was woken while running, it should be queued
                // immediately. Conveniently, we know the state will already be
                // WOKEN, so we can leave it as it is.
                if rescheduled {
                    // The future was woken while we were polling it, so it is
                    // already back in the WOKEN state; queue it to be polled
                    // again, preferring this worker.
                    //
                    // NOTE: Eventually we may want to chose not to re-evaluate
                    // this sometimes, to break out of infinite async loops.
                    this.schedule(Some(worker));
                } else {
                    drop(this);
                }
            }
            // The job panicked. Store the panic in the scope so it can be
            // resumed later.
            Err(err) => {
                this.scope_ptr.store_panic(err);
                // Drop the future here, on its confined thread; see the Ready
                // branch.
                //
                // SAFETY: The LOCKED state gives us exclusive access.
                unsafe { ManuallyDrop::drop(&mut *this.future.get()) };
                drop(this);
            }
        }
        core::mem::forget(abort_guard);

        // In the branches above where the task is dropped (rather than
        // converted into a job-ref and queued), the drop decrements the
        // reference count, freeing the task if no wakers for it are being
        // held.
        //
        // I view this as preferable to the alternative if there are no wakers
        // being held for the task the task will never wake and the scope will
        // wait for it's latch indefinetly, and deadlock.
    }

    /// Creates a new `RawWaker` from the provided pointer.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    unsafe fn clone_as_waker(this: *const ()) -> RawWaker {
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance of `Arc<Self>`.
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
        // instance of `Arc<Self>`.
        let this = unsafe { Arc::from_raw(this.cast::<Self>()) };

        if this.state.swap(WOKEN, Ordering::Relaxed) == READY {
            Worker::with_current(|worker| this.schedule(worker));
        }
    }

    /// Queues self to be executed without consuming the waker.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    unsafe fn wake_by_ref(this: *const ()) {
        // We use manually drop here to prevent us from consuming the arc on
        // drop. This functions like an `&Arc<Self>` rather than an `Arc<Self>`.
        //
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance of `Arc<Self>`.
        let this =
            unsafe { ManuallyDrop::new(Arc::from_raw(this.cast::<Self>())) };

        if this.state.swap(WOKEN, Ordering::Relaxed) == READY {
            // Clone the waker, convert it into a job-ref and queue it.
            let this = ManuallyDrop::into_inner(this.clone());
            Worker::with_current(|worker| this.schedule(worker));
        }
    }

    /// Frees the waker.
    ///
    /// # Safety
    ///
    /// Must be called with a pointer created by calling `Arc::into_raw` on an
    /// instance of `Arc<Self>` that is still alive.
    unsafe fn drop_as_waker(this: *const ()) {
        // Rather than converting back into an arc, we can just decrement the
        // counter here.
        //
        // SAFETY: This is called on a pointer created by `Arc::into_raw` on an
        // instance of `Arc<Self>`.
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

    // SAFETY: Transferring ownership of the `*const Scope` is sound because the
    // pointee is reached only bia atomic ops and is kept alive by the refcount.
    unsafe impl Send for ScopePtr<'_, '_> {}

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

#[cfg(test)]
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

    use super::Scope;
    use crate::ThreadPool;
    use crate::Worker;
    use crate::scope;
    use crate::unwind;
    use crate::util::XorShift64Star;

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
            scope.spawn(|_: &Worker| {
                counter.fetch_add(1, Ordering::Relaxed);
                scope.spawn(|_: &Worker| {
                    counter.fetch_add(1, Ordering::Relaxed);
                });
            });
        });
        assert_eq!(counter.load(Ordering::Relaxed), 4);

        THREAD_POOL.depopulate();
    }

    /// Tests that we can spawn futures onto the thread pool and that they can
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

        fn poll(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Self::Output> {
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

        THREAD_POOL.with_worker(|_: &Worker| {
            scope(|scope| {
                for _ in 0..NUM_JOBS {
                    scope.spawn(|_: &Worker| {
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
                scope.spawn(|_: &Worker| {
                    // Creating a new worker instead of reusing the old one is
                    // bad form, but we may as well test it.
                    THREAD_POOL.with_worker(|worker| {
                        worker.scope(|scope| {
                            scope.spawn(|_: &Worker| {
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
                scope.spawn(move |_: &Worker| {
                    divide_and_conquer(scope, counter_p, 1024)
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
        scope: &'scope Scope<'scope, 'env>,
        counter: &'scope AtomicUsize,
        size: usize,
    ) {
        if size > 1 {
            scope.spawn(move |_: &Worker| {
                divide_and_conquer(scope, counter, size / 2)
            });
            scope.spawn(move |_: &Worker| {
                divide_and_conquer(scope, counter, size / 2)
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
            scope.spawn(move |_: &Worker| {
                for child in children {
                    scope.spawn(move |_: &Worker| {
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

    fn random_tree_inner(
        depth: usize,
        rng: &mut XorShift64Star,
    ) -> Tree<usize> {
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
            scope.spawn(move |_: &Worker| {
                the_final_countdown(scope, bottom_of_stack, max, n - 1)
            });
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

        THREAD_POOL
            .scope(|scope| scope.spawn(|_: &Worker| panic!("Hello, world!")));

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
                scope.spawn(|_: &Worker| {
                    scope.spawn(|_: &Worker| panic!("Hello, world!"))
                })
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
                worker.scope(|scope_2| {
                    scope_2.spawn(|_: &Worker| panic!("Hello, world!"))
                })
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

        fn increment<'slice, 'counter>(
            counters: &'slice [&'counter AtomicUsize],
        ) {
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
