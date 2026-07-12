//! This module defines an executable unit of work called a "Job". Jobs are
//! what get scheduled on the thread pool. There are two core job types:
//! [`StackJob`] and [`HeapJob`]. There is no unifying `Job` trait. Instead,
//! what makes these both jobs is their ability to yield a [`JobRef`].
//!
//! After a job is allocated, we typically refer to it by a [`JobRef`]. Job refs
//! are type-erased, and can be sent between threads without moving the
//! underlying job.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::any::Any;
use core::cell::UnsafeCell;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;

use crate::latch::Latch;
use crate::thread_pool::Worker;
use crate::unwind;

// -----------------------------------------------------------------------------
// JobRef

/// A `JobRef` is a specialized v-table, containing a pointer to work that needs
/// to be executed, and a function pointer that is capable of executing it.
///
/// It is analogous to the chili type `JobShared` or the rayon type `JobRef`.
///
/// # Panics
///
/// Execute functions must never allow panics to unwind out of
/// `JobRef::execute`; this is a safety requirement of [`JobRef::new`]. Panics
/// must be caught and either handed to the pool's panic handler or re-emitted
/// when it is safe to do so.
pub struct JobRef {
    /// A non-null pointer to some type-erased data which can be executed as a
    /// job by the `execute_fn`. This will usually point to either an instance
    /// of `StackJob` or `HeapJob`. But it can contain other things as well.
    job_pointer: NonNull<()>,
    /// A function pointer that can execute the job stored at `job_pointer`.
    /// This usually points to an implementation of `Job::execute` (either
    /// `HeapJob::execute` or `StackJob::execute`). But it can contain other
    /// things as well.
    execute_fn: unsafe fn(NonNull<()>, &Worker),
}

impl JobRef {
    /// Creates a new `JobRef` from raw pointers.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `JobRef::execute` will only be called on the returned `JobRef` when it
    ///   is sound to call `execute_fn` on `job_pointer`. Because a `JobRef` is
    ///   `Send`, `execute` may run on any thread, so this must hold for whatever
    ///   thread runs it (e.g. `!Send` job data must only be touched on its
    ///   owning thread).
    ///
    /// * `execute_fn` will not unwind when called on `job_pointer`.
    #[inline(always)]
    pub unsafe fn new(
        job_pointer: NonNull<()>,
        execute_fn: unsafe fn(NonNull<()>, &Worker),
    ) -> JobRef {
        JobRef {
            job_pointer,
            execute_fn,
        }
    }

    /// Returns an opaque handle that can be saved and compared, without making
    /// `JobRef` itself `Copy + Eq`.
    #[inline(always)]
    pub fn id(&self) -> (usize, usize) {
        (self.job_pointer.as_ptr() as usize, self.execute_fn as usize)
    }

    /// Executes the `JobRef` by passing the execute function on the job pointer.
    #[inline(always)]
    pub fn execute(self, worker: &Worker) {
        // SAFETY: The caller of `JobRef::new` defines the conditions under
        // which this call is sound, and must ensure that this will not be
        // called unless these conditions are met.
        unsafe { (self.execute_fn)(self.job_pointer, worker) }
    }
}

// SAFETY: A `JobRef` is a function pointer plus a type-erased data pointer.
// Function pointers are `Send`; the data pointer may reference `!Send` data
// (e.g. a `!Send` future whose waker lives on another thread), so `Send` cannot
// be derived structurally. We nonetheless need it since, for instance, passing
// a pointer to `!Send` job data between threads is how, an IO thread hands a
// wakeup back to the thread that owns the future.
//
// Sending a `JobRef` between threads is sound. The only operation that
// dereferences the data pointer is `JobRef::execute`, and `JobRef::new`'s
// contract already requires the caller to ensure `execute` is called only when
// it is sound to do so *on the thread that runs it*. Moving the `JobRef` to
// another thread therefore cannot introduce an unsound access the `new` caller
// was not already obliged to rule out.
//
// The fields are private and `new` is the only constructor, so that obligation
// always rests on an `unsafe` call. Entirely safe code cannot fabricate a `JobRef`.
unsafe impl Send for JobRef {}

// -----------------------------------------------------------------------------
// Job queue

/// A queue of jobs. This is a simple wrapper around a vec dequeue that uses
/// inner mutation, and has some more intuitively named methods to enforce
/// conventions.
///
/// Note: This is !Sync because of the unsafe cell.
pub struct JobQueue {
    /// The queued jobs.
    ///
    /// Every method briefly takes `&mut *job_refs.get()` for a single
    /// `VecDeque` operation. That `&mut` is sound because it is always unique:
    ///
    /// * `JobQueue` is `!Sync` (it holds an `UnsafeCell`), so only one thread
    ///   ever accesses it.
    ///
    /// * No method returns a reference into `job_refs`, so no borrow escapes to
    ///   overlap a later call.
    ///
    /// * No user code or `Drop` glue runs while the borrow is live — `JobRef`
    ///   has no `Drop` impl, and `VecDeque`/`JobRef::id` never call back into
    ///   the queue.
    job_refs: UnsafeCell<VecDeque<JobRef>>,
}

impl JobQueue {
    /// Creates a new job queue.
    pub fn new() -> JobQueue {
        JobQueue {
            job_refs: UnsafeCell::new(VecDeque::new()),
        }
    }

    /// Insert a job at the back of the queue (the side with the newest jobs).
    pub fn push_new(&self, job_ref: JobRef) {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.push_back(job_ref);
    }

    /// Insert a job at the front of the queue (the side with the oldest jobs).
    pub fn push_old(&self, job_ref: JobRef) {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.push_front(job_ref);
    }

    /// Removes the newest job in the queue.
    pub fn pop_newest(&self) -> Option<JobRef> {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_back()
    }

    /// Removes the oldest job in the queue.
    pub fn pop_oldest(&self) -> Option<JobRef> {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_front()
    }

    /// Attempt to remove the given job-ref from the back of the queue.
    #[inline(always)]
    pub fn recover_newest(&self, id: (usize, usize)) -> bool {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        if job_refs.back().map(JobRef::id) == Some(id) {
            let _ = job_refs.pop_back();
            true
        } else {
            false
        }
    }

    /// The size of a chunk of jobs.
    const CHUNK_SIZE: usize = 16;

    /// Splits off a series of chunks from the end of the queue (the side with
    /// the newest jobs). Each chunk is of size `CHUNK_SIZE`. Afterwards, at most
    /// `CHUNK_SIZE` jobs will remain in the queue.
    pub fn split(&self) -> Vec<VecDeque<JobRef>> {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        let mut len = job_refs.len();
        let num_chunks = len / Self::CHUNK_SIZE;
        (0..num_chunks)
            .map(|_| {
                let chunk = job_refs.split_off(len - Self::CHUNK_SIZE);
                len -= Self::CHUNK_SIZE;
                chunk
            })
            .collect()
    }

    /// Appends a chunk of jobs (expected to be provided by `split`) to the
    /// queue. Jobs are added to the end (the side with the newest jobs).
    pub fn append(&self, mut split_refs: VecDeque<JobRef>) {
        // SAFETY: unique access to `job_refs` (see the field invariant).
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.append(&mut split_refs);
    }
}

// -----------------------------------------------------------------------------
// Stack allocated work function

/// This union helps us conserve stack space by allowing us to store (a) the
/// function we need to run (b) the output of that function or (c) a captured
/// panic at different times throughout the job's lifecycle.
///
/// As future work, we may want to consider pushing large values into a
/// heap-allocated pool.
union StackJobData<F, T> {
    func: ManuallyDrop<F>,
    output: ManuallyDrop<T>,
    error: ManuallyDrop<Box<dyn Any + Send>>,
}

/// A [`StackJob`] is a job that's allocated on the stack.
///
/// This is analogous to the chili type `JobStack` and the rayon type `StackJob`.
pub struct StackJob<F, T> {
    /// Job-completion signal. Set only by this job's `execute`. The field is
    /// private and never handed out as `&Latch`, so an observed completion
    /// means `execute` has run.
    completed: Latch,
    data: UnsafeCell<StackJobData<F, T>>,
    /// Makes this type `!Send`. Since `Worker` is also `!Send`, this ensures
    /// `new` and `wait` are both given the same worker.
    _not_send: PhantomData<*const ()>,
}

impl<F, T> StackJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Creates a new `StackJob` owned by the given worker.
    #[inline(always)]
    pub fn new(func: F, worker: &Worker) -> StackJob<F, T> {
        StackJob {
            data: UnsafeCell::new(StackJobData {
                func: ManuallyDrop::new(func),
            }),
            completed: worker.new_latch(),
            _not_send: PhantomData,
        }
    }

    /// Creates a `JobRef` pointing to this job. The underlying `StackJob` is
    /// not dropped after the `JobRef` is executed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * This is called at most once for each `StackJob`.
    ///
    /// * After this call, the `StackJob` will not be moved or dropped until one
    ///   of these conditions is met:
    ///
    ///   * (A) `wait` reports that the job has completed.
    ///
    ///   * (B) The `JobRef` has been dropped without `execute` being called.
    #[inline(always)]
    pub unsafe fn as_job_ref(&self) -> JobRef {
        let job_pointer = NonNull::from(self).cast();
        // SAFETY: We must show that `JobRef::execute` will only be called on
        // the returned `JobRef` if it is sound to call `execute_fn` on
        // `job_pointer`.
        //
        // Assume that `JobRef::execute` has been called, under the conditions
        // defined by the safety comment for this function. Then:
        //
        // * `this` is an aligned pointer to an initialized `StackJob<F, T>`,
        //   which will not be invalidated until `wait` reports the job complete.
        //
        //   We created `job_pointer` from a ref to `self`, which is a
        //   `StackJob`, so it must have pointed to an aligned `StackJob` at
        //   some point.
        //
        //   If the caller allows the `JobRef` to be executed, they must also
        //   ensure the pointer stays valid until `wait` has reported the job
        //   complete.
        //
        // * `StackJob::execute` is called at most once on any `StackJob`.
        //
        //   The caller ensures only one `JobRef` is ever created for this
        //   `StackJob`. Since `JobRef::execute` consumes that `JobRef`, it
        //   cannot be called multiple times.
        //
        // `JobRef::new` also requires that the execute function not unwind.
        // `StackJob::execute` never unwinds: it catches panics from the
        // closure (recording them in the latch, to be re-thrown by whoever
        // waits on it), and holds an abort guard while it runs, which turns
        // any other panic into an abort.
        unsafe { JobRef::new(job_pointer, Self::execute) }
    }

    /// Waits for this job to complete, running other work on `worker`
    /// meanwhile. Returns `true` if the job completed with an error.
    #[inline(always)]
    pub fn wait(&self, worker: &Worker) -> bool {
        // Note: This really only works if `&Worker` is the same worker that was
        // used to create this job. Since `Worker` and `StackJob` are both
        // `!Send + !Sync`, we will just assume this is the case.
        worker.wait_for(&self.completed)
    }

    /// Unwraps the stack job back into a closure. This allows the closure to be
    /// executed without indirection in situations where one still has direct
    /// access.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * No `JobRef` currently exists for this `StackJob`.
    ///
    /// * No new `JobRef` will be created for this `StackJob`.
    ///
    /// * If a `JobRef` did exist, it was never executed.
    #[inline(always)]
    pub unsafe fn unwrap_func(mut self) -> F {
        // SAFETY: We have exclusive access to the active union field `func`. We
        // take `self` by value, so no other `unwrap_*` method can also hold it,
        // and the caller guarantees no `JobRef` exists, so `execute` cannot be
        // aliasing `data` through a raw pointer.
        //
        // `func` is the active variant because every `StackJob` is constructed
        // with `func`, only `execute` overwrites it, and the caller guarantees
        // `execute` was never called.
        let func_ref = unsafe { &mut self.data.get_mut().func };
        // SAFETY: The `StackJob` is dropped at the end of this block, so `data`
        // is never accessed again.
        unsafe { ManuallyDrop::take(func_ref) }
    }

    /// Unwraps the job into its return value.
    ///
    /// # Safety
    ///
    /// May only be called after `wait` has returned `false`, meaning the job completed
    /// with a value.
    #[inline(always)]
    pub unsafe fn unwrap_output(mut self) -> T {
        // SAFETY: We have exclusive access to the active union field `output`.
        // We take `self` by value, so no other `unwrap_*` method can also hold
        // it, and `wait` returned `false`, so `execute` ran (by the invariant on
        // `completed`, only `execute` sets the latch). Since `execute` runs at
        // most once, it is no longer running and cannot alias `data`.
        //
        // The caller has observed completion via `wait`, whose `Acquire` load
        // synchronizes with the `Release` store in the `Latch::set` call inside
        // `execute`. Since `execute` writes the `output` field before that
        // store, that write must happen-before this read. So there can be no
        // data-race on this load.
        //
        // `output` is the active variant because `wait` returning `false` means
        // `execute` called `set` with `error_flag == false`, which always
        // follows a write of the `output` field, after which the union is not
        // written again.
        let output_ref = unsafe { &mut self.data.get_mut().output };
        // SAFETY: The `StackJob` is dropped at the end of this block, so `data`
        // is never accessed again.
        unsafe { ManuallyDrop::take(output_ref) }
    }

    /// Unwraps the job into an error.
    ///
    /// # Safety
    ///
    /// May only be called after `wait` has returned `true`, meaning the job completed
    /// with an error.
    #[inline(always)]
    pub unsafe fn unwrap_error(mut self) -> Box<dyn Any + Send> {
        // SAFETY: We have exclusive access to the active union field `error`.
        // We take `self` by value, so no other `unwrap_*` method can also hold
        // it, and `wait` returned `true`, so `execute` ran (by the invariant on
        // `completed`, only `execute` sets the latch). Since `execute` runs at
        // most once, it is no longer running and cannot alias `data`.
        //
        // The caller has observed completion via `wait`, whose `Acquire` load
        // synchronizes with the `Release` store in the `Latch::set` call inside
        // `execute`. Since `execute` writes the `error` field before that store,
        // that write must happen-before this read. So there can be no data-race
        // on this load.
        //
        // `error` is the active variant because `wait` returning `true` means
        // `execute` called `set` with `error_flag == true`, which always
        // follows a write of the `error` field, after which the union is not
        // written again.
        let error_ref = unsafe { &mut self.data.get_mut().error };
        // SAFETY: The `StackJob` is dropped at the end of this block, so `data`
        // is never accessed again.
        unsafe { ManuallyDrop::take(error_ref) }
    }

    /// Executes a `StackJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `this` is an aligned pointer to an initialized `StackJob<F, T>`, which
    ///   will not be invalidated until `wait` reports the job complete.
    ///
    /// * This function is called at most once on any `StackJob`.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The pointer `this` is non-null, aligned, and the caller
        // ensures it points to an initialized `StackJob`.
        //
        // `StackJobs` are always accessed immutably except for `unwrap_func`,
        // `unwrap_output`, and `unwrap_error`. The caller ensures these will not
        // race this call, so the pointer is valid for immutable access.
        let this = unsafe { this.cast::<Self>().as_ref() };
        // Create an abort guard. If the closure panics, this will convert the
        // panic into an abort. Doing so prevents use-after-free for other
        // elements of the stack.
        let abort_guard = unwind::AbortOnDrop;
        // Run the function and record the result. Produces a boolean flag that
        // is true in the event of a panic.
        let error_flag = {
            // SAFETY: Only `unwrap_func`, `unwrap_output` and `unwrap_error`
            // access `data`. Due to their individual safety contracts, they can
            // only be called in a way that will not race with this function, so
            // we must have unique access.
            let data_ref = unsafe { &mut *this.data.get() };
            // SAFETY: Each `StackJob` is constructed using field `func`, and
            // this is the only place we write to the union after construction.
            // As this function is called at most once, it must still be valid
            // to access the union with field `func`.
            let func_ref = unsafe { &mut data_ref.func };
            // SAFETY: The `func` field is overwritten by the following match
            // block, so it will not be accessed again.
            let func = unsafe { ManuallyDrop::take(func_ref) };
            // Run the job. If the job panics, we propagate the panic back to the
            // main thread.
            let result = unwind::halt_unwinding(|| func(worker));
            // Emit different signals depending on if the function completed
            // successfully or panicked.
            match result {
                Ok(output) => {
                    data_ref.output = ManuallyDrop::new(output);
                    false
                }
                Err(error) => {
                    data_ref.error = ManuallyDrop::new(error);
                    true
                }
            }
        };
        // This publishes the write to `data` with a `Release` store, so any
        // thread that later observes completion through `wait` can load `data`
        // without a race.
        //
        // SAFETY: This casts a reference to a raw pointer, which means the
        // pointer must be aligned, non-null, and point to an initialized latch.
        //
        // We also meet Variant 2 of the `set` safety contract:
        //
        // * The latch has not been `set` since it was created or last `reset`,
        //   and calls to `set` do not race.
        //
        //   By the invariant on `completed`, only this `execute` sets the latch,
        //   and it runs at most once, so no prior or racing `set` exists.
        //
        // * The latch will not be dropped or moved until after `check` returns
        //   something other than `Pending`.
        //
        //   The owner does not drop the `StackJob` until `wait` returns, which
        //   happens only once the latch is set, and nothing removes the latch
        //   from the `StackJob`.
        unsafe { Latch::set(&this.completed, error_flag) };
        // Forget the abort guard, re-enabling panics.
        core::mem::forget(abort_guard);
    }
}

// -----------------------------------------------------------------------------
// Heap allocated work function

/// Represents a job stored in the heap. Used to implement `scope` and `spawn`.
///
/// This is analogous to the rayon type `HeapJob`. There is no corresponding
/// chili type.
pub struct HeapJob<F> {
    f: F,
}

impl<F> HeapJob<F>
where
    F: FnOnce(&Worker),
{
    /// Allocates a new `HeapJob` on the heap.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the function `f` does not unwind.
    #[inline(always)]
    pub unsafe fn new(f: F) -> Box<Self> {
        Box::new(HeapJob { f })
    }

    /// Converts the heap job into an "owning" `JobRef`. The job will be
    /// automatically dropped when the `JobRef` is executed (or will leak if it
    /// is not executed).
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * The `JobRef` will not outlive any of the items closed over by the
    ///   function `f`.
    ///
    /// * If `f` is `!Send` then `JobRef::execute` is only called on the thread
    ///   where the `HeapJob` was constructed.
    #[inline(always)]
    pub unsafe fn into_job_ref(self: Box<Self>) -> JobRef {
        // SAFETY: Pointers produced by `Box::into_raw` are never null.
        let job_pointer =
            unsafe { NonNull::new_unchecked(Box::into_raw(self)).cast() };
        // SAFETY: The doc-comment for this function defines the conditions
        // under which this `JobRef` will be considered "executable".
        //
        // We must now show that it is sound to call `HeapJob::execute` on
        // `job_ref` under these conditions, which in turn requires that:
        //
        // * `job_pointer` is an aligned pointer to an initialized `HeapJob`.
        //
        //   We created it with `Box::into_raw(self)`, which yields an aligned,
        //   non-null pointer to the initialized `HeapJob` formerly owned by the
        //   `Box`, so it must be.
        //
        // * `HeapJob::execute` is called at most once on any `HeapJob`.
        //
        //   `into_job_ref` converts the `HeapJob` into a `JobRef`, and
        //   `JobRef::execute` consumes the `JobRef` to call `HeapJob::execute`,
        //   so it can be called at most once.
        //
        // * This function is only called during the lifetime of the items
        //   closed over by the function.
        //
        //   The `JobRef` is not allowed to outlive the items closed over by the
        //   function, so `JobRef::execute` and hence `HeapJob::execute` can
        //   only be called during that interval.
        //
        // * Using or dropping `f` will not violate a `!Send` requirement.
        //
        //   This function's `!Send` safety clause confines `execute` (the only
        //   code that uses or drops `f`) to the construction thread. An
        //   unexecuted `JobRef` leaks the `HeapJob` (no `Drop` impl), so `f` is
        //   never dropped elsewhere.
        //
        // `JobRef::new` also requires that the execute function not unwind.
        // This is left to the caller of `HeapJob::new` to ensure.
        unsafe { JobRef::new(job_pointer, Self::execute) }
    }

    /// Executes a `Box<HeapJob>`, dropping it when completed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `this` was produced by `Box::into_raw` on a `Box<HeapJob<F>>`, and this
    ///   call takes ownership of that allocation.
    ///
    /// * This function is called at most once on any `HeapJob`.
    ///
    /// * Any items the `HeapJob` closes over are still live.
    ///
    /// * If the `HeapJob` is `!Send` then this is called on the thread where
    ///   the `HeapJob` was constructed.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The first clause satisfies the creation and
        // allocation-ownership requirements of `Box::from_raw`, and the
        // at-most-once clause rules out a prior reclaim.
        let this = unsafe { Box::from_raw(this.cast::<Self>().as_ptr()) };
        // Run the job.
        (this.f)(worker);
    }
}
