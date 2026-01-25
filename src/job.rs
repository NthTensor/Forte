//! This module defines an executable unit of work called a [`Job`]. Jobs are what
//! get scheduled on the thread pool. There are two core job types: [`StackJob`]
//! and [`HeapJob`].
//!
//! After a job is allocated, we typically refer to it by a [`JobRef`]. Job refs
//! are type-erased, and can be sent between threads without moving the
//! underlying job.
//!
//! When using a job, one must be extremely careful to ensure that:
//! (a) The job does not outlive anything it closes over.
//! (b) The job remains valid until it is executed for the last time.
//! (c) Each job reference is executed exactly once.

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
use core::sync::atomic::Ordering;
use core::sync::atomic::fence;
use std::thread::Result as ThreadResult;

use crate::latch::Latch;
use crate::platform::AtomicU32;
use crate::thread_pool::Worker;
use crate::unwind;

// -----------------------------------------------------------------------------
// Runnable

/// A job is a unit of work that may be executed by a worker thread. The primary
/// purpose of this trait is to make it easy to create a `JobRef`. The `execute`
/// function is designed to interlock with the `JobRef::execute_fn` field.
trait Job {
    /// Calling this function runs the job.
    ///
    /// # Safety
    ///
    /// Implementors must specify the invariant of the pointer `this` that the
    /// caller is expected to uphold.
    ///
    /// This may be called from a different thread than the one which scheduled
    /// the job, so the implementer must ensure the appropriate traits are met,
    /// whether `Send`, `Sync`, or both.
    ///
    /// Calling this is always considered to "complete" the job, so the caller
    /// must ensure this is called exactly once.
    unsafe fn execute(this: NonNull<()>, worker: &Worker);
}

// -----------------------------------------------------------------------------
// Shared JobRef

/// Effectively a Job trait object. It can be treated as such, even though
/// sometimes a `JobRef` will not point to a type that implements `Job`.
///
/// This is analogous to the chili type `JobShared` or the rayon type `JobRef`.
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
    /// * `job_pointer` and `execute_fn` are *matched*; the `execute_fn` must be
    ///   a function that can safely receive `job_pointer` as it's first argument.
    ///
    /// * `job_pointer` points to an initialized and properly aligned value which
    ///   is neither moved nor dropped until `execute_fn` is called.
    ///
    /// * `job_pointer` is "valid" now and until `execute_fn` is called,
    ///   according to the contract of the specific `execute_fn` being stored.
    #[inline(always)]
    pub unsafe fn new_raw(
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
        // SAFETY: Calling this function on this pointer is valid due to the
        // contract of `JobRef::new_raw`:
        //
        // * `self.execute_fn` and `self.job_pointer` are "matched": every
        //   `JobRef` is constructed via `new_raw`, which requires the caller
        //   to supply a compatible pair.
        //
        // * `self.job_pointer` is valid at this point: `new_raw` requires the
        //   pointer to remain valid until `execute_fn` is called, and we are
        //   calling it now.
        //
        // * This is called at most once: `execute` consumes `self`, so the
        //   pointer cannot be used again via this `JobRef`.
        unsafe { (self.execute_fn)(self.job_pointer, worker) }
    }
}

// SAFETY: `JobRef` is a type-erased data pointer + function pointer tuple. The
// data pointer always points to a `Send` value due to the safety requirements
// of `JobRef::new_raw`. Function pointers are always `Send`. Therefore it is
// sound to move a `JobRef` across thread boundaries.
unsafe impl Send for JobRef {}

// -----------------------------------------------------------------------------
// Job queue

/// A queue of jobs. This is a simple wrapper around a vec dequeue that uses
/// inner mutation, and has some more intiuitively named methods to enforce
/// conventions.
pub struct JobQueue {
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
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.push_back(job_ref);
    }

    /// Insert a job at the front of the queue (the side with the oldest jobs).
    pub fn push_old(&self, job_ref: JobRef) {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.push_front(job_ref);
    }

    /// Removes the newest job in the queue.
    pub fn pop_newest(&self) -> Option<JobRef> {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_back()
    }

    /// Removes the oldest job in the queue.
    pub fn pop_oldest(&self) -> Option<JobRef> {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_front()
    }

    /// Attempt to remove the given job-ref from the back of the queue.
    #[inline(always)]
    pub fn recover_newest(&self, id: (usize, usize)) -> bool {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
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
    /// the newest jobs). Each chunk is of size `CHUNK_SIZE`. After, At most
    /// `CHUNK_SIZE` jobs will be left in the queue.
    pub fn split(&self) -> Vec<VecDeque<JobRef>> {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
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
    /// queue. Jobs are added to the end (the side with the newst jobs).
    pub fn append(&self, mut split_refs: VecDeque<JobRef>) {
        // SAFETY: `JobQueue` is not `Sync`, so this can only be called from one
        // thread. We ensure no other references to the inner value exist by not
        // returning any references from this API, making this exclusive access
        // safe.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.append(&mut split_refs);
    }
}

// -----------------------------------------------------------------------------
// Stack allocated work function

/// A [`StackJob`] is a job that's allocated on the stack.
///
/// This is analogous to the chili type `JobStack` and the rayon type `StackJob`.
pub struct StackJob<F, T> {
    f: UnsafeCell<ManuallyDrop<F>>,
    completed: Latch,
    return_value: UnsafeCell<MaybeUninit<ThreadResult<T>>>,
}

impl<F, T> StackJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Creates a new `StackJob` owned by the current worker.
    #[inline(always)]
    pub fn new(f: F, worker: &Worker) -> StackJob<F, T> {
        StackJob {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
            completed: worker.new_latch(),
            return_value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Creates a `JobRef` pointing to this job. The underlying `StackJob` is
    /// not dropped after the `JobRef` is executed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * The `StackJob` will outlive the `JobRef`.
    ///
    /// * The `StackJob` will not move for the lifetime of the `JobRef`.
    ///
    /// * The `StackJob` does not outlive any data it closes over.
    ///
    /// * This function is not called again so long as the `JobRef` lives.
    #[inline(always)]
    pub unsafe fn as_job_ref(&self) -> JobRef {
        let job_pointer = NonNull::from(self).cast();
        // SAFETY: `JobRef::new_raw` requires:
        //
        // * `job_pointer` and `Self::execute` are matched.
        //
        //   Here, `execute` expects a pointer to `Self`, which is what
        //   `job_pointer` is.
        //
        // * The pointee is live, not moved, and not dropped until `execute_fn`
        //   is called.
        //
        //   Here, the caller guarantees the `StackJob` outlives and does not
        //   move for the lifetime of the `JobRef`.
        //
        // * `execute_fn` to be called at most once.
        //
        //   Here, `JobRef::execute` consumes the `JobRef`, and only one
        //   `JobRef` is created per `StackJob`, so it is called exactly once.
        unsafe { JobRef::new_raw(job_pointer, Self::execute) }
    }

    /// Returns a reference to the latch embedded in this stack job. After this
    /// latch is set, it becomes safe to call `StackJob::return_value`.
    #[inline(always)]
    pub fn completion_latch(&self) -> &Latch {
        &self.completed
    }

    /// Unwraps the stack job back into a closure. This allows the closure to be
    /// executed without indirection in situations where the one still has
    /// direct access.
    ///
    /// # Safety
    ///
    /// The caller must ensure that either this function or `execute` are called
    /// for a given `StackJob` (not both), and that this function must not be
    /// called multiple times.
    #[inline(always)]
    pub unsafe fn unwrap(&mut self) -> F {
        let f_mut = self.f.get_mut();
        // SAFETY: `ManuallyDrop` requires us to ensure that it is not used
        // again after we `take()` it's contents.
        //
        // `take()` is called in two places: once here, and once in `execute`.
        // Since this function is mutually exclusive with `execute`, and is
        // called at most once, the `ManuallyDrop<F>` is not used again.
        unsafe { ManuallyDrop::take(f_mut) }
    }

    /// Unwraps the job into it's return value.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * This is called only after the job's latch is set.
    ///
    /// * That this is called at most once for a given `StackJob`.
    #[inline(always)]
    pub unsafe fn return_value(&mut self) -> ThreadResult<T> {
        // Synchronize with the fence in `StackJob::execute`, establishing a
        // happens-after relationship with the following read..
        fence(Ordering::Acquire);
        // Get a ref to the result.
        let result_ref = self.return_value.get_mut();
        // SAFETY: `assume_init_read` requires:
        //
        // * The `MaybeUninit` is fully initialized.
        //
        //   As this function can only be called if the latch has been set, and
        //   the latch is only set at the end of `StackJob::execute` (after
        //   `return_value` is written and memory is synchronized via the above
        //   fence) the memory must be initialized.
        //
        // * That data not be incorrectly duplicated by repeated calls.
        //
        //   Data is not duplicated because this function is called at most once.
        unsafe { result_ref.assume_init_read() }
    }
}

impl<F, T> Job for StackJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Executes a `StackJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `this` is a non-null, properly aligned pointer to a live instance of
    ///   `StackJob<F, T>`.
    ///
    /// * The `StackJob` will not move or be deallocated until the latch it
    ///   contains is set.
    ///
    /// * Either this function or `unwrap` are called at most once for a given
    ///   `StackJob`.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` can be converted into an immutable
        // reference until we set the latch, and the latch has not yet been set.
        let this = unsafe { this.cast::<Self>().as_ref() };
        // Create an abort guard. If the closure panics, this will convert the
        // panic into an abort. Doing so prevents use-after-free for other
        // elements of the stack.
        let abort_guard = unwind::AbortOnDrop;
        // SAFETY: `f` is a `UnsafeCell<ManuallyDrop<F>>`. Creating a
        // `&mut ManuallyDrop<F>` is only sound so long as no other live
        // references exist.
        //
        // `f` is accessed mutably in two places: once here, and once in
        // `unwrap`. Since this function is mutually exclusive with `unwrap`,
        // and is called at most once, exclusive access is guaranteed.
        let f_ref = unsafe { &mut *this.f.get() };
        // SAFETY: `ManuallyDrop` requires us to ensure that it is not used
        // again after we `take()` it's contents.
        //
        // `take()` is called in two places: once here, and once in `unwrap`.
        // Since this function is mutually exclusive with `unwrap`, and is
        // called at most once, the `ManuallyDrop<F>` is not used again.
        let f = unsafe { ManuallyDrop::take(f_ref) };
        // Run the job. If the job panics, we propagate the panic back to the
        // main thread.
        let result = unwind::halt_unwinding(|| f(worker));
        // Get the uninitialized memory where we should put the return value.
        let return_value = this.return_value.get();
        // SAFETY: Writing to this unsafe cell requires that no other thread
        // holds a reference to it's contents.
        //
        // The `return_value` is only written here and only read within
        // `StackJob::return_value`, and then only after the latch has been set.
        // The latch has not been set, and this function is called at most once,
        // so no concurrent access can occur.
        unsafe { (*return_value).write(result) };
        // This syncrhonizies with the `Acquire` fence within `return_value()`,
        // establishing a happens-before relationship that makes the preceding
        // `return_value` write vsibile to the reader.
        //
        // This is required because latches do not synchronize memory.
        fence(Ordering::Release);
        // SAFETY: The caller ensures the job is valid until the latch is set.
        // Since the latch is a field of the job, the latch must be valid until
        // it is set.
        unsafe { Latch::set(&this.completed) };
        // Forget the abort guard, re-enabling panics.
        core::mem::forget(abort_guard);
    }
}

// -----------------------------------------------------------------------------
// Stack allocated work function on a non-worker thread

/// Like [`StackJob`] but allocated on the stack of a non-worker thread. While
/// this job is pending, the owning thread is fully blocked.
#[cfg(not(feature = "shuttle"))]
pub struct ExternalJob<F, T> {
    f: UnsafeCell<ManuallyDrop<F>>,
    completed: AtomicU32,
    return_value: UnsafeCell<MaybeUninit<ThreadResult<T>>>,
}

#[cfg(not(feature = "shuttle"))]
impl<F, T> ExternalJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Creates a new `ExternalJob`.
    #[inline(always)]
    pub fn new(f: F) -> ExternalJob<F, T> {
        ExternalJob {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
            completed: AtomicU32::new(0),
            return_value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// Creates a `JobRef` pointing to this job. The underlying `ExternalJob` is
    /// not dropped after the `JobRef` is executed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * The `ExternalJob` will not move or be deallocated until the `JobRef`
    ///   is executed.
    ///
    /// * The `JobRef` does not outlive any data the `ExternalJob` closes over.
    ///
    /// * This function is not called again so long as the `JobRef` lives.
    #[inline(always)]
    pub unsafe fn as_job_ref(&self) -> JobRef {
        let job_pointer = NonNull::from(self).cast();
        // SAFETY: The `job_pointer` is trivially aligned and non-null,
        // because it is derived from a reference.
        //
        // The caller must not allow the `ExternalJob` to move or be deallocated
        // until the `JobRef` is executed. This guarantees that `job_pointer`
        // remains valid for the lifetime of `JobRef`, satisfying the
        // requirements of `JobRef::new_raw`.
        //
        // The caller guarantees that this function is not called again while
        // `JobRef` lives, so `Self::execute` can be called at most once for
        // this particular `ExternalJob`. This satisfies the at-most-once
        // execution invariant documented on `Job::execute`.
        unsafe { JobRef::new_raw(job_pointer, Self::execute) }
    }

    /// Waits for the `ExternalJob` to be executed and returns the result.
    ///
    /// # Safety
    ///
    /// This must be called at most once.
    #[inline(always)]
    pub unsafe fn wait_for_value(&mut self) -> ThreadResult<T> {
        // Wait for the complete flag to be set.
        loop {
            atomic_wait::wait(&self.completed, 0);
            if self.completed.load(Ordering::Relaxed) == 1 {
                break;
            }
        }
        // Synchronize memory; we do this with a fence, so that we only do a
        // relaxed load in the case of a spurious wakeup.
        fence(Ordering::Acquire);
        // Get a ref to the result.
        let result_ref = self.return_value.get_mut();
        // SAFETY: `assume_init_read` requires:
        //
        // * The `MaybeUninit` is fully initialized.
        //
        //   As this can only be called if we have observed that `completed` has
        //   been set to 1, and that only happens at the end of
        //   `ExternalJob::execute` (after `return_value` is written and memory
        //   is synchronized via the above fence) the memory must be initialized.
        //
        // * That data not be incorrectly duplicated by repeated calls.
        //
        //   Data is not duplicated because this function is called at most
        //   once.
        unsafe { result_ref.assume_init_read() }
    }
}

#[cfg(not(feature = "shuttle"))]
impl<F, T> Job for ExternalJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Executes an `ExternalJob` from a const pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `this` is a non-null, properly aligned pointer to a live instance
    ///   of `ExternalJob<F, T>`.
    ///
    /// * The `ExternalJob` will not move or be deallocated for as long as
    ///   `completed` remains set to 0.
    ///
    /// * This function is called at most once for a given `ExternalJob`.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` can be converted into an immutable
        // reference until we set the `complete` atomic.
        let this = unsafe { this.cast::<Self>().as_ref() };
        // Create an abort guard. If the closure panics, this will convert the
        // panic into an abort. Doing so prevents use-after-free for other
        // elements of the stack.
        let abort_guard = unwind::AbortOnDrop;
        // SAFETY: `f` is a `UnsafeCell<ManuallyDrop<F>>`. Creating a
        // `&mut ManuallyDrop<F>` is only sound so long as no other live
        // references exist.
        //
        // Since this field is never access mutably except for here and this
        // function is called at most once, exclusive access is guaranteed.
        let f_ref = unsafe { &mut *this.f.get() };
        // SAFETY: `ManuallyDrop` requires us to ensure that it is not used
        // again after we `take()` it's contents.
        //
        // Since it is not used in the remainder of this function, and this
        // function is called at most once, it is indeed not used again.
        let f = unsafe { ManuallyDrop::take(f_ref) };
        // Run the job. If the job panics, we propagate the panic back to the
        // main thread.
        let result = unwind::halt_unwinding(|| f(worker));
        // Get the uninitialized memory where we should put the return value.
        let return_value = this.return_value.get();
        // SAFETY: Writing to this unsafe cell requires that no other thread
        // holds a reference to it's contents.
        //
        // The `return_value` is only read within `ExternalJob::wait_for_value`,
        // and then only after `completed` is set to 1. Since this function is
        // called at most once, `completed` must still be set to 0. Therefore no
        // concurrent access can occur.
        unsafe { (*return_value).write(result) };
        // Set `completed` to 1, allowing reads of the return value. This
        // `Release` store synchronizes with the `Acquire` fence in
        // `ExternalJob::wait_for_value`, establishing a happens-before
        // relationship that makes the preceding `return_value` write visible
        // to the waiting reader.
        this.completed.store(1, Ordering::Release);
        // Notify the waiting thread that the job is complete.
        atomic_wait::wake_one(&this.completed);
        // Forget the abort guard, re-enabling panics.
        core::mem::forget(abort_guard);
    }
}

#[cfg(feature = "shuttle")]
pub struct ExternalJob<F, T> {
    f: UnsafeCell<ManuallyDrop<F>>,
    mutex: shuttle::sync::Mutex<Option<ThreadResult<T>>>,
    condvar: shuttle::sync::Condvar,
}

#[cfg(feature = "shuttle")]
impl<F, T> ExternalJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Creates a new `ExternalJob`.
    #[inline(always)]
    pub fn new(f: F) -> ExternalJob<F, T> {
        ExternalJob {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
            mutex: shuttle::sync::Mutex::new(None),
            condvar: shuttle::sync::Condvar::new(),
        }
    }

    #[inline(always)]
    #[allow(clippy::undocumented_unsafe_blocks)]
    pub unsafe fn as_job_ref(&self) -> JobRef {
        let job_pointer = NonNull::from(self).cast();
        unsafe { JobRef::new_raw(job_pointer, Self::execute) }
    }

    #[inline(always)]
    pub unsafe fn wait_for_value(&mut self) -> ThreadResult<T> {
        let mut value = self.mutex.lock().unwrap();
        while value.is_none() {
            value = self.condvar.wait(value).unwrap();
        }
        Option::take(&mut value).unwrap()
    }
}

#[cfg(feature = "shuttle")]
impl<F, T> Job for ExternalJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    #[inline(always)]
    #[allow(clippy::undocumented_unsafe_blocks)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        let this = unsafe { this.cast::<Self>().as_ref() };
        let abort_guard = unwind::AbortOnDrop;
        let f_ref = unsafe { &mut *this.f.get() };
        let f = unsafe { ManuallyDrop::take(f_ref) };
        let result = unwind::halt_unwinding(|| f(worker));
        let mut value = this.mutex.lock().unwrap();
        *value = Some(result);
        this.condvar.notify_one();
        core::mem::forget(abort_guard);
    }
}

// -----------------------------------------------------------------------------
// Heap allocated work function

/// Represents a job stored in the heap. Used to implement `scope` and `spawn`.
///
/// This is analogous to the rayon type `HeapJob`. There is no corresponding chili type.
pub struct HeapJob<F> {
    f: F,
}

impl<F> HeapJob<F>
where
    F: FnOnce(&Worker) + Send,
{
    /// Allocates a new `HeapJob` on the heap.
    #[inline(always)]
    pub fn new(f: F) -> Box<Self> {
        Box::new(HeapJob { f })
    }

    /// Converts the heap job into an "owning" `JobRef`. The job will be
    /// automatically dropped when the `JobRef` is executed.
    ///
    /// This will leak memory if the `JobRef` is not executed, so the caller
    /// must ensure that it is eventually executed (unless the process is
    /// exiting).
    ///
    /// # Safety
    ///
    /// If the `JobRef` is executed, the caller must ensure that it has not
    /// outlived the data it closes over. In other words, if the closure
    /// references something, that thing must live until the `JobRef` is
    /// executed or dropped.
    #[inline(always)]
    pub unsafe fn into_job_ref(self: Box<Self>) -> JobRef {
        // SAFETY: Pointers produced by `Box::into_raw` are never null.
        let job_pointer = unsafe { NonNull::new_unchecked(Box::into_raw(self)).cast() };

        // SAFETY: The pointer was created by a call to `Box::into_raw` so it is
        // valid to pass in to `Self::execute`.
        //
        // Because this function takes ownership of `Self` to produce a
        // `JobRef`, `JobRef::execute` takes ownership of the `JobRef` to call
        // `Self::execute`, the job_pointer cannot be used after `Self::execute`
        // is called. So it is safe for the pointer to become dangling.
        unsafe { JobRef::new_raw(job_pointer, Self::execute) }
    }
}

impl<F> Job for HeapJob<F>
where
    F: FnOnce(&Worker) + Send,
{
    /// Executes a `Box<HeapJob>`, dropping it when completed.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `this` is a pointer, created by calling
    /// `Box::into_raw` on a `Box<HeapJob<F>>`. After the call `this` must be
    /// treated as dangling.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` was created by `Box::into_raw` and
        // that this is called only once.
        let this = unsafe { Box::from_raw(this.cast::<Self>().as_ptr()) };
        // Run the job.
        (this.f)(worker);
    }
}
