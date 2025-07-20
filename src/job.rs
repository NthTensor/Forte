//! This module defines an executable unit of work called a [`Job`]. Jobs are what
//! get scheduled on the thread-pool. There are two core job types: [`StackJob`]
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
use arraydeque::ArrayDeque;
use core::cell::UnsafeCell;
use core::mem::{ManuallyDrop, MaybeUninit};
use core::ptr::NonNull;
use core::sync::atomic::{Ordering, fence};
use std::thread::Result as ThreadResult;

use crate::latch::Latch;
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
    /// Implements must specify the invariant of the pointer `this` that the
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
    /// This is usually point to an implementation of `Job::execute` (either
    /// `HeapJob::execute` or `StackJob::execute`). But it can contain other
    /// things as well.
    execute_fn: unsafe fn(NonNull<()>, &Worker),
}

impl JobRef {
    /// Creates a new `JobRef` from raw pointers.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `job_pointer` remains valid to pass to
    /// `execute_fn` until the job is executed. What exactly this means is
    /// dependent on the implementation of the execute function.
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
    pub fn id(&self) -> impl Eq + use<> {
        (self.job_pointer, self.execute_fn)
    }

    /// Executes the `JobRef` by passing the execute function on the job pointer.
    #[inline(always)]
    pub fn execute(self, worker: &Worker) {
        // SAFETY: The constructor of `JobRef` is required to ensure this is valid.
        unsafe { (self.execute_fn)(self.job_pointer, worker) }
    }
}

// SAFETY: !Send for raw pointers is not for safety, just as a lint.
unsafe impl Send for JobRef {}

// -----------------------------------------------------------------------------
// Job queue

pub struct JobQueue {
    job_refs: UnsafeCell<ArrayDeque<JobRef, 64>>,
}

impl JobQueue {
    pub fn new() -> JobQueue {
        JobQueue {
            job_refs: UnsafeCell::new(ArrayDeque::new()),
        }
    }

    #[inline(always)]
    pub fn push_back(&self, job_ref: JobRef) -> Option<JobRef> {
        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        if let Err(full) = job_refs.push_back(job_ref) {
            Some(full.element)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn pop_back(&self) -> Option<JobRef> {
        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_back()
    }

    #[inline(always)]
    pub fn pop_front(&self) -> Option<JobRef> {
        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        let job_refs = unsafe { &mut *self.job_refs.get() };
        job_refs.pop_front()
    }
}

// -----------------------------------------------------------------------------
// Stack allocated work function

/// A [`StackJob`] is a job that's allocated on the stack. It's efficient, but
/// relies on us preventing the stack frame from being dropped. Stack jobs are
/// used mainly for `join` and other blocking thread pool operations. They
/// also support explicit return values, transmitted via an attached signal.
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
    /// The caller must ensure that the `StackJob` that the returned `JobRef` refers
    /// to will live as long as the `JobRef`. The caller must also ensure that
    /// the `JobRef` does not outlive the data the `StackJob` closes over; which
    /// is to say, if the closure references something, that thing must exist at
    /// least until the `JobRef` is executed or dropped. Additionally, the
    /// caller must ensure that they never create two different `JobRef`s that
    /// point to the same `StackJob`.
    #[inline(always)]
    pub unsafe fn as_job_ref(&self) -> JobRef {
        let job_pointer = NonNull::from(self).cast();
        // SAFETY: The caller ensures the `StackJob` will outlive the `JobRef`,
        // so it will remain valid to convert this pointer into a reference, and
        // hence it is possible to pass this pointer to `Self::execute`.
        //
        // `Self::execute` cannot be called multiple times because
        // `JobRef::execute` takes ownership of the `JobRef`, and we only create
        // a single `JobRef` for each stack job.
        unsafe { JobRef::new_raw(job_pointer, Self::execute) }
    }

    /// Unwraps the stack job back into a closure. This allows the closure to be
    /// executed without indirection in situations where the one still has
    /// direct access.
    ///
    /// # Safety
    ///
    /// This may only be called before the job is executed.
    #[inline(always)]
    pub unsafe fn unwrap(mut self) -> F {
        // SAFETY: This will not be used again. Given that `execute` has not
        // already been, it will never be used twice.
        unsafe { ManuallyDrop::take(self.f.get_mut()) }
    }

    /// Returns a reference to the signal embedded in this stack job. The
    /// closure's return value is sent over this signal after the job is
    /// executed.
    #[inline(always)]
    pub fn completion_latch(&self) -> &Latch {
        &self.completed
    }

    /// Unwraps the job into it's return value.
    ///
    /// # Safety
    ///
    /// This may only be called after the job has finished executing.
    #[inline(always)]
    pub unsafe fn return_value(mut self) -> ThreadResult<T> {
        // Synchronize with the fence in `StackJob::execute`.
        fence(Ordering::Acquire);
        // Get a ref to the result.
        let result_ref = self.return_value.get_mut();
        // SAFETY: The job has completed, which means the return value must have
        // been initialized. This consumes the job, so there's no chance of this
        // accidentally duplicating data.
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
    /// The caller must ensure that `this` is valid to access a `StackJob`
    /// immutably at least until the `Latch` within the `StackJob` has been set.
    /// As a consequence, this may not be run after a latch has been set. Since
    /// this function sets the latch, the caller must ensure to only call this
    /// function once.
    #[inline(always)]
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` can be converted into an immutable
        // reference until we set the latch, and the latch has not yet been set.
        let this = unsafe { this.cast::<Self>().as_ref() };
        // Create an abort guard. If the closure panics, this will convert the
        // panic into an abort. Doing so prevents use-after-free for other elements of the stack.
        let abort_guard = unwind::AbortOnDrop;
        // SAFETY: This memory location is accessed only in this function and in
        // `unwrap`. The latter cannot have been called because it consumes the
        // stack job. And since this function is called only once, we can
        // guarantee that we have exclusive access.
        let f_ref = unsafe { &mut *this.f.get() };
        // SAFETY: The caller ensures this function is called only once.
        let f = unsafe { ManuallyDrop::take(f_ref) };
        // Run the job. If the job panics, we propagate the panic back to the main thread.
        let result = unwind::halt_unwinding(|| f(worker));
        // Get the uninitialized memory where we should put the return value.
        let return_value = this.return_value.get();
        // SAFETY: The return value is only accessed here and in
        // `StackJob::return_value`. Since the other method consumes the stack
        // job, it's not possible for it to run concurrently. Therefore, we must
        // have exclusive access to the return value.
        unsafe { (*return_value).write(result) };
        // Latches do not participate in memory ordering, so we need to do this manually.
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
    /// # Safety
    ///
    /// This will leak memory if the `JobRef` is not executed, so the caller
    /// must ensure that it is eventually executed (unless the process is
    /// exiting).
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
    /// `Box::into_raw` on a `Box<HeapJob>`. After the call `this` must be
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
