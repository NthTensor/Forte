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

use alloc::{boxed::Box, collections::VecDeque};
use core::{mem::ManuallyDrop, ptr::NonNull};

use crate::{platform::*, signal::Signal, thread_pool::Worker};

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
    #[inline]
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
    #[inline]
    pub fn id(&self) -> impl Eq + use<> {
        (self.job_pointer, self.execute_fn)
    }

    /// Executes the `JobRef` by passing the execute function on the job pointer.
    #[inline]
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
    job_refs: UnsafeCell<VecDeque<JobRef>>,
}

impl JobQueue {
    pub fn new() -> JobQueue {
        JobQueue {
            job_refs: UnsafeCell::new(VecDeque::new()),
        }
    }

    #[inline(always)]
    pub fn push_back(&self, job_ref: JobRef) {
        let job_refs = self.job_refs.get_mut();

        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        unsafe { job_refs.deref().push_back(job_ref) };
    }

    #[inline(always)]
    pub fn pop_back(&self) -> Option<JobRef> {
        let job_refs = self.job_refs.get_mut();

        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        unsafe { job_refs.deref().pop_back() }
    }

    #[inline(always)]
    pub fn pop_front(&self) -> Option<JobRef> {
        let job_refs = self.job_refs.get_mut();

        // SAFETY: The queue itself is only access mutably within `push_back`,
        // `pop_back` and `pop_front`. Since these functions never call each
        // other, we must have exclusive access to the queue.
        unsafe { job_refs.deref().pop_front() }
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
    signal: Signal<T>,
}

impl<F, T> StackJob<F, T>
where
    F: FnOnce(&Worker) -> T + Send,
    T: Send,
{
    /// Creates a new `StackJob` and returns it directly.
    pub fn new(f: F) -> StackJob<F, T> {
        StackJob {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
            signal: Signal::new(),
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
    /// This must not be called after `execute`.
    #[inline(always)]
    pub unsafe fn unwrap(self) -> F {
        let f_ptr = self.f.get_mut();
        // SAFETY: This takes ownership, ensuring we have exclusive access that
        // will not be used again, given that `execute` has not already been
        // called.
        unsafe { ManuallyDrop::take(f_ptr.deref()) }
    }

    /// Returns a reference to the signal embedded in this stack job. The
    /// closure's return value is sent over this signal after the job is
    /// executed.
    #[inline(always)]
    pub fn signal(&self) -> &Signal<T> {
        &self.signal
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
    /// The caller must ensure that `this` can be converted into an immutable
    /// reference to a `StackJob`. If the stack job is allocated on the stack
    /// (and it should be) then this amounts to ensuring this is called before
    /// the stack frame containing the allocation is popped. Put another way:
    /// don't leave a block where you allocate a stack job until you run it.
    ///
    /// This function must be called only once.
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` can be converted into an immutable
        // reference.
        let this = unsafe { this.cast::<Self>().as_ref() };
        // Access the function pointer mutably via the unsafe cell.
        let f_ptr = this.f.get_mut();
        // SAFETY: This memory location is accessed only in this function and in
        // `unwrap`. The latter cannot have been called, because it drops the
        // stack job, so, since this function is called only once, we can
        // guarantee that we have exclusive access which will never be used again.
        let f = unsafe { ManuallyDrop::take(f_ptr.deref()) };
        // Run the job.
        let result = f(worker);
        // SAFETY: This is valid for the access used by `send` because
        // `&this.signal` is an immutable reference to a `Signal`. Because
        // `send` is only called in this function, and this function is never
        // called again, `send` is never called again.
        unsafe { Signal::send(&this.signal, result) }
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
    unsafe fn execute(this: NonNull<()>, worker: &Worker) {
        // SAFETY: The caller ensures `this` was created by `Box::into_raw` and
        // that this is called only once.
        let this = unsafe { Box::from_raw(this.cast::<Self>().as_ptr()) };
        // Run the job.
        (this.f)(worker);
    }
}
