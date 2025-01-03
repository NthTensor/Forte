//! This module provides a toolbox of scheduling primitives called Latches.
//!
//! In the abstract, a latch is a way to block some logic from progressing on a
//! given thread. All latches begin "closed", blocking some logic. When the
//! latch is "opened", the logic progresses. The exact meaning of "closed" and
//! "open" depend on the latch in question.
//!
//! Most latches implement one or both of the two core traits: [`Latch`] and
//! [`Probe`].
//!
//! [`Latch`] represents the "write-side" of the api, which allows consumers to
//! open arbitrary latches (and thus unblock whatever logic is blocked using the
//! latch). It defines a single function, [`Latch::set`], which (possibly)
//! opens the latch.
//!
//! [`Probe`] represents the "read-side" of the api, which allows consumers to
//! test if is latch is closed and spin (or do something else) while waiting for
//! it to open. It defines a single method, [`Probe::probe`], which returns
//! a boolean to indicate if the latch is open.
//!
//! # Safety
//!
//! Latches are an inherently somewhat unsafe construct, because once a latch is
//! becomes "open", the logic it unblocks often deallocates the lock. Refer to
//! specific safety comments for more information.

use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    task::Wake,
};

use parking_lot::{Condvar, Mutex};

use crate::thread_pool::{ThreadPool, WorkerThread};

// -----------------------------------------------------------------------------
// Latches and probes

/// This trait represents the "write-side" of the latch api. It exists to allow
/// consumers to open arbitrary latches (and thus unblock whatever logic is
/// blocked using the latch).
///
/// Latches may choose not implement this trait if they cannot be opened without
/// additional context.
pub trait Latch {
    /// Possibly opens the latch. Calling this method does not have to open the
    /// latch, but there should be some situation in which it does.
    ///
    /// # Safety
    ///
    /// Opening a latch triggers other threads to wake up and (in some cases)
    /// complete. This may, in turn, cause memory to be deallocated and so
    /// forth.
    ///
    /// This function operates on `*const Self` instead of `&self` to allow it
    /// to become dangling during this call. The caller must ensure that the
    /// pointer is valid upon entry, and not invalidated during the call by any
    /// actions other than `set` itself.
    ///
    /// The implementer can assume the pointer is valid when passed into the
    /// function, but must not assume the pointer is valid after the latch is
    /// "opened" (eg. after whatever side-effect can cause logic elsewhere to
    /// progress). It's typically better to read all the fields you will need to
    /// access *before* a latch is set!
    unsafe fn set(this: *const Self);
}

/// This trait represents the "read-side" of the latch api. It exists to allow
/// consumers to check is a latch is open.
///
/// Latches may choose not to implement this if they do not support polling (for
/// example if they use a mutex to block a thread).
pub trait Probe {
    /// Returns `true` if the latch is open, and `false` if it is closed.
    fn probe(&self) -> bool;
}

// -----------------------------------------------------------------------------
// Atomic latch

/// A simple latch implemented using an atomic bool.
pub struct AtomicLatch {
    /// The state of the latch, `true` for open and `false` for closed.
    state: AtomicBool,
}

impl AtomicLatch {
    /// Creates a new closed latch.
    #[inline]
    pub const fn new() -> Self {
        Self {
            state: AtomicBool::new(false),
        }
    }

    /// Resets the latch back to closed.
    #[inline]
    pub fn reset(&self) {
        self.state.store(false, Ordering::Release);
    }
}

impl Default for AtomicLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Latch for AtomicLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. We do not use
        // it after the side-effects (in this case an atomic store) so it is
        // fine if the pointer becomes dangling.
        unsafe {
            (*this).state.store(true, Ordering::Release);
        }
    }
}

impl Probe for AtomicLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.state.load(Ordering::Acquire)
    }
}

// -----------------------------------------------------------------------------
// Wake latch

/// A simple wrapper around an `AtomicLatch` that can wake a worker when set.
pub struct WakeLatch {
    /// An internal atomic latch.
    atomic_latch: AtomicLatch,
    /// The thread pool where the thread lives.
    thread_pool: &'static ThreadPool,
    /// The index of the worker thread to wake when `set()` is called.
    thread_index: usize,
}

impl WakeLatch {
    /// Creates a new closed latch.
    #[inline]
    pub fn new(worker_thread: &WorkerThread) -> WakeLatch {
        WakeLatch {
            atomic_latch: AtomicLatch::new(),
            thread_pool: worker_thread.thread_pool(),
            thread_index: worker_thread.index(),
        }
    }

    #[inline]
    pub const fn new_raw(thread_index: usize, thread_pool: &'static ThreadPool) -> WakeLatch {
        WakeLatch {
            atomic_latch: AtomicLatch::new(),
            thread_pool,
            thread_index,
        }
    }

    /// Resets the latch back to closed.
    #[inline]
    pub fn reset(&self) {
        self.atomic_latch.reset();
    }
}

impl Latch for WakeLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: The thread pool itself is static, so we need only be
        // concerned with the lifetime of the pointer. Since we assume it is
        // valid when passed in and do not use it after the side effects, it is
        // fine if it becomes dangling.
        unsafe {
            let thread_pool = (*this).thread_pool;
            let thread_index = (*this).thread_index;
            Latch::set(&(*this).atomic_latch);
            thread_pool.wake_thread(thread_index);
        }
    }
}

impl Probe for WakeLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.atomic_latch.probe()
    }
}

// -----------------------------------------------------------------------------
// Mutex-lock latch

/// A latch that can be used to block a thread, implemented using a mutex.
pub struct LockLatch {
    mutex: Mutex<bool>,
    cond: Condvar,
}

impl LockLatch {
    /// Creates a new closed latch.
    #[inline]
    pub const fn new() -> LockLatch {
        LockLatch {
            mutex: Mutex::new(false),
            cond: Condvar::new(),
        }
    }

    /// Waits for the latch to open by blocking the thread.
    pub fn wait(&self) {
        let mut guard = self.mutex.lock();
        while !*guard {
            self.cond.wait(&mut guard);
        }
    }

    /// Waits for the latch to open by blocking the thread, then sets it back to closed.
    pub fn wait_and_reset(&self) {
        let mut guard = self.mutex.lock();
        while !*guard {
            self.cond.wait(&mut guard);
        }
        *guard = false;
    }
}

impl Default for LockLatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Latch for LockLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. Side-effects
        // are not transmitted until the `notify_all` call at the very end, so
        // the pointer remains valid for the entire block.
        unsafe {
            let mut guard = (*this).mutex.lock();
            *guard = true;
            (*this).cond.notify_all();
        }
    }
}

// -----------------------------------------------------------------------------
// Counting latch

/// A counting latch stores a decrementing counter and only opens when the
/// counter reaches zero. This means that, unlike other latches, multiple calls
/// to `Latch::set` may be required to open the latch.
pub struct CountLatch {
    counter: AtomicUsize,
    latch: WakeLatch,
}

impl CountLatch {
    /// Creates a new closed latch with the specified count.
    #[inline]
    pub fn with_count(count: usize, owner: &WorkerThread) -> Self {
        Self {
            counter: AtomicUsize::new(count),
            latch: WakeLatch::new(owner),
        }
    }

    /// Increments the count. An additional call to `Latch::set()` will be
    /// required before the latch opens.
    #[inline]
    pub fn increment(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl Latch for CountLatch {
    #[inline]
    unsafe fn set(this: *const Self) {
        // SAFETY: We assume the pointer is valid when passed in. Side-effects
        // are not transmitted until the `Latch::set` call at the very end, so
        // it is fine if the pointer becomes dangling.
        unsafe {
            if (*this).counter.fetch_sub(1, Ordering::SeqCst) == 1 {
                Latch::set(&(*this).latch)
            }
        }
    }
}

impl Probe for CountLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.latch.probe()
    }
}

// -----------------------------------------------------------------------------
// Async set-on-wake

// An async task waker that sets a latch on wake.
pub struct SetOnWake<L>
where
    L: Latch,
{
    latch: L,
}

impl<L> SetOnWake<L>
where
    L: Latch,
{
    /// Creates a waker from a latch.
    pub fn new(latch: L) -> Arc<Self> {
        Arc::new(Self { latch })
    }

    /// Returns a reference to the inner latch.
    pub fn latch(&self) -> &L {
        &self.latch
    }
}

impl<L> Wake for SetOnWake<L>
where
    L: Latch,
{
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // SAFETY: The pointer passed to `Latch::set()` is valid and cannot be
        // invalidated while the arc is held.
        unsafe { Latch::set(&self.latch) };
    }
}
