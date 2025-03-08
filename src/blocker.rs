//! Async blocking utilities.

use core::task::RawWaker;
use core::task::RawWakerVTable;
use core::task::Waker;

use crate::platform::*;

// -----------------------------------------------------------------------------
// States

/// The blocker is not sleeping, and has not been woken.
const IDLE: u32 = 0;

// The blocker is sleeping or is about to go to sleep.
const WAIT: u32 = 1;

// The blocker has been woken at least once since the last time it slept.
const WAKE: u32 = 2;

// -----------------------------------------------------------------------------
// Blocker

/// A blocker lets you block a thread on a the progress of a future.
pub struct Blocker {
    /// The state of a blocker.
    state: AtomicU32,
}

impl Blocker {
    /// Creates a new blocker.
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(IDLE),
        }
    }

    /// Creates an async waker from a signal. This can be used to schedule a
    /// signal when a future comples.
    ///
    /// # Safety
    ///
    /// The blocker must outlive the waker.
    pub unsafe fn as_waker(&self) -> Waker {
        let this: *const Self = self;
        let raw_waker = RawWaker::new(this.cast::<()>(), &RAW_WAKER_VTABLE);
        // SAFETY: The RawWakerVTable api contract is upheald and these
        // functions are all thread-safe.
        unsafe { Waker::from_raw(raw_waker) }
    }

    /// Returns true if calling `block` would have blocked the thread.
    #[inline]
    pub fn would_block(&self) -> bool {
        self.state.load(Ordering::Relaxed) != WAKE
    }

    // Blocks the thread until the future makes progress.
    #[inline]
    pub fn block(&self) {
        let state = self.state.swap(WAIT, Ordering::Relaxed);
        if state != WAKE {
            atomic_wait::wait(&self.state, state);
        }
        self.state.store(IDLE, Ordering::Relaxed);
    }
}

const RAW_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    #[inline(always)]
    |ptr| RawWaker::new(ptr, &RAW_WAKER_VTABLE),
    wake,
    wake,
    |_| {},
);

fn wake(this: *const ()) {
    // SAFETY: This was constructed to be non-null, and the blocker must outlive
    // the waker, and we do not ever access blockers mutabley, so it must be
    // valid to convert this into an imutable reference.
    let blocker = unsafe { &*this.cast::<Blocker>() };
    if blocker.state.swap(WAKE, Ordering::Relaxed) == WAIT {
        atomic_wait::wake_all(&blocker.state);
    }
}
