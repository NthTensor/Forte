//! A core concept in Rayon is the *latch*.

use core::{
    pin::Pin,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::platform::*;

// -----------------------------------------------------------------------------
// States

/// The default state of a latch is `LOCKED`. When in the locked state, `check`
/// returns `false` and `wait` blocks.
const LOCKED: u32 = 0b00;

/// The latch enters the `SIGNAL` state when it is set. When in this state,
/// `check` returns `true` and `wait` does not block.
const SIGNAL: u32 = 0b01;

/// The latch enters the `ASLEEP` state when blocking with `wait`.
const ASLEEP: u32 = 0b10;

// -----------------------------------------------------------------------------
// Latch

/// A [Latch] is a signaling mechanism used to indicate when an event has
/// occurred. The latch begins as *unset* (In the `LOCKED` state), and can later
/// be *set* by any thread (entering the *SIGNAL*) state.
///
/// Each latch is associated with one *owner thread*. This is the thread that
/// may be blocking, waiting for the latch to complete.
///
/// The general idea and spirit for latches (as well as some of the
/// documentation) is due to rayon. However the implementation is specific to
/// forte.
pub struct Latch {
    /// Holds the internal state of the latch. This tracks if the latch has been
    /// set or not.
    state: AtomicU32,
    /// The sleep controller for the owning thread.
    sleep_controller: &'static SleepController,
}

impl Latch {
    /// Creates a new latch, owned by a specific thread.
    pub fn new(sleep_controller: &'static SleepController) -> Latch {
        Latch {
            state: AtomicU32::new(LOCKED),
            sleep_controller,
        }
    }

    /// Checks to see if the latch has been set. Returns true if it has been.
    #[inline(always)]
    pub fn check(&self) -> bool {
        self.state.load(Ordering::Relaxed) == SIGNAL
    }

    /// Waits for the latch to be set. In actuality, this may be woken.
    ///
    /// Returns true if the latch signal was received, and false otherwise.
    #[inline(always)]
    pub fn wait(&self) -> bool {
        if self.state.load(Ordering::Relaxed) == SIGNAL {
            return true;
        }
        let slept = self.sleep_controller.sleep();
        if slept {
            self.state.load(Ordering::Relaxed) == SIGNAL
        } else {
            false
        }
    }

    /// Activates the latch, potentially unblocking the owning thread.
    ///
    /// This takes a raw pointer because the latch may be de-allocated by a
    /// different thread while this function is executing.
    ///
    /// # Safety
    ///
    /// The latch pointer must be valid when passed to this function, and must
    /// not be allowed to become dangling until after the latch is set.
    #[inline(always)]
    pub unsafe fn set(latch: *const Latch) {
        // SAFETY: At this point, the latch must still be valid to dereference.
        let sleep_controller = unsafe { (*latch).sleep_controller };
        // SAFETY: At this point, the latch must still be valid to dereference.
        unsafe { (*latch).state.store(SIGNAL, Ordering::Relaxed) };
        sleep_controller.wake();
    }

    /// Restores the latch to the default state.
    ///
    /// # Safety
    ///
    /// This may only be called when in the `SIGNAL` state, eg. after either `wait` or
    /// `check` has returned `true`.
    #[inline(always)]
    pub unsafe fn reset(&self) {
        self.state.store(LOCKED, Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------------
// Sleeper

/// Used, in combination with a latch to park and unpark threads.
pub struct SleepController {
    state: AtomicU32,
}

impl Default for SleepController {
    fn default() -> SleepController {
        SleepController {
            state: AtomicU32::new(LOCKED),
        }
    }
}

impl SleepController {
    pub fn wake(&self) -> bool {
        let sleep_state = self.state.swap(SIGNAL, Ordering::Relaxed);
        let asleep = sleep_state == ASLEEP;
        if asleep {
            atomic_wait::wake_one(&self.state);
        }
        asleep
    }

    pub fn sleep(&self) -> bool {
        let state = self.state.swap(ASLEEP, Ordering::Relaxed);
        let sleep = state == LOCKED;
        if sleep {
            atomic_wait::wait(&self.state, ASLEEP);
        }
        self.state.store(LOCKED, Ordering::Relaxed);
        sleep
    }
}

// -----------------------------------------------------------------------------
// Async waker

impl Latch {
    /// Creates an async waker from a reference to a latch.
    ///
    /// # Safety
    ///
    /// The latch must outlive the waker.
    pub unsafe fn as_waker(self: Pin<&Self>) -> Waker {
        let this: *const Self = Pin::get_ref(self);
        let raw_waker = RawWaker::new(this.cast::<()>(), &RAW_WAKER_VTABLE);
        // SAFETY: The RawWakerVTable api contract is upheald and these
        // functions are all thread-safe.
        unsafe { Waker::from_raw(raw_waker) }
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
    let latch = this.cast::<Latch>();
    // SAFETY: The latch must be valid for the duration
    unsafe { Latch::set(latch) };
}
