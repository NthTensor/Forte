//! Forte borrows the *latch* concept from Rayon.
//!
//! Every forte worker thread has a single "sleep controller" that it uses to
//! park and unpark itself. Latches build on this to create a simple boolean
//! switch, which allows the owning thread to sleep until the latch becomes set
//! by another thread.
//!
//! Every latch points at one "sleep controller".

use alloc::task::Wake;
use core::borrow::Borrow;

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

/// A Latch is a signaling mechanism used to indicate when an event has
/// occurred. The latch begins as *unset* (In the `LOCKED` state), and can later
/// be *set* by any thread (entering the `SIGNAL`) state.
///
/// Each latch is associated with one *owner thread*. This is the thread that
/// may be blocking, waiting for the latch to complete.
///
/// The general idea and spirit for latches (as well as some of the
/// documentation) is due to rayon. However the implementation is specific to
/// forte.
///
/// ## Memory Ordering
///
/// Latches _do not synchronize memory_. They are only used for signaling. If
/// the thread that sets a latch wishes to transmit a value to the thread
/// waiting for that latch, explicit fences must be used.
pub struct Latch {
    /// Holds the internal state of the latch. This tracks if the latch has been
    /// set or not.
    state: AtomicU32,
    /// Tracks the number of sleeping threads in the pool.
    sleeping: &'static AtomicU32,
    /// The sleep controller for the owning thread.
    sleep_controller: &'static SleepController,
    /// The seat number that owns this latch
    seat_number: usize,
}

impl Latch {
    /// Creates a new latch, owned by a specific thread.
    pub fn new(
        seat_number: usize,
        sleeping: &'static AtomicU32,
        sleep_controller: &'static SleepController,
    ) -> Latch {
        Latch {
            state: AtomicU32::new(LOCKED),
            sleeping,
            sleep_controller,
            seat_number,
        }
    }

    /// Checks to see if the latch has been set. Returns true if it has been.
    #[inline(always)]
    pub fn check(&self) -> bool {
        self.state.load(Ordering::Relaxed) == SIGNAL
    }

    /// Puts the thread to sleep if the latch has not been set. The thread will
    /// be woken when the latch becomes set, but may also wake before then. The
    /// caller should always re-check the latch condition after this returns.
    ///
    /// # Memory Ordering
    ///
    /// This does not synchronize memory. To synchronize memory with the thread
    /// setting the latch, call `fence(Ordering::Acquire)` after this function.
    /// The other thread must issue a corresponding `fence(Ordering::Release)`
    /// call.
    #[cold]
    pub fn wait(&self) {
        // First, check if the latch has been set.
        //
        // In the event of a race with `set`:
        //
        // * If this happens before the store, then we will go to sleep.
        //
        // * If this happens after the store, then we notice and return.
        if self.state.load(Ordering::Relaxed) == SIGNAL {
            return;
        }
        // If it has not been set, go to sleep.
        //
        // In the event of a race with `set`, the `wake` will always cause this
        // to return regardless of memory ordering.
        self.sleep_controller.sleep(self.seat_number, self.sleeping);
    }

    /// Activates the latch, potentially unblocking the owning thread.
    ///
    /// This takes a raw pointer because the latch may be de-allocated by a
    /// different thread while this function is executing.
    ///
    /// # Memory Ordering
    ///
    /// This does not synchronize memory. To synchronize memory with the waiting
    /// thread, call `fence(Ordering::Release)` before this function. The other
    /// thread must issue a corresponding `fence(Ordering::Acquire)` call.
    ///
    /// # Safety
    ///
    /// The latch pointer must be valid when passed to this function. After this
    /// call, the latch pointer may become dangling and must not be dereferenced
    /// unless it is known to still be valid.
    #[inline(always)]
    pub unsafe fn set(latch: *const Latch) {
        // SAFETY: The caller guarantees the latch remain alive until `set`
        // returns.
        let latch = unsafe { &*latch };
        let sleep_controller = latch.sleep_controller;
        // First we set the state to true.
        //
        // In the event of a race with `wait`, this may cause `wait` to return.
        // Otherwise the other thread will sleep within `wait.
        latch.state.store(SIGNAL, Ordering::Relaxed);
        // We must try to wake the other thread, just in case it missed the
        // notification and went to sleep. This guarantees that the other thread
        // will make progress.
        sleep_controller.wake();
    }

    /// Restores the latch to the default state.
    ///
    /// # Deadlocks
    ///
    /// This may only be called by the thread that "owns" the latch, and only
    /// after it has *observed* the latch entering the `SIGNAL` state, e.g.
    /// after either `wait` or `check` has returned `true`.
    ///
    /// Calling `reset` from a different thread or before observing the signal
    /// is likely to result in deadlocks.
    #[inline(always)]
    pub fn reset(&self) {
        self.state.store(LOCKED, Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------------
// Sleeper

/// Used, in combination with a latch to park and unpark threads.
#[cfg(not(feature = "shuttle"))]
pub struct SleepController {
    state: AtomicU32,
}

#[cfg(not(feature = "shuttle"))]
impl SleepController {
    /// Creates a new sleep controller.
    pub const fn new() -> Self {
        SleepController {
            state: AtomicU32::new(LOCKED),
        }
    }

    /// Attempt to wake the thread to which this belongs.
    ///
    /// Returns true if this allows the thread to make progress (by waking it up
    /// or catching it before it goes to sleep) and false if the thread was
    /// running.
    #[inline(always)]
    pub fn wake(&self) -> bool {
        // Set the state to SIGNAL and read the current state, which must be
        // either LOCKED, ASLEEP or SIGNAL.
        let sleep_state = self.state.swap(SIGNAL, Ordering::Relaxed);
        if sleep_state == ASLEEP {
            // If the state was ASLEEP, the thread is either asleep or about to
            // go to sleep.
            //
            // * If it is about to go to sleep (but has not yet called
            //   `atomic_wait::wait`) then setting the state to SIGNAL above
            //   should prevent it from going to sleep.
            //
            // * If it is already waiting, the following notification will wake
            //   it up.
            //
            // Either way, after this call the other thread must make progress.
            atomic_wait::wake_one(&self.state);
        }
        // Return true if the other thread was asleep and not already notified.
        sleep_state == ASLEEP
    }

    /// Attempt to send the thread to sleep. This should only be called on a
    /// single thread, and we say that this controller "belongs" to that thread.
    ///
    /// Returns true if this thread makes a syscall to suspend the thread, and
    /// false if the thread was already woken (letting us skip the syscall).
    #[cold]
    pub fn sleep(&self, seat_number: usize, sleeping: &'static AtomicU32) {
        // Set the state to ASLEEP and read the current state, which must be
        // either LOCKED or SIGNAL.
        let state = self.state.swap(ASLEEP, Ordering::Relaxed);
        // If the state is LOCKED, then we have not yet received a signal, and
        // we should try to put the thread to sleep. Otherwise we should return
        // early.
        if state == LOCKED {
            // Set the sleeping bit for this worker.
            sleeping.fetch_or(1 << seat_number, Ordering::Relaxed);
            // If we have received a signal since entering the sleep state
            // (meaning the state is no longer set to ASLEEP) then this will
            // return immediately.
            //
            // If the state is still ASLEEP, then the next call to `wake` will
            // register that and call `wake_on`.
            //
            // Either way, there is no way we can fail to receive a `wake`.
            atomic_wait::wait(&self.state, ASLEEP);
            // Clear the sleeping bit for this worker.
            sleeping.fetch_and(!(1 << seat_number), Ordering::Relaxed);
        }
        // Set the state back to LOCKED so that we are ready to receive new
        // signals.
        self.state.store(LOCKED, Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------------
// Shuttle sleeper fallback

/// This is a fallback implementation because the futex api is not available on
/// shuttle.
#[cfg(feature = "shuttle")]
pub struct SleepController {
    state: Mutex<u32>,
    condvar: Condvar,
}

#[cfg(feature = "shuttle")]
impl SleepController {
    pub fn new() -> Self {
        SleepController {
            state: Mutex::new(LOCKED),
            condvar: Condvar::new(),
        }
    }

    pub fn wake(&self) -> bool {
        let state = core::mem::replace(&mut *self.state.lock().unwrap(), SIGNAL);
        let asleep = state == ASLEEP;
        if asleep {
            self.condvar.notify_one();
        }
        asleep
    }

    pub fn sleep(&self, seat_number: usize, sleeping: &'static AtomicU32) {
        let mut state = self.state.lock().unwrap();
        if *state == LOCKED {
            *state = ASLEEP;
            sleeping.fetch_or(1 << seat_number, Ordering::Relaxed);
            while *state == ASLEEP {
                state = self.condvar.wait(state).unwrap();
            }
            sleeping.fetch_and(!(1 << seat_number), Ordering::Relaxed);
        }
        *state = LOCKED;
    }
}

// -----------------------------------------------------------------------------
// Async wakers

impl Wake for Latch {
    fn wake(self: Arc<Self>) {
        // SAFETY: The borrowed `Arc` is held for the duration of this call,
        // keeping the `Latch` alive.
        unsafe { Latch::set(self.borrow()) };
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // SAFETY: The borrowed `Arc` is held for the duration of this call,
        // keeping the `Latch` alive.
        unsafe { Latch::set(self.borrow()) };
    }
}
