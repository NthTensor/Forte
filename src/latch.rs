//! Forte borrows the *latch* concept from Rayon. Every forte worker thread has
//! a single binary semaphore, used for parking and unparking the thread.
//!
//! Latches build on top of semaphores; allowing workers to wait for specific
//! events, while also allowing wakeups from other sources on the semaphore.

use alloc::task::Wake;
use core::borrow::Borrow;

use crate::platform::*;

// -----------------------------------------------------------------------------
// States

/// The default state of a latch is `LOCKED`. When in the locked state, `check`
/// returns `Pending` and `wait` blocks.
const LOCKED: u32 = 0b000;

/// The latch enters the `SIGNAL` state when it is set (with error flag false).
/// When in this state, `check` returns `Status::Ok` and `wait` does not block.
const SIGNAL: u32 = 0b001;

/// The latch enters the `ERROR` state when it is set (with error flag true).
/// When in this state, `check` returns `Status::Error` and `wait` does not block.
const ERROR: u32 = 0b010;

/// The latch enters the `ASLEEP` state when blocking with `wait`.
const ASLEEP: u32 = 0b100;

// -----------------------------------------------------------------------------
// Latch

/// A Latch is a signaling mechanism used to indicate when an event has
/// occurred.
///
/// The latch begins as *unset* (In the `LOCKED` state), and can later be *set*
/// by any thread (entering the `SIGNAL`) state. Each latch is "owned" by a
/// single thread at a time; other threads may set the latch, but only the
/// owning thread may call `wait` or `check`.
///
/// The general idea and spirit for latches (as well as some of the
/// documentation) is due to rayon. However the implementation is specific to
/// forte.
///
/// ## Memory Ordering
///
/// `set` stores the latch state with `Release` and `check` loads it with
/// `Acquire`. A `check` that observes a set latch therefore *happens-after*
/// the `set`.
///
/// Therefore, after the owning thread observes a signal through `check`:
///
/// 1. It may reclaim the latch (drop or `reset` it).
///
/// 2. It may access any writes ordered before the call to `set`.
pub struct Latch {
    /// Holds the internal state of the latch. This tracks if the latch has been
    /// set or not.
    state: AtomicU32,
    /// The semaphore that this latch will use for signaling.
    semaphore: &'static Semaphore,
}

/// The result of a [`Latch::check`]: whether the latch has been set, and if so
/// whether it was set with an error.
pub enum Status {
    /// The latch has not been set.
    Pending,
    /// The latch was set without an error.
    Ok,
    /// The latch was set with an error.
    Error,
}

impl Latch {
    /// Creates a new latch backed by the provided semaphore.
    pub fn new(semaphore: &'static Semaphore) -> Latch {
        Latch {
            state: AtomicU32::new(LOCKED),
            semaphore,
        }
    }

    /// Checks to see if the latch has been set.
    ///
    /// # Memory Ordering
    ///
    /// A call to `check` that observes a signal establishes a happens-after
    /// relationship with the `set` that produced the signal, which can be used
    /// to safely reclaim the latch or transmit values between threads.
    #[inline(always)]
    pub fn check(&self) -> Status {
        match self.state.load(Ordering::Acquire) {
            SIGNAL => Status::Ok,
            ERROR => Status::Error,
            _ => Status::Pending,
        }
    }

    /// Checks if the latch has been set, and if not waits for a signal on the
    /// semaphore. This does _not_ wait for the latch to actually become set,
    /// and may return early. The caller should always re-check the latch
    /// condition after this returns.
    ///
    /// # Memory Ordering
    ///
    /// A call to `wait` does not synchronize memory, and so must be used in
    /// conjunction with `check` (see the type-level "Memory Ordering" section).
    #[cold]
    pub fn wait(&self, seat_bitmask: u32, waiting_bitmask: &'static AtomicU32) {
        // First, check if the latch has been set.
        //
        // In the event of a race with `set`:
        //
        // * If this happens before the store, then we will go to sleep.
        //
        // * If this happens after the store, then we notice and return.
        if self.state.load(Ordering::Relaxed) & (SIGNAL | ERROR) != 0 {
            return;
        }
        // If it has not been set, wait for a signal on the semaphore.
        //
        // In the event of a race with `set`, the call to `Semaphore::signal`
        // will always end up unblocking this, no matter the memory-ordering.
        self.semaphore.wait(seat_bitmask, waiting_bitmask);
    }

    /// Sets the latch, and sends a signal over the semaphore.
    ///
    /// This takes a raw pointer because the latch may be de-allocated by a
    /// different thread while this function is executing.
    ///
    /// When `error` is set to `true`, `wait` returns `Status::Error` rather
    /// than `Status::Ok`.
    ///
    /// # Memory Ordering
    ///
    /// A call to `check` that observes the signal produced by this `set`
    /// establishes a happens-after relationship. Stores on this thread made
    /// before this call will be visible to the owning thread after `check`
    /// returns a status other than `Pending`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// * `latch` is a non-null, aligned pointer to an initialized `Latch`.
    ///
    /// * Additionally, one of the following condition variants must be met:
    ///
    ///   1. The latch will not be dropped or moved for the duration of `set`.
    ///
    ///   2. The latch has not been `set` since it was created or last `reset`,
    ///      calls to `set` do not race, and the latch will not be dropped or
    ///      moved until after `check` returns a status other than `Pending`.
    #[inline(always)]
    pub unsafe fn set(latch: *const Latch, error: bool) {
        // First we store a reference to the semaphore (which is 'static) so
        // that we can access it even if the latch pointer becomes dangling.
        //
        // SAFETY: The caller guarantees the latch pointer is aligned and
        // non-null.
        //
        // If Variant 1 is met, the latch cannot be dangling.
        //
        // If Variant 2 is met, the latch cannot become dangling so long as the
        // state is `LOCKED` (because `check` will return `Pending`). Since
        // there can have been no previous call to `set` since construction or
        // the last `reset`, and there can be no racing calls to `set`, the
        // state must be `LOCKED`. Therefore the latch cannot be dangling.
        //
        // Since this pointer is aligned, non-null, is not dangling, and the
        // latch is never accessed mutably, it is valid to access immutably.
        let semaphore = unsafe { (*latch).semaphore };
        // Determine the next state for the latch.
        let state = if error { ERROR } else { SIGNAL };
        // Next we update the state.
        //
        // In the event of a race with `wait`, this may cause `wait` to return.
        // Otherwise the other thread will sleep within `wait`.
        //
        // SAFETY: The latch must still be valid to dereference, up to the point
        // where we begin this store, by the same reasoning as above.
        //
        // The `Release` store synchronizes with the `Acquire` load in `check`
        // to supply the second half of the Variant 2 argument: the latch does
        // not merely live *up to* this store, the store is also ordered
        // *before* any later free. The owner may free the latch as soon as
        // `check` reports non-`Pending`, and that check is ordered after this
        // store, so there can be no data race.
        unsafe { (*latch).state.store(state, Ordering::Release) };
        // Finally we try to signal the target thread on it's semaphore, just in
        // case it missed the notification and is currently waiting. This
        // guarantees that the other thread will make progress.
        semaphore.signal();
    }

    /// Restores the latch to the default state.
    ///
    /// # Deadlocks
    ///
    /// This must only be called by the thread that "owns" the latch, and only
    /// after it has *observed* `check` return something other than `Pending`.
    ///
    /// Calling `reset` from a different thread or before observing the signal
    /// is likely to result in deadlocks.
    #[inline(always)]
    pub fn reset(&self) {
        self.state.store(LOCKED, Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------------
// Signals

/// A low-overhead binary semaphore used for task signaling.
pub struct Semaphore {
    state: AtomicU32,
}

impl Semaphore {
    /// Creates a new signal.
    pub const fn new() -> Self {
        Semaphore {
            state: AtomicU32::new(LOCKED),
        }
    }

    /// Sends a signal to the semaphore.
    ///
    /// Returns true if this allows a waiting thread to make progress (by
    /// signaling it while it was waiting or catching it before it started
    /// waiting) and false if the thread was running.
    #[inline(always)]
    pub fn signal(&self) -> bool {
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

    /// Waits for a signal on the semaphore.
    ///
    /// Calls to `wait` should be fully ordered. In other words, this method
    /// must not be called on the same value by two different threads unless a
    /// "happens-before relationship" has been established between the calls via
    /// memory synchronization.
    #[cold]
    pub fn wait(&self, seat_mask: u32, waiting_bitmask: &'static AtomicU32) {
        // Set the state to ASLEEP and read the current state.
        let state = self.state.swap(ASLEEP, Ordering::Relaxed);
        // The previous state should not have been ASLEEP, because calls to
        // `sleep` must be fully ordered, and the state is only set to ASLEEP
        // while `sleep` is executing.
        //
        // If the state is LOCKED, then we have not yet received a signal, and
        // we should try to put the thread to sleep. Otherwise we should return
        // early.
        if state == LOCKED {
            // Set the sleeping bit for this worker.
            waiting_bitmask.fetch_or(seat_mask, Ordering::Relaxed);
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
            waiting_bitmask.fetch_and(!seat_mask, Ordering::Relaxed);
        }
        // Set the state back to LOCKED so that we are ready to receive new
        // signals.
        self.state.store(LOCKED, Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------------
// Async wakers

impl Wake for Latch {
    fn wake(self: Arc<Self>) {
        // SAFETY: The borrowed `Arc` is held for the duration of this call,
        // keeping the `Latch` alive, and satisfying Variant 1 of `Latch::set`.
        unsafe { Latch::set(self.borrow(), false) };
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // SAFETY: The borrowed `Arc` is held for the duration of this call,
        // keeping the `Latch` alive, and satisfying Variant 1 of `Latch::set`.
        unsafe { Latch::set(self.borrow(), false) };
    }
}
