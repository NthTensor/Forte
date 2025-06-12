//! This modules defines a basic signal that can be used to notify a waiting
//! thread about the completion of a job. Signals can be used to transport the
//! outcome or return value of a job.
//!
//! The implementation here is loosely adapted from chili and the oneshot crate,
//! modified to use a futex instead of a CAS loop.

use crate::platform::*;

// -----------------------------------------------------------------------------
// States

/// The default state of a signal, with no waiting recever and no sent value.
pub const IDLE: u32 = 0b00;

/// A bit set by the recever when it is waiting, and needs the sender to wake it up.
pub const WAIT: u32 = 0b01;

/// A bit set by the sender when data has been transmitted to the recever.
pub const SENT: u32 = 0b10;

// -----------------------------------------------------------------------------
// Signal

/// A signal transmits a single value across threads, exactly once. Signals are
/// Forte's core synchronization primitive. They take the place of rayon's
/// latches or chili's oneshot channels.
///
/// The api contract for signals is somewhat subtle, but it is governed by one
/// general principle: A signal must be dropped after used (data has been sent
/// over it).
pub struct Signal<T = ()> {
    /// The state of the signal, used for synchronization and sleeping.
    state: AtomicU32,
    /// The value transmitted by the signal.
    value: UnsafeCell<Option<T>>,
}

impl<T: Send> Signal<T> {
    /// Creates a new signal.
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(IDLE),
            value: UnsafeCell::new(None),
        }
    }

    /// Receives the signal if it has been sent, without blocking.
    ///
    /// # Panics
    ///
    /// This panics if called on a signal on which data has already been
    /// received.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `recv` and `try_receive` are only called
    /// from a single thread.
    pub unsafe fn try_recv(&self) -> Option<T> {
        // If the SENT bit has been set, read it and return it.
        if self.state.load(Ordering::Acquire) & SENT != 0 {
            // Use the unsafe cell to get mutable access to the value.
            let value_ptr = self.value.get_mut();

            // Read the value from the signal.
            //
            // Panic if `recv` or `try_recv` has already returned data.
            //
            // SAFETY: The other thread only ever accesses this memory
            // location once, before entering the SENT state. Because we are
            // now in the SENT state, and there can be no other calls to
            // `recv` or `try_recv` happening on other threads, we can
            // guarantee that we have exclusive access to this memory
            // location. The SENT flag also tells us that that the memory
            // location is initialized.
            let value = unsafe { value_ptr.deref().take().unwrap() };

            Some(value)
        } else {
            None
        }
    }

    /// Receives the signal, or waits for it to be sent.
    ///
    /// # Panics
    ///
    /// This panics if called on a signal on which data has already been
    /// received.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `recv` and `try_receive` are only called
    /// from a single thread.
    #[cold]
    pub unsafe fn recv(&self) -> T {
        // Loop to mitigate spurious wake-ups.
        loop {
            // Set the WAIT bit and load the current state.
            let state = self.state.fetch_or(WAIT, Ordering::Acquire);

            // If the SENT bit has been set, read it and return it.
            if state & SENT != 0 {
                // Use the unsafe cell to get mutable access to the value.
                let value_ptr = self.value.get_mut();

                // Read the value from the signal.
                //
                // Panic if `recv` or `try_recv` has already returned data.
                //
                // SAFETY: The other thread only ever accesses this memory
                // location once, before entering the SENT state. Because we are
                // now in the SENT state, and there can be no other calls to
                // `recv` or `try_recv` happening on other threads, we can
                // guarantee that we have exclusive access to this memory
                // location. The SENT flag also tells us that that the memory
                // location is initialized.
                return unsafe { value_ptr.deref().take().unwrap() };
            }

            // If a value has not been sent, wait until it is.
            atomic_wait::wait(&self.state, state);
        }
    }

    /// Sends the signal to the receiving thread.
    ///
    /// # Panics
    ///
    /// This panics if called more than once on the same signal.
    ///
    /// # Safety
    ///
    /// Sending a signal may wake other threads, which may cause signals
    /// allocated on that thread's stack to be deallocated.
    ///
    /// This function operates on `*const Self` instead of `&self` to allow it
    /// to become dangling during this call. The caller must ensure that the
    /// pointer is convertible to an immutable reference upon entry, and not
    /// invalidated during the call by any actions other than `send` itself.
    #[inline(always)]
    pub unsafe fn send(signal: *const Self, value: T) {
        // Use the unsafe cell to get mutable access to the value.
        //
        // SAFETY: The caller ensures that this pointer is convertible to a
        // reference, and we have not yet done anything that would cause another
        // thread to invalidate it.
        let value_ptr = unsafe { (*signal).value.get_mut() };

        // Load the current state of the signal.
        //
        // SAFETY: The caller ensures that this pointer is convertible to a
        // reference, and we have not yet done anything that would cause another
        // thread to invalidate it.
        let state = unsafe { (*signal).state.load(Ordering::Relaxed) };

        // Panic if the signal has already been sent.
        if state & SENT != 0 {
            panic!("attempted to send value over signal, but signal has already been sent");
        }

        // Write the value into the signal.
        //
        // SAFETY: The other thread only ever accesses this memory location when
        // the signal is in the SENT state. Because we are responsible for
        // setting that state, and the assert above ensures that we are not
        // already in that state, we can be sure that we have unique access to
        // the memory location.
        unsafe { *value_ptr.deref() = Some(value) };

        // Set the bit for the SENT state. Note: This can cause the `signal`
        // pointer to become dangling.
        //
        // SAFETY: The caller ensures that this pointer is convertible to a
        // reference, and we have not yet done anything that would cause another
        // thread to invalidate it.
        let state = unsafe { (*signal).state.fetch_or(SENT, Ordering::Release) };
        if state & WAIT != 0 {
            // If the WAIT bit is set, then we receiving thread is asleep and we must wake it.
            //
            // SAFETY: The caller ensures that this pointer is convertible to a
            // reference. It's not possible for it to have been invalidated
            // because when in the WAIT state, the receiver thread must either
            // be asleep or about to sleep, so setting the SENT bit cannot have
            // caused the signal to be deallocated.
            atomic_wait::wake_one(unsafe { &(*signal).state });
        }
    }
}

impl<T: Send> Default for Signal<T> {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: References to signals have to be sent between threads for them to
// work, so they must be `Sync`. And signals themselves transmit values between
// threads, so the type `T` must be `Send`.
unsafe impl<T: Send> Sync for Signal<T> {}
