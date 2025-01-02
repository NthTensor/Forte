use std::{
    cell::{Cell, UnsafeCell},
    mem::{needs_drop, MaybeUninit},
    sync::atomic::{AtomicUsize, Ordering},
};

// -----------------------------------------------------------------------------
// Call on drop guard

// A guard that calls the specified closure when it is dropped. This is used
// internally to run logic when a `Future` is canceled or completed.
pub struct CallOnDrop<F: FnMut()>(pub F);

impl<F: FnMut()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

// -----------------------------------------------------------------------------
// Slot

/// A slot is a simple atomic store. Like `Option`, slots are either empty or
/// contain a single value. But unlike `Option`, slots are opaque: the only way
/// to tell if a slot contains a value is to remove it.
///
/// Slot supports only two operations:
/// + `put` inserts a value into an empty slot (and fails when occupied).
/// + `take` removes a value from an occupied slot (and files when empty).
///
/// Both these operations are lock-free. The failing path costs only an atomic
/// read. The success path costs an atomic read and three quick writes (which
/// should only ever cause one cache miss on other threads).
///
/// Neither `put` nor `take` will spin.
pub struct Slot<T> {
    slot: UnsafeCell<MaybeUninit<T>>,
    flag: AtomicUsize,
}

// A flag state indicating the slot is empty. This allows `put` but not `take`.
const NONE: usize = 0;

// A flag state indicating either a `put` or a `take` is in progress.
const LOCK: usize = 1;

// A flag state indicating the slot is occupied. This allows `take` but not `put`.
const SOME: usize = 2;

impl<T> Slot<T> {
    /// Creates an empty slot.
    pub const fn empty() -> Slot<T> {
        Slot {
            slot: UnsafeCell::new(MaybeUninit::uninit()),
            flag: AtomicUsize::new(NONE),
        }
    }

    /// Tries to put a new value in the slot. If the slot is already occupied
    /// the new value is returned. Returning `None` indicates a successful insertion.
    pub fn put(&self, value: T) -> Option<T> {
        match self
            .flag
            .compare_exchange(NONE, LOCK, Ordering::Acquire, Ordering::Relaxed)
        {
            Err(_) => Some(value),
            Ok(_) => {
                unsafe {
                    let slot = &mut *(self.slot.get());
                    slot.write(value);
                };
                self.flag.store(SOME, Ordering::Release);
                None
            }
        }
    }

    /// Takes the value from the slot. Returns none if the slot is empty.
    pub fn take(&self) -> Option<T> {
        match self
            .flag
            .compare_exchange(SOME, LOCK, Ordering::Acquire, Ordering::Relaxed)
        {
            Err(_) => None,
            Ok(_) => {
                let value;
                unsafe {
                    let slot = &mut *(self.slot.get());
                    value = slot.assume_init_read();
                };
                self.flag.store(NONE, Ordering::Release);
                Some(value)
            }
        }
    }
}

impl<T> Drop for Slot<T> {
    fn drop(&mut self) {
        // If `T` dosn't need to be dropped then neither does `Slot`.
        if needs_drop::<T>() {
            let Slot { flag, slot } = self;
            // SAFETY: The flag value is always set to either `NONE` or `SOME`.
            // Slots are never dropped when the flag is `LOCK`. If the flag is
            // `NONE` then the slot is empty and nothing needs to be dropped.
            // If it is `SOME` then the value is initalized and we must
            // manually drop it.
            unsafe {
                if *flag.get_mut() == SOME {
                    slot.get_mut().as_mut_ptr().drop_in_place();
                }
            }
        }
    }
}

/// SAFETY: A `Slot<T>` contains `T` so is `Send` iff `T` is send.
unsafe impl<T> Send for Slot<T> where T: Send {}

/// SAFETY: A `&Slot<T>` lets you get a `T` via `Slot::take()`. If `Slot<T>` is
/// `Sync` this could cause `T` to be sent to another thread. So `Slot<T>` is
/// `Sync` iff `T` is `Send`.
unsafe impl<T> Sync for Slot<T> where T: Send {}

// -----------------------------------------------------------------------------
// Xorshift fast prng (taken from rayon)

/// [xorshift*] is a fast pseudorandom number generator which will
/// even tolerate weak seeding, as long as it's not zero.
///
/// [xorshift*]: https://en.wikipedia.org/wiki/Xorshift#xorshift*
pub struct XorShift64Star {
    state: Cell<u64>,
}

impl XorShift64Star {
    /// Initializes the prng with a seed. Provided seed must be nonzero.
    pub fn new(seed: u64) -> Self {
        XorShift64Star {
            state: Cell::new(seed),
        }
    }

    /// Returns a pseudorandom number.
    pub fn next(&self) -> u64 {
        let mut x = self.state.get();
        debug_assert_ne!(x, 0);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Return a pseudorandom number from `0..n`.
    pub fn next_usize(&self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}
