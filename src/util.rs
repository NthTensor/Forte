use core::{
    cell::Cell,
    hash::Hasher,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::hash::DefaultHasher;

/// [xorshift*] is a fast pseudorandom number generator which will
/// even tolerate weak seeding, as long as it's not zero.
///
/// [xorshift*]: https://en.wikipedia.org/wiki/Xorshift#xorshift*
#[cfg(not(feature = "shuttle"))]
pub struct XorShift64Star {
    state: Cell<u64>,
}

#[cfg(not(feature = "shuttle"))]
impl XorShift64Star {
    pub fn new() -> Self {
        // Any non-zero seed will do -- this uses the hash of a global counter.
        let mut seed = 0;
        while seed == 0 {
            let mut hasher = DefaultHasher::new();
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            hasher.write_usize(COUNTER.fetch_add(1, Ordering::Relaxed));
            seed = hasher.finish();
        }

        XorShift64Star {
            state: Cell::new(seed),
        }
    }

    #[allow(dead_code)]
    pub fn from_seed(seed: u64) -> Self {
        XorShift64Star {
            state: Cell::new(seed),
        }
    }

    fn next(&self) -> u64 {
        let mut x = self.state.get();
        debug_assert_ne!(x, 0);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Return a value from `0..n`.
    pub fn next_usize(&self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

#[cfg(feature = "shuttle")]
pub struct XorShift64Star;

#[cfg(feature = "shuttle")]
impl XorShift64Star {
    pub fn new() -> Self {
        Self
    }

    pub fn next_usize(&self, n: usize) -> usize {
        use shuttle::rand::Rng;
        use shuttle::rand::thread_rng;

        thread_rng().gen_range(0..n)
    }
}
