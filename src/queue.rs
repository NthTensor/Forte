//! A fast concurrent queue

// This uses the same alignment as `CachePadded
#[cfg(target_arch = "s390x", repr(align(256)))]
const CACHE_LINE_SIZE: usize = 256;
#[cfg(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
))]
const CACHE_LINE_SIZE: usize = 128;
#[cfg(not(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "hexagon",
    target_arch = "m68k",
    target_arch = "s390x",
)))]
const CACHE_LINE_SIZE: usize = 64;
#[cfg(any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "hexagon",
))]
const CACHE_LINE_SIZE: usize = 32;
#[cfg(target_arch = "m68k")]
const CACHE_LINE_SIZE: usize = 16;

pub struct Queue<T> {
    // The head of the queue
    head: CachePadded<AtomicU32>,
    // The tail of the queue
    tail: CachePadded<AtomicU32>,
    // The buffer of pointers to T
    buffer: Box<CachePadded<[UnsafeCell<MaybeUninit<T>>]>>,
    // States for the buffer items, same length as buffer
    states: Box<CachePadded<[AtomicI8]>>,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {}
