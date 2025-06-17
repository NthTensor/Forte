//! An async-compatible thread-pool aiming for "speed through simplicity".
//!
//! Forte is a parallel & async work scheduler designed to accommodate very large
//! workloads with many short-lived tasks. It replicates the `rayon_core` api
//! but with native support for futures and async tasks. Its design was
//! prompted by the needs of the bevy game engine, but should be applicable to
//! any problem that involves running both synchronous and asynchronous work
//! concurrently.
//!
//! The thread-pool provided by this crate does not employ work-stealing. Forte
//! instead uses "Heartbeat Scheduling", an alternative load-balancing technique
//! that (theoretically) provides provably small overheads and good utilization.
//! The end effect is that work is only parallelized every so often, allowing
//! more work to be done sequentially on each thread and amortizing the
//! synchronization overhead.
//!
//! # Acknowledgments
//!
//! Large portions of the code are direct ports from various versions of
//! `rayon_core`, with minor simplifications and improvements. We also relied
//! upon `chili` and `spice` for reference while writing the heartbeat
//! scheduling. Support for futures is based on an approach sketched out by
//! members of the `rayon` community to whom we are deeply indebted.

// -----------------------------------------------------------------------------
// Boilerplate for building without the standard library

#![no_std]

extern crate alloc;
extern crate std;

// -----------------------------------------------------------------------------
// Modules

mod blocker;
mod job;
mod scope;
mod signal;
mod thread_pool;

// -----------------------------------------------------------------------------
// Top-level exports

pub use scope::Scope;
pub use thread_pool::ThreadPool;
pub use thread_pool::Worker;
pub use thread_pool::block_on;
pub use thread_pool::join;
pub use thread_pool::scope;
pub use thread_pool::spawn;
pub use thread_pool::spawn_async;
pub use thread_pool::spawn_future;

// -----------------------------------------------------------------------------
// Platform Support

// This crate uses `loom` for testing, which requires mocking all of the core
// threading primitives (`Mutex` and the like). Unfortunately there are some
// minor differences between the `loom` and `std`.
//
// To make things a bit simpler, we re-export all the important types in the
// `primitives` module. Where necessary we wrap the `std` implementation to make
// it match up with `loom`.

#[cfg(not(loom))]
mod platform {

    // Core exports

    pub use alloc::sync::Arc;
    pub use alloc::sync::Weak;
    pub use core::cell::Cell;
    pub use core::sync::atomic::AtomicBool;
    pub use core::sync::atomic::AtomicU32;
    pub use core::sync::atomic::Ordering;
    pub use std::sync::Barrier;
    pub use std::sync::Condvar;
    pub use std::sync::Mutex;
    pub use std::thread::Builder as ThreadBuilder;
    pub use std::thread::JoinHandle;
    pub use std::thread::available_parallelism;
    pub use std::thread_local;

    // Unsafe Cell

    pub struct UnsafeCell<T> {
        data: core::cell::UnsafeCell<T>,
    }

    impl<T> UnsafeCell<T> {
        #[inline(always)]
        pub const fn new(data: T) -> Self {
            UnsafeCell {
                data: core::cell::UnsafeCell::new(data),
            }
        }

        #[inline(always)]
        pub fn get_mut(&self) -> MutPtr<T> {
            MutPtr {
                ptr: self.data.get(),
            }
        }
    }

    pub struct MutPtr<T: ?Sized> {
        ptr: *mut T,
    }

    #[allow(clippy::mut_from_ref)]
    impl<T: ?Sized> MutPtr<T> {
        /// Dereferences the pointer.
        ///
        /// # Safety
        ///
        /// This is equivalent to dereferencing a *mut T pointer, so all the
        /// same safety considerations apply here.
        ///
        /// Because the `MutPtr` type can only be created by calling
        /// `UnsafeCell::get_mut` on a valid `UnsafeCell`, we know the pointer
        /// will never be null.
        #[inline(always)]
        pub unsafe fn deref(&self) -> &mut T {
            // SAFETY: The safety requirements of this pointer dereference are
            // identical to those of the function.
            unsafe { &mut *self.ptr }
        }
    }
}

#[cfg(loom)]
mod platform {

    // Core exports

    use core::ops::Deref;

    pub use loom::cell::Cell;
    pub use loom::cell::UnsafeCell;
    pub use loom::sync::Arc;
    pub use loom::sync::Condvar;
    pub use loom::sync::Mutex;
    pub use loom::sync::atomic::AtomicBool;
    pub use loom::sync::atomic::AtomicU32;
    pub use loom::sync::atomic::Ordering;
    pub use loom::sync::atomic::fence;
    pub use loom::thread::Builder as ThreadBuilder;
    pub use loom::thread::JoinHandle;
    pub use loom::thread_local;

    // Queue

    pub type UnboundedQueue<T> = crossbeam_queue::SegQueue<T>;
    pub type Queue<T, R> = thingbuf::ThingBuf<T, R>;

    // Available parallelism

    pub fn available_parallelism() -> std::io::Result<std::num::NonZero<usize>> {
        panic!("available_parallelism does not work on loom");
    }
}
