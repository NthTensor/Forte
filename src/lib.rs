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

#![no_std]
#![cfg_attr(feature = "shuttle", allow(dead_code))]
#![cfg_attr(feature = "shuttle", allow(unused_imports))]

// -----------------------------------------------------------------------------
// Boilerplate for building without the standard library

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

// This crate uses `shuttle` for testing, which requires mocking all of the core
// threading primitives (`Mutex` and the like).
//
// To make things a bit simpler, we re-export all the important types in the
// `primitives` module.

#[cfg(not(feature = "shuttle"))]
mod platform {

    // Core exports

    pub use alloc::sync::Arc;
    pub use alloc::sync::Weak;
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
}

#[cfg(feature = "shuttle")]
mod platform {

    // Core exports

    pub use shuttle::sync::Arc;
    pub use shuttle::sync::Barrier;
    pub use shuttle::sync::Condvar;
    pub use shuttle::sync::Mutex;
    pub use shuttle::sync::Weak;
    pub use shuttle::sync::atomic::AtomicBool;
    pub use shuttle::sync::atomic::AtomicU32;
    pub use shuttle::sync::atomic::Ordering;
    pub use shuttle::thread::Builder as ThreadBuilder;
    pub use shuttle::thread::JoinHandle;
    pub use shuttle::thread_local;

    // Available parallelism

    pub fn available_parallelism() -> std::io::Result<core::num::NonZero<usize>> {
        panic!("available_parallelism does not work on shuttle");
    }
}
