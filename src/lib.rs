//! Forte is a thread pool that uses lazy scheduling techniques to reduce latency and
//! maximize parallel utilization. It is a close cousin to rayon-core, and takes
//! loose inspiration from zig's `chili` library, the Beam virtual machine, and Java's
//! `ForkJoinPool`.
//!
//! It features:
//! + Statically defined and dynamically sized thread pools.
//! + Fully stack-allocated and inlined fork/join parrellism.
//! + The ability to execute both closures and futures on the same pool.
//! + Hybrid scopes that can contain work distributed across multiple thread pools.
//! + A primitive for awaiting async work in non-async contexts without spinning.
//! + An exposed unsafe api, built for for low-level integration and customization.
//!
//! Here's an example of what it looks like:
//!
//! ```
//! # use forte::ThreadPool;
//! # use forte::Worker;
//! // Allocate a new thread pool.
//! static THREAD_POOL: ThreadPool = ThreadPool::new();
//!
//! fn main() {
//!     // Resize the pool to fill the available number of cores.
//!     THREAD_POOL.resize_to_available();
//!
//!     // Register this thread as a worker on the pool.
//!     THREAD_POOL.with_worker(|worker| {
//!         // Spawn a job onto the pool. The closure also accepts a worker, because the
//!         // job may be executed on a different thread. This will be the worker for whatever
//!         // thread it executes on.
//!         worker.spawn(|worker: &Worker| {
//!             // Spawn another job after this one runs, using the provided local worker.
//!             worker.spawn(|_: &Worker| { });
//!
//!             // Spawn another job, this time using the thread pool. If it is not already,
//!             // methods like this may temporarily turn the current thread into a participant
//!             // in the pool.
//!             THREAD_POOL.spawn(|_: &Worker| { });
//!
//!             // Spawn a third job, which will automatically use the parent thread pool.
//!             // This will panic if not called within a worker context, and is generally
//!             // not recommended.
//!             forte::spawn(|_: &Worker| { });
//!         });
//!
//!         // Spawn a future as a job.
//!         let task = THREAD_POOL.spawn(async { "Hello World" });
//!
//!         // Do two operations in parallel, and await the result of each. This is the most
//!         // efficient and hyper-optimized thread pool operation.
//!         let (a, b) = worker.join(|_| "a", |_| "b");
//!         assert_eq!(a, "a");
//!         assert_eq!(b, "b");
//!
//!         // Wait for that task we started earlier, without using `await`.
//!         let result = worker.block_on(task);
//!         assert_eq!(result, "Hello World");
//!     });
//!
//!     // Wait for all the worker threads to gracefully halt.
//!     THREAD_POOL.depopulate();
//! }
//! ```
//!
//! # Creating Thread Pools
//!
//! Thread pools must be static and const constructed. You don't have to worry
//! about `LazyStatic` or anything else; to create a new thread pool, just call
//! [`ThreadPool::new`] and create a new static to name your pool.
//!
//! ```rust,no_run
//! # use forte::ThreadPool;
//! # use forte::Worker;
//! // Allocate a new thread pool.
//! static THREAD_POOL: ThreadPool = ThreadPool::new();
//! ```
//!
//! This attaches a new thread pool to your program named `THREAD_POOL`, which you
//! can begin to schedule work on immediately. The thread pool will exist for
//! the entire duration of your program, and will shut down when your program
//! completes.
//!
//! # Resizing Thread Pools
//!
//! Thread pools are dynamically sized; When your program starts they have size
//! zero (meaning no worker threads are running). You can change the number of
//! works assigned to a pool using [`ThreadPool::grow`], [`ThreadPool::shrink`]
//! and [`ThreadPool::resize_to`]. But most of the time you will want to call
//! [`ThreadPool::resize_to_available`], which will resize the pool to exploit
//! all the available parallelism on your system by spawning a worker thread for
//! every core.
//!
//! When a thread pool has workers, those workers will automatically help
//! execute all work done on the pool. The number of worker threads attached to
//! a pool roughly determines the amount of parallelism available. If an
//! external thread tries to use a pool of size zero (with no workers), it will
//! still be able to do work, it just won't be done in parallel. And if multiple
//! external threads use an empty pool at the same time, they will sometimes try
//! to collaborate and help each-other out with work.
//!
//! ```
//! # use forte::ThreadPool;
//! # static THREAD_POOL: ThreadPool = ThreadPool::new();
//! // Create as many worker threads as possible.
//! THREAD_POOL.resize_to_available();
//!
//! // Do some potentially parallel work. These may (or may not) run in parallel.
//! THREAD_POOL.join(|_| println!("world"), |_| println!("hello" ));
//!
//! // This may have printed "hello world" or "worldhello ".
//!
//! // Gracefully shut down all the worker threads.
//! THREAD_POOL.depopulate();
//!
//! // Do the same work, but this time we know it will execute serially (because
//! // there are no workers to parallelized it).
//! THREAD_POOL.join(|_| println!("world"), |_| println!("hello "));
//!
//! // This will always print "hello world" (because join happens execute things
//! // backwards in this case).
//! ```
//!
//! # Workers
//!
//! Thread pools are comprised of (and run on) workers, represented as instances
//! of the [`Worker`] type. All work done on the pool is done in a "worker
//! context" created by [`Worker::occupy`]. The recommended way to access a
//! worker context for a specific pool is via [`ThreadPool::with_worker`].
//!
//! ```
//! # use forte::ThreadPool;
//! # static THREAD_POOL: ThreadPool = ThreadPool::new();
//! THREAD_POOL.with_worker(|worker_1| {     // <-- Creates a worker on the pool.
//!     THREAD_POOL.with_worker(|worker_2| { // <-- Returns a reference to the existing worker.
//!         // These pointers are identical.
//!         assert!(std::ptr::eq(worker_1, worker_2));
//!     });                                  // <-- Leaving this scope does nothing.
//! });                                      // <-- Leaving this scope frees the worker.
//! ```
//!
//! Every worker holds a local queue of tasks, as well as metadata that allows
//! other workers on the pool to communicate with it and wake it from sleep.
//! When existing outermost scope (where the worker was actually allocated), all
//! tasks left in the local queue are executed.
//!
//! You will only ever receive `&Worker` references, because the worker is not
//! allowed to move or be mutably referenced. Worker are `!Send` and `!Sync`,
//! and are meant to represent local-only data.
//!
//! To access the current worker context, you can use [`Worker::map_current`] or
//! `[Worker::with_current`]. These allow executing work on arbitrary pools, and
//! can be used to write library code that works normally dispute not knowing
//! about the thread pool static defined by the application.
//!
//! ```rust
//! # use forte::Worker;
//! # fn foo() {}
//! fn my_library_function() {
//!     Worker::with_current(|maybe_worker| match maybe_worker {
//!         // This was called from within a worker context.
//!         Some(worker) => worker.spawn(|_: &Worker| foo()),
//!         // This was not called from within a worker context.
//!         None => foo()
//!     })
//! }
//!
//! ```
//!
//! # Core Operations
//!
//! Thread pools support four core operations:
//! * *Join.* Executes two non-static closures, possibly in parallel, and waits for them to complete.
//! * *Spawn.* Runs a static closure or future in the background.
//! * *Scope.* Runs multiple non-static closures or futures, and waits for them all to complete.
//! * *Block on.* Waits for a future to complete (outside of an async context).
//!
//! All of these with the exception of *Spawn* are blocking; they have a
//! specific join-point where a thread must wait for the all the forks of the
//! parallel operation to complete before proceeding. While it is waiting,
//! threads will attempt to do background work, or help each-other out with
//! their assigned workload.
//!
//! Each operation is available in three different "flavors", depending on the
//! information available at the callsite.
//!
//! | Operation | Headless | Thread pool | Worker |
//! |-----------|----------|-------------|--------|
//! | *Join*     | [`join()`] | [`ThreadPool::join()`] | [`Worker::join()`]
//! | *Spawn*    | [`spawn()`] | [`ThreadPool::spawn()`] | [`Worker::spawn()`]
//! | *Scope*    | [`scope()`] | [`ThreadPool::scope()`] | [`Worker::scope()`]
//! | *Block on* | [`block_on()`] | [`ThreadPool::block_on()`] | [`Worker::block_on()`]
//!
//! * *Worker.* Uses the provided worker context.
//! * *Thread pool.* Looks for an existing worker context, creates one if it dosn't find one.
//! * *Headless.* Looks for an existing worker context, and panics if it dosn't find one.
//!
//! The headless and thread pool flavors are more or less just aliases for the
//! worker flavor. Where possible, the worker flavor should be preferred to the
//! thread pool flavor, and the thread pool flavor should be preferred to the
//! headless flavor.
//!
//! # Theory & Background
//!
//! Forte is based on `rayon_core`, to the extent that during development it was
//! often possible to port code from `rayon_core` more or less verbatim.
//! However, forte and rayon differ significantly in their goals and approach.
//!
//! Rayon uses an approach to work-stealing adapted from Cilk and Intel TBB.
//! These techniques are largely the industry standard.
//!
//! [^TZANNES]: Tzannes et al. 2024, <https://dl.acm.org/doi/pdf/10.1145/2629643>

#![no_std]
#![cfg_attr(feature = "shuttle", allow(dead_code))]
#![cfg_attr(feature = "shuttle", allow(unused_imports))]

// -----------------------------------------------------------------------------
// Boilerplate for building without the standard library

extern crate alloc;
extern crate std;

// -----------------------------------------------------------------------------
// Modules

mod compile_fail;
mod job;
mod latch;
mod scope;
mod thread_pool;
mod unwind;
mod util;

// -----------------------------------------------------------------------------
// Trait markers

#[doc(hidden)]
pub struct FnOnceMarker();

#[doc(hidden)]
pub struct FutureMarker();

// -----------------------------------------------------------------------------
// Top-level exports

pub use scope::Scope;
pub use scope::ScopedSpawn;
pub use thread_pool::Spawn;
pub use thread_pool::ThreadPool;
pub use thread_pool::Worker;
pub use thread_pool::Yield;
pub use thread_pool::block_on;
pub use thread_pool::join;
pub use thread_pool::scope;
pub use thread_pool::spawn;

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
    pub use core::sync::atomic::AtomicBool;
    pub use core::sync::atomic::AtomicPtr;
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
    pub use shuttle::sync::atomic::AtomicPtr;
    pub use shuttle::sync::atomic::AtomicU32;
    pub use shuttle::sync::atomic::Ordering;
    pub use shuttle::thread::Builder as ThreadBuilder;
    pub use shuttle::thread::JoinHandle;
    pub use shuttle::thread_local;

    pub use shuttle::rand::Rng;
    pub use shuttle::rand::thread_rng;

    // Available parallelism

    pub fn available_parallelism() -> std::io::Result<core::num::NonZero<usize>> {
        panic!("available_parallelism does not work on shuttle");
    }
}
