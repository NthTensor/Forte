//! Forte is a thread pool that uses lazy scheduling techniques to reduce latency and
//! maximize parallel utilization. It is a close cousin to rayon-core, and takes
//! loose inspiration from zig's `chili` library, the Beam virtual machine, and Java's
//! `ForkJoinPool`.
//!
//! It features:
//!
//! * Statically defined and dynamically sized thread pools.
//!
//! * Fully stack-allocated and inlined fork/join parallelism.
//!
//! * The ability to execute both closures and futures on the same pool.
//!
//! * A primitive for awaiting async work in non-async contexts without spinning.
//!
//! * An exposed unsafe api, built for low-level integration and customization.
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
//! workers assigned to a pool using [`ThreadPool::grow`], [`ThreadPool::shrink`]
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
//! to collaborate and help each other out with work.
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
//! // there are no workers to parallelize it).
//! THREAD_POOL.join(|_| println!("world"), |_| println!("hello "));
//!
//! // This will always print "hello world" (because join executes the second
//! // closure first when running in serial).
//! ```
//!
//! # Workers
//!
//! Thread pools are comprised of (and run on) workers, represented as instances
//! of the [`Worker`] type. All work done on the pool is done in a "worker
//! context" created by [`Membership::activate`]. The recommended way to access a
//! worker context for a specific pool is via [`ThreadPool::with_worker`].
//!
//! ```
//! # use forte::ThreadPool;
//! # static THREAD_POOL: ThreadPool = ThreadPool::new();
//! THREAD_POOL.with_worker(|worker_1| {     // <-- Sets up this thread as a worker.
//!     THREAD_POOL.with_worker(|worker_2| { // <-- Returns a reference to the existing worker.
//!         // These pointers are identical.
//!         assert!(std::ptr::eq(worker_1, worker_2));
//!     });                                  // <-- Leaving this scope does nothing.
//! });                                      // <-- Leaving this scope frees the worker.
//! ```
//!
//! Every worker holds a local queue of tasks, as well as metadata that allows
//! other workers on the pool to communicate with it and wake it from sleep.
//! When exiting the outermost scope (where the worker was actually allocated), all
//! tasks left in the local queue are executed.
//!
//! You will only ever receive `&Worker` references, because the worker is not
//! allowed to move or be mutably referenced. Workers are `!Send` and `!Sync`,
//! and are meant to represent local-only data.
//!
//! To access the current worker context, you can use [`Worker::map_current`] or
//! [`Worker::with_current`]. These allow executing work on arbitrary pools, and
//! can be used to write library code that works normally despite not knowing
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
//! ```
//!
//! # Core Operations
//!
//! Thread pools support five core operations:
//! * *Join.* Executes two non-static closures, possibly in parallel, and waits for them to complete.
//! * *Spawn.* Runs a static closure or future in the background.
//! * *Scope.* Runs multiple non-static closures or futures, and waits for them all to complete.
//! * *Block on.* Waits for a future to complete (outside of an async context).
//! * *Broadcast.* Runs the same operation across all workers.
//!
//!

#![no_std]

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
mod time;
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

pub use latch::Latch;
pub use scope::Scope;
pub use scope::SpawnScoped;
pub use thread_pool::Broadcast;
pub use thread_pool::DEFAULT_POOL;
pub use thread_pool::DefaultThreadPool;
pub use thread_pool::Membership;
pub use thread_pool::Spawn;
pub use thread_pool::Task;
pub use thread_pool::ThreadPool;
pub use thread_pool::Worker;
pub use thread_pool::Yield;
pub use thread_pool::block_on;
pub use thread_pool::broadcast;
pub use thread_pool::join;
pub use thread_pool::num_members;
pub use thread_pool::scope;
pub use thread_pool::spawn;
pub use thread_pool::spawn_broadcast;
pub use thread_pool::spawn_on;

// -----------------------------------------------------------------------------
// Platform Support

// This exists to make it easy to swap out the basic parallelism primitives.
// Currently there are no alternative implementations, but there may be in
// future.
mod platform {

    pub use core::sync::atomic::AtomicBool;
    pub use core::sync::atomic::AtomicPtr;
    pub use core::sync::atomic::AtomicU32;
    pub use core::sync::atomic::AtomicUsize;
    pub use core::sync::atomic::Ordering;
    pub use core::sync::atomic::fence;

    pub use alloc::sync::Arc;
    pub use std::sync::Mutex;
    pub use std::thread::Builder as ThreadBuilder;
    pub use std::thread::JoinHandle;
    pub use std::thread_local;

    pub use std::thread::available_parallelism;

    use std::sync::LazyLock;
    pub struct Lazy<T>(LazyLock<T>);

    impl<T> Lazy<T> {
        pub const fn new(init: fn() -> T) -> Self {
            Lazy(LazyLock::new(init))
        }

        pub fn get(&'static self) -> &'static T {
            LazyLock::force(&self.0)
        }
    }
}
