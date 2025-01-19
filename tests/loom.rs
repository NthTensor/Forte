//! Tests using the `loom` testing framework.

#![cfg(loom)]
//#![allow(unused_must_use)]
#![allow(clippy::useless_vec)]

use core::hint::black_box;

use async_task::Task;
use loom::model::Builder;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Condvar, Mutex};

use tracing::{info, Level};
use tracing_subscriber::fmt::Subscriber;

use forte::prelude::*;

// -----------------------------------------------------------------------------
// Infrastructure

fn model<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let subscriber = Subscriber::builder()
        .with_max_level(Level::ERROR)
        .with_test_writer()
        .without_time()
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let mut model = Builder::new();
        model.log = true;
        model.check(f);
    });
}

/// Provides access to a thread pool which can be treated as static for the
/// purposes of testing.
fn with_thread_pool<F>(f: F)
where
    F: Fn(&'static ThreadPool) + 'static,
{
    info!("### SETTING UP TEST");

    // Create a new thread pool.
    let thread_pool = Box::new(ThreadPool::new());
    let ptr = Box::into_raw(thread_pool);

    // SAFETY: We want to create have a reference to the thread pool which can
    // be treated as `'static` by the callback `f`. We will assume that `f` has
    // no side-effects except for those created by calls to the thread pool.
    // This problem comes down to ensuring that `thread_pool` lives for the
    // duration of `f` and also outlives anything spawned onto the pool by `f`.
    //
    // The first condition is easily satisfied: the thread pool is not dropped
    // until the end of the scope. For the latter condition, the call to
    // `wait_until_inactive()` blocks the thread until all work spawned onto the
    // pool completes, and `resize_to(0)` blocks until all of the pool's threads
    // terminate.
    //
    // For all intents and purposes, so long as `f` has no other side-effects,
    // `thread_pool` can be treated as if it has a `'static` lifetime within
    // `f`.
    unsafe {
        let thread_pool = &*ptr;
        info!("### POPULATING POOL");
        thread_pool.populate();
        info!("### STARTING TEST");
        f(thread_pool);
        info!("### SHUTTING DOWN POOL");
        thread_pool.resize_to(0);
        // This assert ensures that all spawned jobs are run.
        assert!(thread_pool.pop().is_none());
    };

    // SAFETY: This was created by `Box::into_raw`.
    let thread_pool = unsafe { Box::from_raw(&mut *ptr) };
    drop(thread_pool);

    info!("### TEST COMPLETE");
}

// -----------------------------------------------------------------------------
// Workload tracking

struct Workload {
    counter: AtomicUsize,
    is_done: Mutex<bool>,
    completed: Condvar,
}

impl Workload {
    fn new(count: usize) -> Workload {
        Workload {
            counter: AtomicUsize::new(count),
            is_done: Mutex::new(false),
            completed: Condvar::new(),
        }
    }

    fn execute(&self) {
        if 1 == self.counter.fetch_sub(1, Ordering::Relaxed) {
            let mut is_done = self
                .is_done
                .lock()
                .expect("failed to acquire workload lock");
            *is_done = true;
            self.completed.notify_all();
        }
    }

    fn wait_until_complete(&self) {
        let mut is_done = self
            .is_done
            .lock()
            .expect("failed to acquire workload lock");
        while !*is_done {
            is_done = self
                .completed
                .wait(is_done)
                .expect("failed to reacquire workload lock");
        }
    }
}

// -----------------------------------------------------------------------------
// Pool resizing

/// Tests for concurrency issues within the `with_thread_pool` helper function.
/// This spins up a thread pool with a single thread, then spins it back down.
#[test]
pub fn empty() {
    model(|| {
        with_thread_pool(|_threads| {});
    });
}

/// Tests for concurrency issues when increasing the size of the pool.
#[test]
pub fn resize_grow() {
    model(|| {
        with_thread_pool(|threads| {
            threads.grow(1);
        });
    });
}

/// Tests for concurrency issues when shrinking the size of the pool.
#[test]
pub fn resize_shrink() {
    model(|| {
        with_thread_pool(|threads| {
            threads.shrink(1);
        });
    });
}

// -----------------------------------------------------------------------------
// Core API

/// Tests for concurrency issues when spawning a static closure.
#[test]
pub fn spawn_closure() {
    model(|| {
        with_thread_pool(|threads| {
            let workload: &Workload = Box::leak(Box::new(Workload::new(1)));
            threads.spawn(|| {
                workload.execute();
            });
            workload.wait_until_complete();
        });
    });
}

/// Tests for concurrency issues when spawning a static future.
#[test]
pub fn spawn_future() {
    model(|| {
        with_thread_pool(|threads| {
            let workload: &Workload = Box::leak(Box::new(Workload::new(1)));
            let task = threads.spawn_future(async {
                workload.execute();
            });
            task.detach();
            workload.wait_until_complete();
        });
    });
}

/// Tests for concurrency issues in join operations.
#[test]
pub fn join() {
    model(|| {
        with_thread_pool(|threads| {
            threads.join(|| black_box(()), || black_box(()));
        });
    });
}

/// Tests for concurrency issues when blocking on a future.
#[test]
pub fn block_on() {
    model(|| {
        with_thread_pool(|threads| {
            threads.block_on(async {
                black_box(());
            });
        });
    });
}

/// Tests for concurrency issues when spawning a future and then blocking on the
/// resulting task.
#[test]
pub fn spawn_and_block() {
    model(|| {
        with_thread_pool(|threads| {
            let task = threads.spawn_future(async {
                black_box(());
            });
            threads.block_on(task);
        });
    });
}

// -----------------------------------------------------------------------------
// Scoped API

/// Test for concurrency issues when creating a scope.
#[test]
pub fn scope_empty() {
    model(|| {
        with_thread_pool(|threads| {
            threads.scope(|_| {});
        });
    });
}

/// Tests for concurrency issues when returning a value from a scope.
#[test]
fn scope_result() {
    model(|| {
        with_thread_pool(|threads| {
            let x = threads.scope(|_| 22);
            assert_eq!(x, 22);
        });
    });
}

/// Tests for concurrency issues when spawning a scoped closure.
#[test]
pub fn scope_spawn() {
    model(|| {
        with_thread_pool(|threads| {
            let vec = vec![1, 2, 3];
            threads.scope(|scope| {
                scope.spawn(|_| {
                    black_box(vec.len());
                });
            });
        });
    });
}

/// Tests for concurrency issues when spawning multiple scoped closures.
#[test]
pub fn scope_two() {
    model(|| {
        with_thread_pool(|threads| {
            let counter = &AtomicUsize::new(0);
            threads.scope(|scope| {
                scope.spawn(|_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
                scope.spawn(|_| {
                    counter.fetch_add(10, Ordering::SeqCst);
                });
            });
            let v = counter.load(Ordering::SeqCst);
            assert_eq!(v, 11);
        });
    });
}

/// Tests for concurrency issues when spawning a scoped future, and blocking on
/// it.
#[test]
pub fn scope_future() {
    model(|| {
        with_thread_pool(|threads| {
            let vec = vec![1, 2, 3];
            let mut task: Option<Task<usize>> = None;
            threads.scope(|scope| {
                let scoped_task = scope.spawn_future(async { black_box(vec.len()) });
                task = Some(scoped_task);
            });
            let task = task.expect("task should be initialized after scoped spawn");
            let len = threads.block_on(task);
            assert_eq!(len, vec.len());
        });
    });
}
