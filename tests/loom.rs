//! Tests using the `loom` testing framework.

#![cfg(loom)]
//#![allow(unused_must_use)]
#![allow(clippy::useless_vec)]

use core::hint::black_box;

use async_task::Task;
use forte::latch::Latch;
use forte::prelude::*;
use loom::model::Builder;
use loom::sync::atomic::AtomicBool;
use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering;
use tracing::Level;
use tracing::info;
use tracing_subscriber::fmt::Subscriber;

// -----------------------------------------------------------------------------
// Infrastructure

fn model<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let subscriber = Subscriber::builder()
        .compact()
        .with_max_level(Level::TRACE)
        .without_time()
        .with_thread_names(false)
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
    F: Fn(&'static ThreadPool, &Worker) + 'static,
{
    info!("### SETTING UP TEST");

    // Create a new thread pool.
    let thread_pool = Box::new(ThreadPool::new());
    let ptr = Box::into_raw(thread_pool);

    // SAFETY: We want to create a reference to the thread pool which can
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
        thread_pool.resize_to(2);
        info!("### STARTING TEST");
        thread_pool.as_worker(|worker| {
            let worker = worker.unwrap();
            f(thread_pool, worker);
        });
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
// Latches

#[test]
pub fn latch() {
    model(|| {
        let flag = Box::leak(Box::new(AtomicBool::new(false)));
        let latch = Box::leak(Box::new(Latch::new()));
        loom::thread::spawn(|| {
            flag.store(true, Ordering::Release);
            latch.set();
        });
        latch.wait();
        assert!(flag.load(Ordering::Acquire));
    });
}

#[test]
pub fn multi_latch() {
    model(|| {
        let a = Box::leak(Box::new(Latch::new()));
        let b = Box::leak(Box::new(Latch::new()));
        loom::thread::spawn(|| {
            a.set();
            b.wait();
            a.set();
        });
        a.wait();
        b.set();
        a.wait();
    });
}

// -----------------------------------------------------------------------------
// Pool resizing

/// Tests for concurrency issues within the `with_thread_pool` helper function.
/// This spins up a thread pool with a single thread, then spins it back down.
#[test]
pub fn thread_pool() {
    model(|| {
        with_thread_pool(|_, _| black_box(()));
    });
}

// -----------------------------------------------------------------------------
// Core API

/// Tests for concurrency issues when spawning a static closure.
#[test]
pub fn spawn_closure() {
    model(|| {
        with_thread_pool(|_, worker| {
            let complete = Box::leak(Box::new(AtomicBool::new(false)));
            worker.spawn(|_| {
                complete.store(true, Ordering::Release);
            });
            worker.run_until(&complete);
        });
    });
}

/// Tests for concurrency issues when spawning a static future.
#[test]
pub fn spawn_future() {
    model(|| {
        with_thread_pool(|_, worker| {
            let complete = Box::leak(Box::new(AtomicBool::new(false)));
            let task = worker.spawn_future(async {
                complete.store(true, Ordering::Release);
            });
            task.detach();
            worker.run_until(&complete);
        });
    });
}

/// Tests for concurrency issues in join operations.
#[test]
pub fn join() {
    model(|| {
        with_thread_pool(|_, worker| {
            worker.join(|_| black_box(()), |_| black_box(()));
        });
    });
}

/// Tests for concurrency issues when blocking on a future.
#[test]
pub fn block_on() {
    model(|| {
        with_thread_pool(|_, worker| {
            worker.block_on(async {
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
        with_thread_pool(|_, worker| {
            let task = worker.spawn_future(async {
                black_box(());
            });
            worker.block_on(task);
        });
    });
}

// -----------------------------------------------------------------------------
// Scoped API

/// Test for concurrency issues when creating a scope.
#[test]
pub fn scope_empty() {
    model(|| {
        with_thread_pool(|_, worker| {
            worker.scope(|_| {});
        });
    });
}

/// Tests for concurrency issues when returning a value from a scope.
#[test]
fn scope_result() {
    model(|| {
        with_thread_pool(|_, worker| {
            let result = worker.scope(|_| 22);
            assert_eq!(result, 22);
        });
    });
}

/// Tests for concurrency issues when spawning a scoped closure.
#[test]
pub fn scope_spawn() {
    model(|| {
        with_thread_pool(|_, worker| {
            let complete = AtomicBool::new(false);
            worker.scope(|scope| {
                scope.spawn(|_| {
                    complete.store(true, Ordering::Release);
                });
            });
            worker.run_until(&complete);
        });
    });
}

/// Tests for concurrency issues when spawning multiple scoped closures.
#[test]
pub fn scope_two() {
    model(|| {
        with_thread_pool(|_, worker| {
            let counter = &AtomicUsize::new(0);
            worker.scope(|scope| {
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
        with_thread_pool(|_, worker| {
            let vec = vec![1, 2, 3];
            let task = worker.scope(|scope| scope.spawn_future(async { black_box(vec.len()) }));
            let len = worker.block_on(task);
            assert_eq!(len, vec.len());
        });
    });
}
