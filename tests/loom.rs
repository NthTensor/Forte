//! Tests using the `loom` testing framework.

#![cfg(loom)]

use core::hint::black_box;

use loom::model::Builder;
use tracing::{info, Level};
use tracing_subscriber::fmt::Subscriber;

use forte::prelude::*;

fn model<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let subscriber = Subscriber::builder()
        .with_max_level(Level::DEBUG)
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
    F: Fn(&'static ThreadPool),
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
        info!("### FINISHING TEST");
        thread_pool.wait_until_inactive();
        info!("### SHUTTING DOWN POOL");
        thread_pool.resize_to(0);
    };

    // SAFETY: This was created by `Box::into_raw`.
    let thread_pool = unsafe { Box::from_raw(&mut *ptr) };
    drop(thread_pool);

    info!("### TEST COMPLETE");
}

// Test of the `with_thread_pool` helper function. This spins up a thread pool
// with a single thread, then spins it back down.
#[test]
pub fn resize_one() {
    model(|| {
        with_thread_pool(|_| {});
    });
}

// Tests increasing the size of the pool
#[test]
pub fn resize_grow() {
    model(|| {
        with_thread_pool(|threads| {
            threads.grow(1);
        });
    });
}

// Tests shrinking the size of the pool
#[test]
pub fn resize_shrink() {
    model(|| {
        with_thread_pool(|threads| {
            threads.shrink(1);
        });
    });
}

#[test]
pub fn join() {
    model(|| {
        with_thread_pool(|threads| {
            threads.join(|| black_box(threads), || black_box(threads));
        });
    });
}

#[test]
pub fn block_on() {
    model(|| {
        with_thread_pool(|threads| {
            threads.block_on(async {
                black_box(threads);
            });
        });
    });
}

#[test]
pub fn spawn_closure() {
    model(|| {
        with_thread_pool(|threads| {
            threads.spawn(move || {
                black_box(threads);
            });
        });
    });
}

#[test]
pub fn spawn_future() {
    model(|| {
        with_thread_pool(|threads| {
            let task = threads.spawn_future(async move {
                black_box(threads);
            });
            task.detach();
        });
    });
}

#[test]
pub fn spawn_and_block() {
    model(|| {
        with_thread_pool(|threads| {
            let task = threads.spawn_future(async move {
                black_box(threads);
            });
            threads.block_on(task);
        });
    });
}
