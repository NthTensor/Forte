//! Tests using the Shuttle testing framework.

#![cfg(feature = "shuttle")]
#![allow(unused_imports)]

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;

use forte::ThreadPool;
use forte::Worker;
use shuttle::hint::black_box;
use shuttle::sync::atomic::AtomicBool;
use shuttle::sync::atomic::AtomicUsize;
use shuttle::sync::atomic::Ordering;
use tracing::Level;
use tracing_subscriber::fmt::Subscriber;

// -----------------------------------------------------------------------------
// Infrastructure

/// Provides access to a thread pool which can be treated as static for the
/// purposes of testing.
fn with_thread_pool<F>(f: F) -> impl Fn() + 'static
where
    F: Fn(&'static ThreadPool) + 'static,
{
    move || {
        let thread_pool = Box::new(ThreadPool::new());
        let thread_pool_ptr = Box::into_raw(thread_pool);

        // SAFETY: This thread pool is never dropped.
        let thread_pool_ref = unsafe { &*thread_pool_ptr };
        f(thread_pool_ref);
    }
}

// -----------------------------------------------------------------------------
// Pool resizing

/// Tests for concurrency issues within the `with_thread_pool` helper function.
/// This spins up a thread pool with a single thread, then spins it back down.
#[test]
pub fn shuttle_populate_depopulate() {
    let test = with_thread_pool(|pool| {
        pool.populate();
        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

// -----------------------------------------------------------------------------
// Core API

/// Tests spawning a worker on a pool of size one.
#[test]
pub fn shuttle_spawn_closure() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(1);
        pool.spawn(|_: &Worker| {});
        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

#[derive(Default)]
struct CountFuture {
    count: usize,
}

impl Future for CountFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.count == 128 {
            Poll::Ready(())
        } else {
            self.count += 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Tests spawning a nontrivial future on a pool of size one.
#[test]
pub fn shuttle_spawn_future() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(1);
        let task = pool.spawn(CountFuture::default());
        assert!(task.is_finished());
        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

/// Tests a two-level join operation on a pool of size one.
#[test]
pub fn join_4_on_1() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(1);

        let counter = AtomicUsize::new(0);
        pool.join(
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
        );
        assert_eq!(counter.load(Ordering::Relaxed), 4);

        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

/// Tests a two-level join operation on a pool of size two.
#[test]
pub fn join_4_on_2() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(2);

        let counter = AtomicUsize::new(0);
        pool.join(
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
        );
        assert_eq!(counter.load(Ordering::Relaxed), 4);

        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

/// Tests a two-level join operation on a pool of size three.
#[test]
pub fn join_4_on_3() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(3);

        let counter = AtomicUsize::new(0);
        pool.join(
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
            |worker| {
                worker.join(
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                    |_| counter.fetch_add(1, Ordering::Relaxed),
                )
            },
        );
        assert_eq!(counter.load(Ordering::Relaxed), 4);

        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}

/// Tests a moderately deep join operation on a large pool.
#[test]
pub fn join_long() {
    let test = with_thread_pool(|pool| {
        pool.resize_to(8);

        fn increment(worker: &Worker, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (head, tail) = slice.split_at_mut(1);

                    worker.join(|_| head[0] += 1, |worker| increment(worker, tail));
                }
            }
        }

        let mut vals = [0; 10];
        pool.expect_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, [1; 10]);

        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 100_000);
}
