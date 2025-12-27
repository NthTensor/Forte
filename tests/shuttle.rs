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

/*

fn trace<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let subscriber = Subscriber::builder()
        .compact()
        .with_max_level(Level::TRACE)
        .without_time()
        .with_thread_names(false)
        .finish();

    tracing::subscriber::with_default(subscriber, f);
}

*/

/// Provides access to a thread pool which can be treated as static for the
/// purposes of testing.
fn with_thread_pool<F>(f: F) -> impl Fn() + 'static
where
    F: Fn(&'static ThreadPool) + 'static,
{
    move || {
        let thread_pool = Box::new(ThreadPool::new());
        let thread_pool_ptr = Box::into_raw(thread_pool);

        // SAFETY: TODO
        let thread_pool_ref = unsafe { &*thread_pool_ptr };
        f(thread_pool_ref);

        // SAFETY: TODO
        let thread_pool = unsafe { Box::from_raw(&mut *thread_pool_ptr) };
        drop(thread_pool);
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

    shuttle::check_dfs(test, None);
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

    shuttle::check_dfs(test, None);
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

    shuttle::check_dfs(test, None);
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

    shuttle::check_pct(test, 100_000, 10_000);
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

    shuttle::check_pct(test, 100_000, 10_000);
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

    shuttle::check_pct(test, 100_000, 10_000);
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
        pool.with_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, [1; 10]);

        pool.depopulate();
    });

    shuttle::check_pct(test, 100_000, 10_000);
}

/*

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

*/
