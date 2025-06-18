//! Tests using the Shuttle testing framework.

#![cfg(feature = "shuttle")]
#![allow(unused_imports)]

use forte::ThreadPool;
use shuttle::hint::black_box;
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
fn with_thread_pool<F>(f: F)
where
    F: Fn(&'static ThreadPool) + 'static,
{
    let thread_pool = Box::new(ThreadPool::new());
    let ptr = Box::into_raw(thread_pool);

    // SAFETY: TODO
    unsafe {
        let thread_pool = &*ptr;
        f(thread_pool);
    };

    // SAFETY: TODO
    let thread_pool = unsafe { Box::from_raw(&mut *ptr) };
    drop(thread_pool);
}

// -----------------------------------------------------------------------------
// Pool resizing

/// Tests for concurrency issues within the `with_thread_pool` helper function.
/// This spins up a thread pool with a single thread, then spins it back down.
#[test]
pub fn thread_pool() {
    fn resize(thread_pool: &'static ThreadPool) {
        thread_pool.resize_to(3);
        thread_pool.resize_to(0);
    }

    shuttle::check_dfs(|| with_thread_pool(resize), None);
}

// -----------------------------------------------------------------------------
// Core API

/*

/// Tests for concurrency issues when spawning a static closure.
#[test]
pub fn spawn_closure() {
    fn spawn(thread_pool: &'static ThreadPool) {
        thread_pool.spawn(|worker| {
            black_box(worker);
        });
    }

    shuttle::check_pct(|| with_thread_pool(spawn), 200, 200);
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

*/
