//! A benchmark for fork-join workloads adapted from `chili`.

use std::collections::{HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};

use criterion::black_box;
use dashmap::DashSet;
use divan::Bencher;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const SIZES: &[usize] = &[8, 16, 32, 64, 128, 256, 512];

fn sizes() -> impl Iterator<Item = usize> {
    SIZES.iter().cloned()
}

// -----------------------------------------------------------------------------
// Benchmark

#[divan::bench(args = sizes(), threads = false)]
fn baseline(bencher: Bencher, size: usize) {
    bencher.bench_local(move || {
        let mut visited = HashSet::new();

        let mut queue = VecDeque::new();
        queue.push_back((size / 2, size / 2));

        while let Some((x, y)) = queue.pop_front() {
            // Visit the y - 1 square
            if y > 0 {
                if visited.insert((x, y - 1)) {
                    queue.push_back((x, y - 1));
                }
            }

            // Visit the y + 1 square
            if y < size - 1 {
                if visited.insert((x, y + 1)) {
                    queue.push_back((x, y + 1));
                }
            }

            // Visit the x - 1 square
            if x > 0 {
                if visited.insert((x - 1, y)) {
                    queue.push_back((x - 1, y));
                }
            }

            // Visit the x + 1 square
            if x < size - 1 {
                if visited.insert((x + 1, y)) {
                    queue.push_back((x + 1, y));
                }
            }

            for i in 0..200 {
                let mut s = DefaultHasher::new();
                i.hash(&mut s);
                black_box(s.finish());
            }
        }
    });
}

static COMPUTE: forte::ThreadPool = forte::ThreadPool::new();

#[divan::bench(args = sizes(), threads = false)]
fn forte(bencher: Bencher, size: usize) {
    use forte::{Scope, Worker};

    fn visit<'scope, 'env>(
        size: usize,
        visited: &'env DashSet<(usize, usize)>,
        x: usize,
        y: usize,
        scope: &'scope Scope<'scope, 'env>,
        worker: &Worker,
    ) {
        // Visit the y - 1 square
        if y > 0 {
            if visited.insert((x, y - 1)) {
                scope.spawn_on(worker, move |worker: &Worker| {
                    visit(size, visited, x, y - 1, scope, worker)
                });
            }
        }

        // Visit the y + 1 square
        if y < size - 1 {
            if visited.insert((x, y + 1)) {
                scope.spawn_on(worker, move |worker: &Worker| {
                    visit(size, visited, x, y + 1, scope, worker)
                });
            }
        }

        // Visit the x - 1 square
        if x > 0 {
            if visited.insert((x - 1, y)) {
                scope.spawn_on(worker, move |worker: &Worker| {
                    visit(size, visited, x - 1, y, scope, worker)
                });
            }
        }

        // Visit the x + 1 square
        if x < size - 1 {
            if visited.insert((x + 1, y)) {
                scope.spawn_on(worker, move |worker: &Worker| {
                    visit(size, visited, x + 1, y, scope, worker)
                });
            }
        }

        for i in 0..200 {
            let mut s = DefaultHasher::new();
            i.hash(&mut s);
            black_box(s.finish());
        }
    }

    COMPUTE.with_worker(|worker| {
        bencher.bench_local(|| {
            let visited = DashSet::new();

            worker.scope(|scope| {
                visit(size, &visited, size / 2, size / 2, scope, worker);
            });
        });
    });
}

#[divan::bench(args = sizes(), threads = false)]
fn rayon(bencher: Bencher, size: usize) {
    use rayon::{Scope, scope};

    fn visit<'scope>(
        size: usize,
        visited: &'scope DashSet<(usize, usize)>,
        x: usize,
        y: usize,
        scope: &Scope<'scope>,
    ) {
        // Visit the y - 1 square
        if y > 0 {
            if visited.insert((x, y - 1)) {
                scope.spawn(move |scope| {
                    visit(size, visited, x, y - 1, scope);
                });
            }
        }

        // Visit the y + 1 square
        if y < size - 1 {
            if visited.insert((x, y + 1)) {
                scope.spawn(move |scope| {
                    visit(size, visited, x, y + 1, scope);
                });
            }
        }

        // Visit the x - 1 square
        if x > 0 {
            if visited.insert((x - 1, y)) {
                scope.spawn(move |scope| {
                    visit(size, visited, x - 1, y, scope);
                });
            }
        }

        // Visit the x + 1 square
        if x < size - 1 {
            if visited.insert((x + 1, y)) {
                scope.spawn(move |scope| {
                    visit(size, visited, x + 1, y, scope);
                });
            }
        }

        for i in 0..200 {
            let mut s = DefaultHasher::new();
            i.hash(&mut s);
            black_box(s.finish());
        }
    }

    bencher.bench_local(|| {
        let visited = DashSet::new();

        scope(|scope| {
            visit(size, &visited, size / 2, size / 2, scope);
        });
    });
}

fn main() {
    let fmt_layer = fmt::layer()
        .without_time()
        .with_target(false)
        .with_thread_names(true)
        .compact();

    tracing_subscriber::registry().with(fmt_layer).init();

    COMPUTE.resize_to_available();

    divan::main();
}
