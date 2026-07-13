//! A benchmark for fork-join workloads adapted from `chili`.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use criterion::black_box;
use dashmap::DashSet;
use divan::Bencher;

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
    use forte::Scope;
    use forte::Worker;

    fn visit<'scope, 'env>(
        size: usize,
        visited: &'env DashSet<(usize, usize)>,
        x: usize,
        y: usize,
        scope: &'scope Scope<'scope, 'env>,
    ) {
        // Visit the y - 1 square
        if y > 0 {
            if visited.insert((x, y - 1)) {
                scope.spawn(move |_worker: &Worker| {
                    visit(size, visited, x, y - 1, scope)
                });
            }
        }

        // Visit the y + 1 square
        if y < size - 1 {
            if visited.insert((x, y + 1)) {
                scope.spawn(move |_worker: &Worker| {
                    visit(size, visited, x, y + 1, scope)
                });
            }
        }

        // Visit the x - 1 square
        if x > 0 {
            if visited.insert((x - 1, y)) {
                scope.spawn(move |_worker: &Worker| {
                    visit(size, visited, x - 1, y, scope)
                });
            }
        }

        // Visit the x + 1 square
        if x < size - 1 {
            if visited.insert((x + 1, y)) {
                scope.spawn(move |_worker: &Worker| {
                    visit(size, visited, x + 1, y, scope)
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
                visit(size, &visited, size / 2, size / 2, scope);
            });
        });
    });
}

#[divan::bench(args = sizes(), threads = false)]
fn rayon(bencher: Bencher, size: usize) {
    use rayon::Scope;
    use rayon::scope;

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
    COMPUTE.resize_to_available();

    divan::main();
}
