//! A benchmark for fork-join workloads adapted from `chili`.

use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use criterion::black_box;
use divan::Bencher;

const SIZES: &[usize] = &[8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4012, 8196];

fn sizes() -> impl Iterator<Item = usize> {
    SIZES.iter().cloned()
}

// -----------------------------------------------------------------------------
// Benchmark

#[divan::bench(args = sizes(), threads = false)]
fn baseline(bencher: Bencher, size: usize) {
    bencher.bench_local(move || {
        for i in 0..size {
            for j in 0..200 {
                let mut s = DefaultHasher::new();
                i.hash(&mut s);
                j.hash(&mut s);
                black_box(s.finish());
            }
        }
    });
}

static COMPUTE: forte::ThreadPool = forte::ThreadPool::new();

#[divan::bench(args = sizes(), threads = false)]
fn forte(bencher: Bencher, size: usize) {
    use forte::Worker;

    COMPUTE.with_worker(|worker| {
        bencher.bench_local(|| {
            worker.scope(|scope| {
                for i in 0..size {
                    scope.spawn(move |_: &Worker| {
                        for j in 0..200 {
                            let mut s = DefaultHasher::new();
                            i.hash(&mut s);
                            j.hash(&mut s);
                            black_box(s.finish());
                        }
                    });
                }
            });
        });
    });
}

#[divan::bench(args = sizes(), threads = false)]
fn rayon(bencher: Bencher, size: usize) {
    use rayon::scope;

    bencher.bench_local(|| {
        scope(|scope| {
            for i in 0..size {
                scope.spawn(move |_| {
                    for j in 0..200 {
                        let mut s = DefaultHasher::new();
                        i.hash(&mut s);
                        j.hash(&mut s);
                        black_box(s.finish());
                    }
                });
            }
        });
    });
}

fn main() {
    COMPUTE.resize_to_available();

    divan::main();
}
