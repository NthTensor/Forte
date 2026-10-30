//! A shallow, *wide* fork/join workload: many independent elements, each doing
//! only a little work, summed by recursively splitting the slice in half across
//! `join`. This is the shape of e.g. a `par_iter` over a few hundred roots that
//! finishes in tens of microseconds.
//!
//! Unlike the deep, fine-grained tree in `deep_tree`, the parallelism here is
//! genuinely worth exploiting — but only if work is exposed and stolen
//! *promptly*, because the whole fan-out is short-lived.
//!
//! Reports `baseline` (serial), `forte`, and `rayon`.

use divan::Bencher;
use forte::Worker;

static COMPUTE: forte::ThreadPool = forte::ThreadPool::new();

/// A chunk of CPU work with a real data dependency so it can't be optimized
/// away. `iters` sets the granularity.
#[inline]
fn compute(iters: u64) -> u64 {
    let mut seed = 0x9E3779B97F4A7C15u64.wrapping_add(iters);
    for i in 0..iters {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(i);
        seed ^= seed >> 17;
    }
    seed
}

fn run_serial(work: &[u64]) -> u64 {
    work.iter().fold(0, |acc, &w| acc ^ compute(w))
}

fn run_forte(work: &[u64], worker: &Worker) -> u64 {
    match work {
        [] => 0,
        [w] => compute(*w),
        _ => {
            let (a, b) = work.split_at(work.len() / 2);
            let (l, r) = worker.join(|w| run_forte(a, w), |w| run_forte(b, w));
            l ^ r
        }
    }
}

fn run_rayon(work: &[u64]) -> u64 {
    match work {
        [] => 0,
        [w] => compute(*w),
        _ => {
            let (a, b) = work.split_at(work.len() / 2);
            let (l, r) = rayon::join(|| run_rayon(a), || run_rayon(b));
            l ^ r
        }
    }
}

const WIDTHS: &[usize] = &[32, 128, 512, 2048];
/// A little work per element — coarse enough to not be a pure add, fine enough
/// that the fan-out is short-lived.
const WORK: u64 = 64;

fn work_for(width: usize) -> Vec<u64> {
    vec![WORK; width]
}

#[divan::bench(args = WIDTHS)]
fn baseline(bencher: Bencher, width: usize) {
    let work = work_for(width);
    bencher.bench_local(move || run_serial(&work));
}

#[divan::bench(args = WIDTHS)]
fn forte(bencher: Bencher, width: usize) {
    let work = work_for(width);
    COMPUTE.with_worker(|worker| {
        bencher.bench_local(move || run_forte(&work, worker));
    });
}

#[divan::bench(args = WIDTHS)]
fn rayon(bencher: Bencher, width: usize) {
    let work = work_for(width);
    bencher.bench_local(move || run_rayon(&work));
}

fn main() {
    COMPUTE.resize_to_available();
    divan::main();
}
