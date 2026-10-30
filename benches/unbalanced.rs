//! An *unbalanced* fork/join workload: a flat list of elements where a few
//! dominate the runtime, summed by recursively splitting the slice in half
//! across `join`.
//!
//! Every 32nd element is heavy; the rest are light. Splitting the slice in half
//! lands the heavy elements unevenly, so a purely static partition strands most
//! workers on light work while a few grind through the heavy ones. Only prompt
//! work-stealing recovers the parallelism — this stresses whether exposed work
//! actually migrates to idle workers rather than being reclaimed by its
//! producer.
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

const WIDTHS: &[usize] = &[64, 256, 1024];
const LIGHT: u64 = 16;
const HEAVY: u64 = 20_000;

/// Every 32nd element is heavy; the rest are light.
fn work_for(width: usize) -> Vec<u64> {
    (0..width)
        .map(|i| if i % 32 == 0 { HEAVY } else { LIGHT })
        .collect()
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
