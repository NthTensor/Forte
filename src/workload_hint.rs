//! A hint that keeps workers from parking near the end of a long workload.

use crate::Worker;
use crate::time;

/// Marks the start of a parallel workload, such as a tree of
/// [`join`](Worker::join)s.
///
/// Near the end of a workload, workers that run out of work park, and the last
/// parts may wait for a parked worker to wake. To avoid this, create a hint
/// when the workload starts, copy it into each part, and call
/// [`WorkloadHint::complete_part`] when a part finishes. If the workload has
/// run for a long time, `complete_part` keeps the worker searching instead of
/// parking (see [`Worker::search_until`]).
///
/// ```
/// # use forte::{ThreadPool, WorkloadHint, Worker};
/// fn sum(worker: &Worker, values: &[u64], hint: WorkloadHint) -> u64 {
///     if values.len() <= 1024 {
///         let total = values.iter().sum();
///         hint.complete_part(worker);
///         return total;
///     }
///     let (left, right) = values.split_at(values.len() / 2);
///     let (a, b) =
///         worker.join(|w| sum(w, left, hint), |w| sum(w, right, hint));
///     a + b
/// }
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// # THREAD_POOL.populate();
/// # let values: Vec<u64> = (0..100_000).collect();
/// let total = THREAD_POOL
///     .with_worker(|worker| sum(worker, &values, WorkloadHint::new()));
/// # assert_eq!(total, values.iter().sum());
/// # THREAD_POOL.depopulate();
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkloadHint(u32);

impl WorkloadHint {
    /// Ticks are stored divided by `2^UNIT_SHIFT`, and wrap at 32 bits.
    const UNIT_SHIFT: u32 = 6;

    /// How many heartbeats a workload must run before
    /// [`WorkloadHint::complete_part`] keeps the worker searching. Searching
    /// costs CPU time, so it is kept for long workloads.
    const LONG_HEARTBEATS: u64 = 8;

    /// Marks the start of a workload. Reads the clock.
    #[inline]
    pub fn new() -> WorkloadHint {
        WorkloadHint(Self::pack(time::ticks()))
    }

    /// Whether the workload has run for [`WorkloadHint::LONG_HEARTBEATS`], as
    /// of `worker`'s most recent heartbeat check.
    #[inline]
    fn is_long(self, worker: &Worker) -> bool {
        let checked = worker.last_heartbeat_check();
        // A worker that has never checked the heartbeat has no reading.
        if checked == 0 {
            return false;
        }
        // A reading from before the workload started wraps to a huge value.
        // (A reading from hours earlier can wrap back into range, which at
        // worst makes one part keep its worker searching needlessly.)
        let elapsed = Self::pack(checked).wrapping_sub(self.0);
        elapsed <= u32::MAX / 2 && elapsed >= Self::threshold(worker)
    }

    /// Call when a part of the workload finishes on `worker`. If the workload
    /// is long, asks it to keep searching rather than park.
    #[inline]
    pub fn complete_part(self, worker: &Worker) {
        if self.is_long(worker) {
            worker.search_until(
                time::ticks()
                    + time::duration_to_ticks(Worker::SEARCH_DURATION),
            );
        }
    }

    #[inline]
    fn pack(ticks: u64) -> u32 {
        (ticks >> Self::UNIT_SHIFT) as u32
    }

    #[inline]
    fn threshold(worker: &Worker) -> u32 {
        let ticks = Self::LONG_HEARTBEATS * worker.heartbeat_ticks();
        u32::try_from(ticks >> Self::UNIT_SHIFT).unwrap_or(u32::MAX / 2)
    }
}

impl Default for WorkloadHint {
    /// Marks the start of a workload now (see [`WorkloadHint::new`]).
    fn default() -> WorkloadHint {
        WorkloadHint::new()
    }
}
