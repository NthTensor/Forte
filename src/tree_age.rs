//! Tracking how long a tree of `join`s has been running.

use core::time::Duration;

use crate::Worker;
use crate::time;

/// How long a tree of [`join`](Worker::join)s has been running, carried by
/// value down the recursion that builds it.
///
/// Near the end of a long tree, workers that run out of work would normally
/// park, and the last pieces of work then wait for them to be woken. Code that
/// builds a tree of `join`s can avoid this by passing a `TreeAge` to each
/// branch, updating it with [`TreeAge::mark_branch`] when the branch starts,
/// and calling [`TreeAge::mark_leaf`] when a leaf of work finishes. Once the
/// tree has been running for a while, `mark_leaf` shares the worker's queued
/// work with idle siblings right away and asks the worker to keep searching for
/// work rather than park when it runs out (see [`Worker::share_now`] and
/// [`Worker::search_until`]).
///
/// The age is measured from the first time a piece of the tree is stolen, so
/// a tree that never leaves the worker that started it never reads the clock.
/// It packs into 32 bits, so carrying it adds little to each `join`.
///
/// ```
/// # use forte::{TreeAge, Worker, ThreadPool};
/// fn sum(worker: &Worker, values: &[u64], age: TreeAge) -> u64 {
///     let age = age.mark_branch(worker);
///     if values.len() <= 1024 {
///         let total = values.iter().sum();
///         age.mark_leaf(worker);
///         return total;
///     }
///     let (left, right) = values.split_at(values.len() / 2);
///     let (a, b) = worker.join(|w| sum(w, left, age), |w| sum(w, right, age));
///     a + b
/// }
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// # THREAD_POOL.populate();
/// # let values: Vec<u64> = (0..100_000).collect();
/// # let total = THREAD_POOL.with_worker(|worker| sum(worker, &values, TreeAge::new()));
/// # assert_eq!(total, values.iter().sum());
/// # THREAD_POOL.depopulate();
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TreeAge(u32);

impl TreeAge {
    /// Set once the tree is known to have run for [`TreeAge::LONG_HEARTBEATS`].
    const LONG: u32 = 1 << 31;
    /// The bits holding when a piece of the tree was first stolen.
    const STAMP: u32 = Self::LONG - 1;
    /// Ticks are stored divided by `2^UNIT_SHIFT`.
    const UNIT_SHIFT: u32 = 6;

    /// The number of heartbeats a tree must have run, since a piece of it was
    /// first stolen, before its leaves share their work and search. Shorter
    /// trees finish before waking a thread would pay off.
    pub const LONG_HEARTBEATS: u64 = 8;

    /// How long a worker searches for work after a leaf of a long tree. About
    /// the time it takes to wake a parked thread.
    pub const SEARCH: Duration = Duration::from_micros(25);

    /// The age of a tree that has just started.
    #[inline]
    pub const fn new() -> TreeAge {
        TreeAge(0)
    }

    /// Updates the age at the start of a branch running on `worker`. Reads the
    /// clock only if the branch was stolen.
    #[inline]
    pub fn mark_branch(self, worker: &Worker) -> TreeAge {
        if worker.migrated() {
            self.stolen()
        } else {
            self
        }
    }

    /// Updates the age for a branch known to have been stolen: the first steal
    /// stamps the time, and later ones check whether the tree has become long.
    #[inline]
    pub fn stolen(self) -> TreeAge {
        if self.is_long() {
            return self;
        }
        let now = time::ticks();
        if self.0 == 0 {
            TreeAge(Self::stamp(now))
        } else if self.is_long_at(now) {
            TreeAge(self.0 | Self::LONG)
        } else {
            self
        }
    }

    /// Whether the tree is already known to be long.
    #[inline]
    pub fn is_long(self) -> bool {
        self.0 & Self::LONG != 0
    }

    /// Whether the tree is long at `now` (a [`time::ticks`] value).
    #[inline]
    pub fn is_long_at(self, now: u64) -> bool {
        if self.is_long() {
            return true;
        }
        let first = self.0 & Self::STAMP;
        if first == 0 {
            return false;
        }
        let heartbeat = time::duration_to_ticks(time::HEARTBEAT);
        let threshold = (Self::LONG_HEARTBEATS * heartbeat) >> Self::UNIT_SHIFT;
        u64::from(Self::stamp(now).wrapping_sub(first) & Self::STAMP)
            >= threshold
    }

    /// Call after a leaf of work finishes on `worker`. If the tree is known to
    /// be long, shares the worker's queued work and asks it to keep searching.
    #[inline]
    pub fn mark_leaf(self, worker: &Worker) {
        if self.is_long() {
            Self::share_and_search(worker, time::ticks());
        }
    }

    /// Like [`TreeAge::mark_leaf`], for a caller that has just read the clock:
    /// also counts the tree as long if it has become long by `now`.
    #[inline]
    pub fn mark_leaf_at(self, worker: &Worker, now: u64) {
        if self.is_long_at(now) {
            Self::share_and_search(worker, now);
        }
    }

    fn share_and_search(worker: &Worker, now: u64) {
        worker.share_now();
        worker.search_until(now + time::duration_to_ticks(Self::SEARCH));
    }

    fn stamp(now: u64) -> u32 {
        // Never zero, which means "not yet stolen".
        ((now >> Self::UNIT_SHIFT) as u32 & Self::STAMP) | 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_becomes_long_after_threshold() {
        let heartbeat = time::duration_to_ticks(time::HEARTBEAT);
        let start = 1_000_000_000;
        let age = TreeAge(TreeAge::stamp(start));
        assert!(!age.is_long_at(start));
        assert!(
            !age.is_long_at(start + (TreeAge::LONG_HEARTBEATS - 1) * heartbeat)
        );
        assert!(
            age.is_long_at(start + (TreeAge::LONG_HEARTBEATS + 1) * heartbeat)
        );
        assert!(!TreeAge::new().is_long_at(start));
    }
}
