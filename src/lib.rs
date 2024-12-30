//! An async-compatible thread-pool aiming for "speed through simplicity".
//!
//! Forte is a parallel & async work scheduler designed to accommodate very large
//! workloads with many short-lived tasks. It replicates the `rayon_core` api
//! but with native support for futures and async tasks. It's design was
//! prompted by the needs of the bevy game engine, but should be applicable to
//! any problem that involves running both synchronous and asynchronous work
//! concurrently.
//!
//! The thread-pool provided by this crate does not employ work-stealing. Forte
//! instead uses "Heartbeat Scheduling", an alternative load-balancing technique
//! that (theoretically) provides provably small overheads and good utilization.
//! The end effect is that work is only parallelized every so often, allowing
//! more work to be done sequentially on each thread and amortizing the
//! synchronization overhead.
//!
//! # Acknowledgments
//!
//! Large portions of the code are direct ports from various versions of
//! `rayon_core`, with minor simplifications and improvements. We also relied
//! upon `chili` and `spice` for reference while writing the heartbeat
//! scheduling. Support for futures is based on an approach sketched out by
//! members of the `rayon` community to whom we are deeply indebted.

pub mod job;
pub mod latch;
pub mod scope;
pub mod thread_pool;

mod util;

pub mod prelude {
    pub use crate::{
        scope::Scope,
        thread_pool::{ThreadPool, WorkerThread},
    };
}
