# Forte

[![Crates.io](https://img.shields.io/crates/v/forte.svg)](https://crates.io/crates/forte)
[![Docs](https://docs.rs/forte/badge.svg)](https://docs.rs/forte/latest/forte/)

An async-compatible thread-pool aiming for "speed through simplicity".

Forte is a parallel & async work scheduler designed to accommodate very large workloads with many short-lived tasks. It replicates the `rayon_core` api but with native support for futures and async tasks. 
Its design was prompted by the needs of the bevy game engine, but should be applicable to any problem that involves running both synchronous and asynchronous work concurrently.

The thread-pool provided by this crate does not employ work-stealing. 
Forte instead uses "Heartbeat Scheduling", an alternative load-balancing technique that (theoretically) provides provably small overheads and good utilization.
The end effect is that work is only parallelized every so often, allowing more work to be done sequentially on each thread and amortizing the synchronization overhead.

# Acknowledgments

Large portions of the code are direct ports from various versions of `rayon_core`, with minor simplifications and improvements. 
We also relied upon `chili` and `spice` for reference while writing the heartbeat scheduling.
Support for futures is based on an approach sketched out by members of the `rayon` community to whom we are deeply indebted.

# License

Forte is distributed under the terms of both the MIT license and the Apache License (Version 2.0).
See LICENSE-APACHE and LICENSE-MIT for details.
Opening a pull request is assumed to signal agreement with these licensing terms.

## Shutdown semantics

Heap-allocated jobs created via `HeapJob::into_job_ref` are owned by the
job reference and will be dropped when executed. If a `JobRef` is placed
into the shared injector but never executed (for example, if the pool is
shut down before the job is drained) the allocation may be leaked. This is
an intentional tradeoff to avoid adding complex bookkeeping to the fast
path of job scheduling.

If you need to ensure that all remaining shared jobs are executed (and thus
freed) before shutdown, call `ThreadPool::drain_execute()` which will run
remaining shared jobs on the calling thread. Note this drains only the
shared injector queue; work that remains in other workers' local deques may
not be reachable by this call.
