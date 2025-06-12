# Forte

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
