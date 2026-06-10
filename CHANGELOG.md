
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog],
and this project adheres to [Semantic Versioning].

This project is currently in early [pre-release], and there may be arbitrary breaking changes between pre-release versions.

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html
[pre-release]: https://semver.org/spec/v2.0.0.html#spec-item-9

## [Unreleased]

### Added 

- `Worker::spawn_local` for spawning `!Send` work.
- `Worker::broadcast`, `ThreadPool::spawn_broadcast`, and `broadcast` for blocking broadcasts.
- `ThreadPool::broadcast`, `ThreadPool::spawn_broadcast`, and `spawn_broadcast` for non-blocking broadcasts.
- `ThreadPool::num_members` method which returns the current number of member threads.
- `ThreadPool::get_worker` which looks up the worker if it exists.
- `ThreadPool::enroll` which requests membership and blocks until it is granted.
- `ThreadPool::try_enroll` which requests a membership and returns None if none are available.

### Changed

- `Lease` and `StackJob` have been refactored to improve stack utilization.
- Work sharing has been rewritten to improve performance.
- Thread pools can now have a max of 32 members at a time.
- `ThreadPool::with_worker` now waits for a membership to become available.
- `spawn`, `Scope::spawn`, and `Worker::spawn` now accept closures and futures.
- `Lease` is now called `Membership`.
- `Scope` now has two lifetimes instead of one, and is more flexible.

### Removed

- All versions of `spawn_future` and `spawn_async`; just use `spawn` instead.
- `claim_lease` has been replaced with `try_enroll`.
- `Worker::occupy` has been replaced with `Membership::activate`.
- Removed the shuttle testing framework (it's incompatible with crossbeam queues).

## [1.0.0-alpha.4]

### Added

- `forte-rayon-compat` crate, for running rayon on top of forte.
- `Worker::migrated()` which is `true` when the current job has moved between threads.

### Changed

- Heartbeat frequency is now 100 microseconds.
- Heartbeats are now more evenly distributed between workers.
- The heartbeat thread goes to sleep when the pool is not in use.
- Shared work is now queued and claimed in the order it was shared.
- The `Scope` API is now identical to `rayon-core` and does not use `Pin<T>`.
- `Scope::new`, `Scope::add_reference` and `Scope::remove_reference` are now private.

### Security

- Forte now implements exception safety. Panics can no longer cause UB.

## [1.0.0-alpha.3]

### Added

- `ThreadPool::claim_lease` method.
- `shuttle` test suite and feature flag (temporarily disabled).

### Changed

- Rename `ThreadPool::as_worker` to `ThreadPool::with_worker`.
- Make `Scope` public.
- Make `Yield` public. 
- Improved safety comments for `scope`.
- Code format no longer requires nightly.

### Removed

- `loom` cfg flag and test suite.

### Fixed 

- Fixed some broken examples and doc-tests.
- Various broken links, spelling mistakes, and indentations.
- Documentation for `yield_now` and `yield_local`.

## [1.0.0-alpha.2]

This was the first usable build of forte, and is the start point for this change log. All subsequent changes are relative to this pre-release.

## [1.0.0-alpha.1]

The initial release of this library was intended only to claim the name on `crates.io`. It was still early in development and did not employ a change-log.
