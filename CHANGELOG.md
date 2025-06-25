# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog],
and this project adheres to [Semantic Versioning].

This project is currently in early [pre-release], and there may be arbitrary breaking changes between pre-release versions.

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html
[pre-release]: https://semver.org/spec/v2.0.0.html#spec-item-9

## [Unreleased]

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
