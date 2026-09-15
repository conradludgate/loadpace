# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-rc.5](https://github.com/conradludgate/loadpace/compare/loadpace-v0.1.0-rc.4...loadpace-v0.1.0-rc.5) - 2026-09-15

### Added

- *(core)* simplify endpoint configuration
- *(core)* decay pacing without feedback

## [0.1.0-rc.4](https://github.com/conradludgate/loadpace/compare/loadpace-v0.1.0-rc.3...loadpace-v0.1.0-rc.4) - 2026-09-15

### Added

- *(core)* make probing controller driven

### Fixed

- *(probes)* add positive probes in rate space
- *(gradient)* use absolute queue tolerance
- *(core)* saturate extreme time arithmetic
- *(controller)* record observed pacing waits
- *(simulator)* advance sub-nanosecond arrivals

### Other

- *(core)* detail controller method contracts
- define trusted deployment boundary
- *(core)* complete public API rustdoc
- *(core)* cover controller lifecycle edges
- *(fairness)* model heterogeneous network RTT
- *(core)* remove unreachable scheduling error
- *(controller)* remove redundant request tracking
- *(controller)* own load refresh sequencing
- *(controller)* own dispatch transition timing
- *(adapters)* hide probe controls
- *(core)* document probe validation panics
- *(core)* remove avoidable panic paths

### Changed

- Make `dispatch_state` own time-driven controller refreshes and return probe
  transition deadlines to adapters.
- Make `on_dispatched` return the current `DispatchState` when a reservation
  cannot be committed atomically.

## [0.1.0-rc.3](https://github.com/conradludgate/loadpace/compare/loadpace-v0.1.0-rc.2...loadpace-v0.1.0-rc.3) - 2026-09-14

### Added

- *(adapters)* drive probes on queued demand

### Fixed

- *(controller)* repair gradient convenience method
- *(latency)* age the minimum RTT baseline
- *(controller)* time-gate healthy gradient updates

### Other

- *(api)* explain crate responsibilities
- *(readme)* explain Loadpace purpose
- *(controller)* compare fractional concurrency approximately
- format test imports
- *(config)* initialize probe schedules explicitly
- format workspace
- *(layout)* move core crate into workspace member
