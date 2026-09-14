# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
