# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-rc.4](https://github.com/conradludgate/loadpace/compare/loadpace-rama-v0.1.0-rc.3...loadpace-rama-v0.1.0-rc.4) - 2026-09-15

### Added

- *(core)* simplify endpoint configuration

### Fixed

- *(adapters)* preserve feedback silence on cancellation

### Other

- *(rama)* align DNS scenarios with endpoint assumptions

## [0.1.0-rc.3](https://github.com/conradludgate/loadpace/compare/loadpace-rama-v0.1.0-rc.2...loadpace-rama-v0.1.0-rc.3) - 2026-09-15

### Added

- *(rama)* add adaptive DNS p2c balancing

### Fixed

- *(rama)* declare dependency MSRV

### Other

- *(core)* detail controller method contracts
- define trusted deployment boundary
- *(adapters)* clarify API and deployment scope
- *(rama)* satisfy current clippy
- *(controller)* own load refresh sequencing
- delegate changelogs to release-plz
- *(controller)* own dispatch transition timing
- *(adapters)* hide probe controls
- *(adapters)* clarify dispatch lifecycle
- *(adapters)* make internal state explicit
- *(adapters)* model request lifecycle explicitly

## [0.1.0-rc.2](https://github.com/conradludgate/loadpace/releases/tag/loadpace-rama-v0.1.0-rc.2) - 2026-09-14

### Added

- *(adapters)* drive probes on queued demand
- *(rama)* implement adaptive service
- *(rama)* add adapter crate scaffold

### Fixed

- *(adapters)* use rand 0.10 make_rng
- *(latency)* age the minimum RTT baseline
- *(adapters)* arm dispatch notifications before checks
- *(rama)* match service future contract
- *(rama)* expose owned service futures

### Other

- *(api)* explain crate responsibilities
- *(readme)* explain Loadpace purpose
- *(rama)* assert positive probe dispatches demand
- *(rama)* advance paused clock for probes
- format workspace
- *(rama)* clarify transient admission rejection
- *(rama)* document service integration
- *(rama)* use async service fixtures
- *(rama)* format adapter
