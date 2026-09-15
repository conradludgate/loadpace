# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- *(rama)* add adaptive DNS discovery with atomic P2C admission

### Changed

- *(rama)* remove the redundant inner-service `Arc`

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
