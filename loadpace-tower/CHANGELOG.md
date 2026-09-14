# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-rc.2](https://github.com/conradludgate/loadpace/compare/loadpace-tower-v0.1.0-rc.1...loadpace-tower-v0.1.0-rc.2) - 2026-09-14

### Added

- *(probe)* schedule probes by time

### Fixed

- *(tower)* enable tokio select macro
- *(tower)* use runtime clock for dispatch deadlines
- *(probe)* wake Tower dispatches on transitions
- *(tower)* preserve endpoint thread safety
- *(tower)* replace shared readiness waker

### Other

- *(tower)* use timeout for dispatch wakeups
- *(tower)* trim runtime tokio features
- forbid unsafe code
- *(tower)* format probe closure
- *(tower)* arm probe wakeup timers
- *(tower)* avoid exact timer boundaries
- *(tower)* format controller helper
- *(tower)* centralize probe transition wakeups
- *(tower)* remove unsafe future projection
- *(tower)* narrow controller access
