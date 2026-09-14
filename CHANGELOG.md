# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-rc.2](https://github.com/conradludgate/loadpace/compare/loadpace-v0.1.0-rc.1...loadpace-v0.1.0-rc.2) - 2026-09-14

### Added

- *(probe)* schedule probes by time

### Fixed

- *(probe)* shorten default fairness cadence
- *(probe)* wake Tower dispatches on transitions
- *(latency)* learn baseline from the first sample
- *(controller)* restore fair rebalancing after churn
- *(controller)* gate growth on paced samples
- *(gcra)* preserve pacing phase across rate changes
- *(gradient)* use short and long RTT feedback
- *(config)* reject invalid controller parameters
- *(probe)* validate probe schedules
- *(gcra)* validate representable pacing rates
- *(controller)* reject foreign completion tokens
- *(tower)* replace shared readiness waker

### Other

- forbid unsafe code
- *(fairness)* allow server churn to settle
- *(latency)* cover cold-start baseline fallback
- *(fairness)* require rebalancing after joins
- describe saturation and probe timing
- *(fairness)* drive seeded probes in churn scenarios
- *(simulator)* model worker pool saturation
- *(simulator)* add fairness and churn scenarios
- *(tower)* narrow controller access
- *(gcra)* separate pacing from reservations
- Configure release-plz publishing
