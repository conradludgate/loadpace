# Tune queue and controller settings

Use this guide when adapting Loadpace to a real service. Start with the
defaults, measure behavior, and change one control parameter at a time.

## Choose the queue capacity

Set the queue capacity to the amount of scheduling lookahead P2C needs, not to
the amount of overload you want to hide:

```rust
let config = EndpointConfig::default().queue_capacity(4);
```

Small values such as `1..=4` keep queue latency visible and make backpressure
responsive. A larger value gives the balancer more lookahead but also allows
more work to become committed to an endpoint before it is dispatched.

## Set the emergency inflight cap

The inflight cap protects against stuck requests and broken transports:

```rust
let config = EndpointConfig::default().max_inflight(1024);
```

Keep it comfortably above the normal operating point. If it participates in
normal traffic, first check the RTT estimator, rate calculation, and queue
capacity; Loadpace is designed for GCRA to provide normal control.

## Adjust the latency estimator

`LatencyEstimatorConfig` controls the conservative initial RTT, the short and
long EWMAs, and the minimum measurable RTT. Samples should be measured from
actual dispatch to response completion. Do not include time spent waiting in
Loadpace's queue or pacer.

```rust
let config = EndpointConfig {
    latency: LatencyEstimatorConfig {
        initial_rtt: Duration::from_millis(50),
        short_alpha: 0.25,
        long_alpha: 0.05,
        min_rtt: Duration::from_micros(1),
        baseline_window: Duration::from_secs(60),
    },
    ..EndpointConfig::default()
};
```

## Adjust the operating point

`Gradient2Config` keeps the target concurrency fractional. Values such as
`1.3` are intentional and are converted into a rate using Little's Law:

```text
rate = target concurrency / expected RTT
```

Start by changing the initial value and bounds. Change `gain`, `smoothing`, or
`update_interval` only when the controller is clearly adapting too slowly or
too aggressively in a simulator. Healthy operating-point updates are limited
to one per `update_interval`, so the controller does not learn faster merely
because responses arrive faster. The controller deliberately does not increase
from application-limited samples.

## Tune probes only when needed

Probes are temporary controller-owned perturbations: positive probes add a
fixed request rate, while negative probes multiply the base rate by a factor
below one. Expressing positive probes in requests per second gives clients the
same exploration opportunity even when their network RTTs differ. The probes
help independent clients escape an unfair equilibrium without permanently
assigning capacity.

The controller automatically considers the configured schedule as normal
dispatch and load-selection operations refresh endpoint state. The adapters
trigger this through ordinary operations, so application code does not need to
start probes, supply randomness, or run a timer. Start with the default
schedule. If membership churn requires a slower or faster fairness response,
tune it as part of the endpoint configuration:

```rust
let config = EndpointConfig {
    probe_schedule: ProbeSchedule {
        min_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(5),
        ..ProbeSchedule::default()
    },
    ..EndpointConfig::default()
};
```

Use `EndpointController::new_with_seed` for reproducible simulations and
tests. Production controllers use their own entropy source.

## Watch these metrics

For each endpoint, monitor:

- expected, short, long, and baseline RTT;
- target and effective concurrency;
- base and effective rate;
- committed and virtual-tail TAT;
- queued and inflight requests;
- completed requests and failures;
- active probe state.

If queue depth is persistently full, the service is either overloaded or its
controller is too conservative. If queue depth is always empty but latency is
high, investigate the endpoint and transport rather than increasing the local
queue.
