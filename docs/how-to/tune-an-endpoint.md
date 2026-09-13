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

Start by changing the initial value and bounds. Change `gain` or `smoothing`
only when the controller is clearly adapting too slowly or too aggressively in
a simulator. The controller deliberately does not increase from
application-limited samples.

## Use probes carefully

Positive probes add a fixed amount to the base operating point. Negative
probes multiply it by a factor below one. A caller supplies the RNG so tests
can use a deterministic seed:

```rust
let mut rng = rand::rngs::StdRng::seed_from_u64(7);
controller.maybe_start_probe(&ProbeSchedule::default(), &mut rng, now);
```

Treat probing as a temporary perturbation. It is useful for escaping unfair
multi-client equilibria, but it should not be used as a permanent capacity
assignment mechanism.

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
