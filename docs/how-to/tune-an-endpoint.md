# Tune an endpoint

Use this guide when adapting Loadpace to a real service. Configure workload
assumptions first, measure the result, and change one setting at a time.

## Estimate startup RTT and concurrency

Every endpoint configuration requires two deployment-specific inputs:

```rust
use std::time::Duration;
use loadpace::EndpointConfig;

let config = EndpointConfig::new(
    Duration::from_millis(20), // expected dispatch-to-response RTT
    32,                        // expected sustainable concurrency
);
```

Use an ordinary healthy RTT, including network and service time but excluding
Loadpace admission and pacing delay. Set initial concurrency near the traffic
level one endpoint is expected to sustain, not necessarily to `1`. Loadpace
uses Little's Law to derive the initial rate:

```text
initial rate = initial concurrency / expected RTT
```

In this example, `32 / 0.020` gives 1,600 requests per second. If responses do
not arrive, Loadpace allows one expected RTT of grace and then halves this rate
for every additional expected RTT without feedback.

## Set the queue-delay tolerance

The tolerated queueing delay defaults to half the expected RTT. Override it
when the service has a known latency budget:

```rust
let config = EndpointConfig::new(Duration::from_millis(20), 32)
    .with_queue_tolerance(Duration::from_millis(5));
```

This is an absolute duration added to the learned minimum RTT. A lower value
reacts sooner to queueing; a higher value permits more latency before reducing
the operating point.

## Choose the scheduling horizon

Set queue capacity to the amount of scheduling lookahead P2C needs, not to the
amount of overload you want to hide:

```rust
let config = EndpointConfig::new(Duration::from_millis(20), 32)
    .with_queue_capacity(4);
```

The default is four accepted-but-not-yet-dispatched requests per endpoint.
Small values keep queue latency visible and make backpressure responsive. A
larger value gives the balancer more lookahead but commits more local work
before dispatch.

## Set the emergency inflight cap

The inflight cap protects against stuck requests and broken transports:

```rust
let config = EndpointConfig::new(Duration::from_millis(20), 32)
    .with_max_inflight(1024);
```

By default it is the larger of 1,024 and four times initial concurrency. Keep
it comfortably above the normal operating point. If it participates in normal
traffic, first inspect RTT estimation and pacing; GCRA is the normal actuator.

## Understand derived policy

Loadpace deliberately does not expose Gradient2, EWMA, or probe parameters
through `EndpointConfig`. They are implementation details derived from the
workload assumptions:

- short and long RTT EWMA weights target half-lives of roughly 2 and 20
  expected RTTs at the initial concurrency;
- the healthy controller update interval is 2 expected RTTs;
- the minimum-RTT baseline covers at least 60 seconds and 1,200 expected RTTs;
- randomized fairness probes use the library's tested policy.

This keeps normal configuration stable if the internal controller changes.
Use `EndpointController::new_with_seed` when simulations need reproducible
probe decisions.

## Watch these metrics

For each endpoint, monitor:

- expected, short, long, and baseline RTT;
- target and effective concurrency;
- base, probed, and effective rate;
- feedback silence and its decay factor;
- committed and virtual-tail TAT;
- queued and inflight requests;
- completed requests and failures;
- active probe state.

If queue depth is persistently full, the service is overloaded or its
controller assumptions are too conservative. If queue depth is always empty
but latency is high, investigate the endpoint and transport rather than
increasing the local queue.
