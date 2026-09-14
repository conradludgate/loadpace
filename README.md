# Loadpace

Adaptive client-side load balancing for changing service fleets, with bounded
backpressure.

## Why Loadpace exists

A conventional load balancer answers “which endpoint should receive this
request?” A production client also needs to answer “should I accept this
request yet?” That second question becomes difficult when client instances are
scaled horizontally, endpoints have different capacities and latencies, and
discovery adds or removes servers over time.

Static rate or concurrency limits cannot adapt to those conditions: a limit
that protects a small or slow endpoint can leave a larger endpoint idle, while
a limit chosen for the larger endpoint can overwhelm the smaller one. An
unbounded client queue hides the overload instead of applying backpressure.

Loadpace gives every endpoint its own adaptive feedback loop. It learns a safe
operating point from latency and outcomes, turns that point into a smooth
request schedule, predicts the completion cost of newly queued work, and stops
accepting work when its small local scheduling horizon is full. The result is
an adaptive client-side balancer that can use changing capacity without
flooding endpoints or hiding overload behind a queue.

The core `loadpace` crate contains the controller and simulator. Framework
adapters are separate crates, starting with [`loadpace-tower`](https://crates.io/crates/loadpace-tower)
and [`loadpace-rama`](https://crates.io/crates/loadpace-rama).

Loadpace treats client-side load balancing as a control problem:

- learn how much load each endpoint can safely sustain;
- turn that operating point into a smooth request rate;
- route work using predicted completion cost;
- stop accepting work when the local scheduling horizon is full.

> **Experimental:** the API and controller constants are still expected to
> evolve as the simulator and real workloads teach us more.

## Start here

The documentation is organised using [Diátaxis](https://diataxis.fr/): each
page has one primary purpose.

### Tutorial

Learn by building a small paced client:

- [Build a paced endpoint client](docs/tutorial.md)

### How-to guides

Use these when you already know what you want to accomplish:

- [Integrate dynamic discovery with Tower P2C](docs/how-to/integrate-with-tower.md)
- [Integrate a Rama service](docs/how-to/integrate-with-rama.md)
- [Run a deterministic simulation](docs/how-to/run-simulator.md)
- [Tune queue and controller settings](docs/how-to/tune-an-endpoint.md)

### Reference

Look up the public types, defaults, and state transitions:

- [Controller and configuration reference](docs/reference/controller.md)
- [Tower adapter reference](docs/reference/tower.md)
- [Rama adapter reference](docs/reference/rama.md)
- [Rust API documentation](https://docs.rs/loadpace)
- [Tower API documentation](https://docs.rs/loadpace-tower)
- [Rama API documentation](https://docs.rs/loadpace-rama)

### Explanation

Understand the design and the reasoning behind it:

- [How Loadpace controls and routes work](docs/explanation/design.md)

## Install

Loadpace targets the Rust 2024 Edition and requires Rust 1.85 or newer.

The core controller and simulator have no async-runtime or framework
dependency:

```toml
[dependencies]
loadpace = "0.1"
```

For Tower integration, add the adapter and Tower itself:

```toml
[dependencies]
loadpace = "0.1"
loadpace-tower = "0.1"
tower = { version = "0.5", features = ["util"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For Rama integration, add the adapter and Rama itself:

```toml
[dependencies]
loadpace = "0.1"
loadpace-rama = "0.1"
rama = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

## Minimal Tower example

Wrap any suitable Tower service in an adaptive endpoint from `loadpace-tower`:

```rust
use std::convert::Infallible;
use loadpace::EndpointConfig;
use loadpace_tower::AdaptiveEndpoint;
use tower::{service_fn, ServiceExt};

#[tokio::main]
async fn main() -> Result<(), Infallible> {
    let endpoint = AdaptiveEndpoint::new(
        service_fn(|request: u64| async move { Ok::<_, Infallible>(request * 2) }),
        EndpointConfig::default(),
    );

    let response = endpoint.oneshot(21).await?;
    assert_eq!(response, 42);
    Ok(())
}
```

For dynamic endpoints, use `loadpace_tower::AdaptiveDiscovery` and Tower's existing
`tower::balance::p2c::Balance`; see the [Tower integration guide](docs/how-to/integrate-with-tower.md).

## Minimal Rama example

Rama services use `serve` directly, so the adapter reserves a bounded
scheduling slot when the call is made:

```rust
use std::convert::Infallible;
use loadpace::EndpointConfig;
use loadpace_rama::AdaptiveEndpoint;
use rama::Service;

#[derive(Clone)]
struct Double;

impl Service<u64> for Double {
    type Output = u64;
    type Error = Infallible;

    async fn serve(&self, request: u64) -> Result<Self::Output, Self::Error> {
        Ok(request * 2)
    }
}

#[tokio::main]
async fn main() -> Result<(), loadpace_rama::ServiceError<Infallible>> {
    let endpoint = AdaptiveEndpoint::new(Double, EndpointConfig::default());
    assert_eq!(endpoint.serve(21).await?, 42);
    Ok(())
}
```

For Rama's direct service model, a full scheduling horizon returns
`ServiceError::Rejected` from the returned future. See the [Rama integration
guide](docs/how-to/integrate-with-rama.md) for handling rejection and
wrapping a service with `AdaptiveLayer`.

## What the crate provides

The `loadpace` crate's `EndpointController` is the runtime-independent core.
It combines:

- a smoothed endpoint RTT estimate;
- a fractional Gradient2 operating point;
- Little's Law to derive a request rate;
- GCRA pacing and virtual queue prediction;
- controller-driven temporary additive and multiplicative probes;
- explicit failure and cancellation handling;
- a bounded scheduling queue and emergency inflight cap.

The separate `loadpace-tower` crate provides:

- `AdaptiveEndpoint<S>: tower::Service<Request>`;
- `AdaptiveLayer`, for composing an endpoint in a Tower layer stack;
- a predicted completion-cost `tower::load::Load` metric;
- `AdaptiveDiscovery`, which wraps inserted services with fresh controller state;
- compatibility with Tower's `p2c::Balance`;
- demand-driven automatic probing while an endpoint has queued work.

The separate `loadpace-rama` crate provides:

- `AdaptiveEndpoint<S>: rama::Service<Request>`;
- `AdaptiveLayer`, for wrapping services in Rama layer stacks;
- the same predicted completion-cost metric and controller inspection helpers;
- bounded admission errors through `ServiceError::Rejected`;
- demand-driven automatic probing while an endpoint has queued work.

The public `simulate` function in `loadpace` provides a deterministic
worker-pool simulator for comparing controller changes and endpoint
saturation.

## Backpressure guarantee

The controller accepts work only while its configured scheduling horizon has room.
Requests waiting for a GCRA slot have not been sent to the server. When every
endpoint exposed by an adapter is full, that adapter can keep readiness
pending and preserve upstream backpressure.

The inflight cap is intentionally a generous emergency safety valve. Normal
control comes from GCRA pacing, not from rounding the fractional operating
point into an integer semaphore.

## Testing

The repository includes tests for:

- GCRA spacing, debt, and cancellation;
- RTT smoothing and fractional Gradient2 behavior;
- positive and negative probing;
- bounded queue admission and response cancellation;
- concurrent inner responses;
- dynamic discovery and Tower P2C integration;
- deterministic simulation, worker-pool saturation, and unequal endpoint latency;
- seeded multi-client fairness and server-capacity churn scenarios.

Run the full suite with:

```text
cargo test --workspace --all-features --all-targets
```

## Project status

The first implementation covers the deterministic controller, simulator, and
Tower and Rama adapters. Hyper remains a separate future crate. Failure
classification beyond adapter-level errors, richer transport-readiness
prediction, RTT baseline aging, and production tuning remain active design areas.
See [How Loadpace controls and routes work](docs/explanation/design.md) for the current
boundaries and open questions.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
