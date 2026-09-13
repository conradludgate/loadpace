# Loadpace

Runtime-independent adaptive client-side load balancing and backpressure.

The core `loadpace` crate contains the controller and simulator. Framework
adapters are separate crates, starting with [`loadpace-tower`](https://crates.io/crates/loadpace-tower).

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
- [Run a deterministic simulation](docs/how-to/run-simulator.md)
- [Tune queue and controller settings](docs/how-to/tune-an-endpoint.md)

### Reference

Look up the public types, defaults, and state transitions:

- [Controller and configuration reference](docs/reference/controller.md)
- [Tower adapter reference](docs/reference/tower.md)
- [Rust API documentation](https://docs.rs/loadpace)
- [Tower API documentation](https://docs.rs/loadpace-tower)

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

## What the crate provides

The `loadpace` crate's `EndpointController` is the runtime-independent core.
It combines:

- a smoothed endpoint RTT estimate;
- a fractional Gradient2 operating point;
- Little's Law to derive a request rate;
- GCRA pacing and virtual queue prediction;
- temporary additive positive and multiplicative negative probes;
- explicit failure and cancellation handling;
- a bounded scheduling queue and emergency inflight cap.

The separate `loadpace-tower` crate provides:

- `AdaptiveEndpoint<S>: tower::Service<Request>`;
- a predicted completion-cost `tower::load::Load` metric;
- `AdaptiveDiscovery`, which wraps inserted services with fresh controller state;
- compatibility with Tower's `p2c::Balance`.

The public `simulate` function in `loadpace` provides a deterministic
fixed-service-time simulator for comparing controller changes.

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
- deterministic simulation and unequal endpoint latency.

Run the full suite with:

```text
cargo test --workspace --all-features --all-targets
```

## Project status

The first implementation covers the deterministic controller, simulator, and
Tower adapter with dynamic discovery and P2C integration. Hyper and Rama
adapters are intentionally separate future crates. Automatic probe scheduling,
failure classification beyond adapter-level errors, richer transport-readiness
prediction, and production tuning remain active design areas. See [How Loadpace
controls and routes work](docs/explanation/design.md) for the current
boundaries and open questions.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
