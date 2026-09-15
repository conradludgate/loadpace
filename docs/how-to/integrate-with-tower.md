# Integrate dynamic discovery with Tower P2C

Use this guide when you already have a Tower discovery stream and want
`loadpace-tower` to adapt each discovered endpoint before Tower balances
requests.

## Add the adapter

Add the core crate, Tower adapter, and the Tower features used by your client:

```toml
[dependencies]
loadpace = "0.1"
loadpace-tower = "0.1"
tower = { version = "0.5", features = ["balance"] }
```

## Wrap the discovery stream

Your discovery source should yield Tower changes:

```rust
tower::discover::Change<Key, Service>
```

Insertions create a new endpoint controller. Removals are passed through
unchanged.

```rust
use loadpace::EndpointConfig;
use loadpace_tower::AdaptiveDiscovery;
use std::time::Duration;
use tower::balance::p2c::Balance;

let config = EndpointConfig::new(Duration::from_millis(20), 32);
let adaptive = AdaptiveDiscovery::new(discovery, config);
let client = Balance::new(adaptive);
```

The discovered service and its future must satisfy the `Send + 'static`
bounds required when the resulting `AdaptiveEndpoint` is used as a service.

## Send requests through the balancer

Use Tower's normal readiness contract:

```rust
use tower::ServiceExt;

let response = client.oneshot(request).await?;
```

If the discovery source is empty, or every endpoint's local scheduling
horizon is full, readiness remains pending. Do not add an unbounded buffer in
front of the balancer; doing so would hide the backpressure Loadpace is meant
to preserve.

## Understand endpoint selection

Tower's P2C balancer samples two ready endpoints and selects the one with the
lower `Loadpace` metric. The metric predicts the completion time of one more
request using:

```text
virtual GCRA queue tail + expected endpoint RTT
```

This allows endpoints with different RTTs and learned rates to be compared
without a global sort.

## Handle errors

The adapter classifies an `Ok` response as healthy and an inner-service error
as a failure. Failures reduce that endpoint's operating point, so an endpoint
that fails quickly does not look healthy merely because its latency is low.

Retries should be sent through the same Loadpace service. A retry path that
bypasses readiness and pacing can recreate the overload that the original
request was intended to avoid.

For protocol-specific response classification, drive `EndpointController`
directly and pass `Outcome::Success` or `Outcome::Failure` according to your
application's semantics.
