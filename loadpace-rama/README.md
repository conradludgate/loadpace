# loadpace-rama

Adaptive Rama services for clients balancing requests across a changing
fleet.

## Why this adapter exists

A Rama service call can be accepted locally even when the selected endpoint is
already saturated. Static limits do not account for different endpoint
capacities or changing workload conditions, while an unbounded queue hides
backpressure from the caller.

`loadpace-rama` wraps each service with the [`loadpace`](https://crates.io/crates/loadpace)
controller. It keeps admission bounded, paces dispatch using the endpoint's
learned operating point, and exposes predicted completion cost for a higher-
level balancer. This gives Rama applications the same adaptive control as the
Tower integration while fitting Rama's direct `serve` model.

## Usage

```toml
[dependencies]
loadpace = "0.1"
loadpace-rama = { version = "0.1", features = ["dns"] }
rama = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Wrap a Rama [`Service`](https://docs.rs/rama/0.4/rama/trait.Service.html)
with `AdaptiveEndpoint`:

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

Rama has no `poll_ready` reservation phase. `AdaptiveEndpoint::serve` admits
the request synchronously into a bounded virtual scheduling horizon. If that
horizon is full, the future resolves to `ServiceError::Rejected` without
calling the inner service. If an admitted future is dropped before dispatch,
its reservation is cancelled; cancellation after dispatch is recorded as a
failure sample.

Use `AdaptiveLayer` when composing Rama layers:

```rust
use loadpace::EndpointConfig;
use loadpace_rama::AdaptiveLayer;

let endpoint = AdaptiveLayer::new(EndpointConfig::default()).layer(inner);
```

## Adaptive DNS balancing

Enable the `dns` feature to put adaptive endpoint discovery directly in front
of a Rama connector:

```rust,no_run
use loadpace_rama::dns::{AdaptiveDnsConfig, AdaptiveDnsLayer};
use rama::Layer;

# let connector = rama::service::service_fn(|request: rama::net::client::ConnectRequest| async move {
#     Ok::<_, std::convert::Infallible>(request)
# });
let connector = AdaptiveDnsLayer::new(AdaptiveDnsConfig::new()).layer(connector);
```

The layer belongs immediately outside the connector (or connector stack) that
consumes Rama's `ConnectorTarget`. For a hostname request it resolves and
caches the address set, samples two distinct endpoints, and compares their
current Loadpace load. It reserves admission on the preferred endpoint while
both sampled controller locks are still held; if that endpoint rejects, it
tries the alternate before releasing the locks. Selection and admission are
therefore one atomic P2C decision rather than a DNS picker followed by a
separate reservation race.

The selected concrete IP is written only to Rama's connector-target extension.
The request's original authority remains unchanged, preserving TLS SNI and HTTP
host behavior.

Adaptive state is identified by `(hostname, port, IP)`. Cached results refresh
in the background after 30 seconds, retain surviving endpoint controllers, and
discard removed addresses from new selections. Entries idle for 300 seconds or
without a successful refresh for 300 seconds are evicted; the cache is bounded
to 1024 host/port entries by default. These policies and the per-endpoint
`EndpointConfig` are configurable through `AdaptiveDnsConfig`.

This crate is experimental. See the [Loadpace repository](https://github.com/conradludgate/loadpace)
for the design and current documentation.
