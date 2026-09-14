# Build a paced endpoint client

In this tutorial we will wrap a small Tower service in Loadpace and send a
request through it. At the end, the service will have an adaptive controller
that paces future requests and records endpoint observations.

## Create a project

Create a binary crate and add these dependencies:

```text
cargo new paced-client
cd paced-client
cargo add loadpace
cargo add loadpace-tower
cargo add tower --features util
cargo add tokio --features macros,rt-multi-thread
```

The `loadpace-tower` crate provides `AdaptiveEndpoint`. Tower's `util` feature
gives us `service_fn` and `ServiceExt` for this tutorial.

## Wrap a service

Replace `src/main.rs` with:

```rust
use std::convert::Infallible;

use loadpace::EndpointConfig;
use loadpace_tower::AdaptiveEndpoint;
use tower::{service_fn, Service, ServiceExt};

#[tokio::main]
async fn main() -> Result<(), Infallible> {
    let endpoint = AdaptiveEndpoint::new(
        service_fn(|request: u64| async move {
            Ok::<_, Infallible>(request * 2)
        }),
        EndpointConfig::default(),
    );

    let response = endpoint.oneshot(21).await?;
    println!("response: {response}");

    Ok(())
}
```

Run it:

```text
cargo run
```

You should see:

```text
response: 42
```

The first request can dispatch immediately. The endpoint starts with a
conservative RTT and operating-point estimate; after responses arrive, the
controller updates its rate and future requests are paced by GCRA.

## Inspect the controller

Keep the endpoint in a variable rather than consuming it if you want to read
its metrics:

```rust
let mut endpoint = AdaptiveEndpoint::new(service, EndpointConfig::default());
let response = endpoint.ready().await?.call(request).await?;
let snapshot = endpoint.snapshot();

println!("response: {response:?}");
println!("expected RTT: {:?}", snapshot.expected_rtt);
println!("target concurrency: {}", snapshot.target_concurrency);
println!("effective rate: {} req/s", snapshot.effective_rate);
```

The snapshot separates queued work from actual inflight work. Queue delay is
not included in the RTT sample.

You have now built the smallest useful Loadpace client. To connect several
discovered endpoints, continue with [the Tower integration guide](how-to/integrate-with-tower.md).
