# Integrate a Rama service

Use `loadpace-rama` when the endpoint you want to pace implements Rama's
`rama::Service` trait.

## Add the dependencies

```toml
[dependencies]
loadpace = "0.1"
loadpace-rama = "0.1"
rama = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

## Wrap the service

Rama services implement `serve(&self, request)` and return a future. The
adapter shares the service across clones and runs concurrent requests when the
inner service supports concurrent calls.

```rust
use std::convert::Infallible;
use loadpace::{EndpointConfig, ScheduleError};
use loadpace_rama::{AdaptiveEndpoint, ServiceError};
use rama::Service;

#[derive(Clone)]
struct Backend;

impl Service<u64> for Backend {
    type Output = u64;
    type Error = Infallible;

    async fn serve(&self, request: u64) -> Result<Self::Output, Self::Error> {
        Ok(request)
    }
}

async fn send(endpoint: &AdaptiveEndpoint<Backend>, request: u64) {
    match endpoint.serve(request).await {
        Ok(response) => println!("{response}"),
        Err(ServiceError::Rejected(ScheduleError::QueueFull)) => {
            // Apply application-specific backoff or shed the request.
        }
        Err(ServiceError::Rejected(error)) => {
            eprintln!("request was not admitted: {error:?}");
        }
        Err(ServiceError::Inner(error)) => match error {},
    }
}
```

Admission is synchronous because Rama does not have Tower's `poll_ready`
contract. Calling `serve` reserves one slot immediately; if the configured
virtual horizon is full, the returned future resolves to
`ServiceError::Rejected` and the backend is not called. Dropping an admitted
future releases its reservation. Dropping it after dispatch records a failure,
which keeps cancellation visible to the controller.

## Use a layer

When the service is assembled through Rama layers, use `AdaptiveLayer`:

```rust
use loadpace::EndpointConfig;
use loadpace_rama::AdaptiveLayer;
use rama::Layer;

let paced = AdaptiveLayer::new(EndpointConfig::default()).layer(backend);
```

The layer creates independent controller state for each inner service it wraps.
Clone the resulting `AdaptiveEndpoint` to share one endpoint's controller and
virtual queue across callers.

## Inspect load and probe state

`AdaptiveEndpoint::load_metric` returns the predicted completion cost in
seconds, which can be used by a higher-level Rama balancer. `snapshot` exposes
the RTT, concurrency, queue, inflight, completion, and probe state for
metrics. With the default `EndpointConfig`, the controller automatically
advances the probe schedule during normal dispatch and load-selection
operations. No timer, background task, or application probe callback is
required.

See the [controller reference](../reference/controller.md) for the shared
semantics and defaults.
