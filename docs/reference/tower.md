# Tower adapter reference

This page documents the public API in `loadpace-tower`. The controller behind
the adapter is documented in [the core reference](controller.md).

## Crate setup

```toml
[dependencies]
loadpace = "0.1"
loadpace-tower = "0.1"
tower = { version = "0.5", features = ["balance"] }
```

The adapter targets the Rust 2024 Edition and requires Rust 1.85 or newer.

## `AdaptiveEndpoint<S>`

`AdaptiveEndpoint<S>` implements:

```text
tower::Service<Request, Response = S::Response, Error = S::Error>
tower::load::Load<Metric = LoadMetric>
```

`poll_ready` holds a bounded Tower readiness reservation. `call` creates a
future that waits for GCRA and inner-service readiness. Actual dispatch time
starts immediately before `S::call`, so local queue and readiness delay do not
enter the RTT sample. Dropping the future cancels a queued reservation or
records a dispatched request as failed.

## `LoadMetric`

`LoadMetric` contains the predicted completion delay for one additional
request, in seconds. Lower values are better. It is intended for Tower's
`balance::p2c::Balance`, which samples two ready endpoints and chooses the
lower-load service.

The prediction includes the endpoint's virtual GCRA queue tail and expected
RTT, allowing endpoints with different learned rates and latencies to be
compared without a global sort.

## `AdaptiveDiscovery<D, Request>`

The wrapper maps discovery insertions:

```text
Change::Insert(key, service)
    → Change::Insert(key, AdaptiveEndpoint::new(service, config.clone()))
```

Removal events pass through. Controller state is fresh after re-insertion of a
key; state retention across discovery churn is not currently implemented.
