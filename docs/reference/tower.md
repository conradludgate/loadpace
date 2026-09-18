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
tower::Service<Request, Response = S::Response, Error = tower::BoxError>
tower::load::Load<Metric = LoadMetric>
```

`poll_ready` reports bounded admission readiness. `call` creates a future that
waits for GCRA and inner-service readiness. Actual dispatch time starts
immediately before `S::call`, so local queue and readiness delay do not enter
the RTT sample. Dropping the future cancels a queued reservation or records a
dispatched request as failed.

The endpoint is not `Clone`. Tower's P2C balancer owns one mutable service per
discovered backend and does not require its endpoint services to be cloneable.
Keeping that ownership explicit also lets the adapter coordinate its single
readiness waiter directly with controller state, without a shared admission
semaphore.

`AdaptiveLayer` wraps a service in an endpoint and can be used directly in a
`tower::ServiceBuilder` stack. The inner service needs to be `Send`, but does
not need to be `Sync`; its response type does not need to be `Send`. Its error
type must implement `Into<tower::BoxError>`, as with Tower's `Buffer`.

An inner `poll_ready` error permanently closes the endpoint. Queued requests
and subsequent outer readiness checks return a boxed `loadpace_tower::ServiceError`
sharing the original cause through `std::error::Error::source`. The failed
inner service is never polled again, and Tower's balancer can remove the
endpoint. A `call` made using a readiness grant obtained before the failure
also returns that shared error without reserving another queue slot.

Already dispatched requests finish normally. Errors from their response
futures are converted directly into `tower::BoxError` and penalize the
controller without closing admission.

`snapshot` returns controller metrics. Probes are controller-owned and are
automatically considered as the adapter refreshes endpoint state during normal
dispatch and load selection. Application code does not need a timer or a probe
task. Probe changes wake queued dispatches immediately; dispatch waits also wake
at the next scheduled decision and when an active probe expires so the base
pacing rate is recomputed.

## `LoadMetric`

`LoadMetric` contains the predicted completion delay for one additional
request, in seconds. Lower values are better. It is intended for Tower's
`balance::p2c::Balance`, which samples two ready endpoints and chooses the
lower-load service.

The prediction includes the endpoint's virtual GCRA queue tail and expected
RTT, allowing endpoints with different learned rates and latencies to be
compared without a global sort.

## `AdaptiveDiscovery<D>`

The wrapper maps discovery insertions:

```text
Change::Insert(key, service)
    → Change::Insert(key, AdaptiveEndpoint::new(service, config.clone()))
```

Removal events pass through. Controller state is fresh after re-insertion of a
key; state retention across discovery churn is not currently implemented. The
stream mapping itself does not constrain the inserted service or request type;
those bounds apply only when the wrapped endpoint is used as a service.
