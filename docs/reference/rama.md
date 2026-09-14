# Rama adapter reference

`loadpace-rama` adapts the runtime-independent controller to Rama 0.4's
`rama::Service` and `rama::Layer` traits.

## `AdaptiveEndpoint<S>`

`AdaptiveEndpoint<S>` implements `rama::Service<Request>` when `S` is a
`rama::Service<Request>` that is `Send + Sync + 'static`. The wrapped service is
stored in an `Arc`, so clones share one controller and can run concurrent inner
service futures.

Unlike Tower, Rama has no `poll_ready` method. Calling `serve` synchronously
reserves a slot in the endpoint's bounded virtual scheduling horizon. The
returned future then waits for its GCRA deadline, dispatches the request, and
records the actual service latency.

## `ServiceError<E>`

The adapter distinguishes admission failures from errors returned by the
wrapped service:

- `ServiceError::Rejected(ScheduleError)` means the request was not dispatched;
- `ServiceError::Inner(E)` means the request reached the inner service.

Dropping an admitted future before dispatch cancels its virtual reservation.
Dropping it after dispatch records a failure outcome. This preserves bounded
admission and makes cancellation visible to the controller.

## `AdaptiveLayer`

`AdaptiveLayer` implements `rama::Layer` and clones its `EndpointConfig` into a
new `AdaptiveEndpoint` for each wrapped service. Use it in a Rama layer stack
when the endpoint boundary is known at composition time.

## Metrics and probes

`load_metric` returns the controller's predicted completion cost in seconds.
`snapshot` returns the current controller state. `start_positive_probe`,
`start_negative_probe`, and `maybe_start_probe` mirror the core probe controls;
the adapter also automatically drives the configured schedule when queued
demand waits behind in-flight work. It wakes waiting dispatches when an active
probe changes, expires, or causes a rate transition.
