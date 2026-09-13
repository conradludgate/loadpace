# Controller and configuration reference

This is the lookup-oriented reference for the current public controller API.
For reasons behind the design, see [the explanation](../explanation/design.md).

The crate targets the Rust 2024 Edition and requires Rust 1.85 or newer. It
has no async-runtime or framework dependency. Framework adapters are published
separately; see the [Tower adapter reference](tower.md).

## `EndpointConfig`

| Field | Default | Meaning |
| --- | ---: | --- |
| `queue_capacity` | `4` | Accepted but not-yet-dispatched requests per endpoint |
| `max_inflight` | `1024` | Emergency cap on actually dispatched requests |
| `latency` | `LatencyEstimatorConfig::default()` | RTT estimator parameters |
| `gradient` | `Gradient2Config::default()` | Fractional Gradient2 operating-point parameters |

The builder-style methods `.queue_capacity(value)` and
`.max_inflight(value)` cover the two most common settings.

## `EndpointController`

| Method | Effect |
| --- | --- |
| `new(config, now)` | Creates fresh endpoint state and an immediately available first pacing slot |
| `reserve(now)` | Appends a bounded virtual queue reservation |
| `dispatch_state(reservation, now)` | Reports `Ready`, a pacing deadline, FIFO wait, inflight limit, or cancellation |
| `on_dispatched(reservation, now)` | Commits a reservation after the transport is ready |
| `on_complete(request, outcome, latency, now)` | Releases inflight state and updates feedback; returns whether the token belonged to an active request |
| `cancel(reservation, now)` | Removes a queued reservation and rebuilds the virtual tail |
| `load(now)` | Returns predicted completion delay in seconds; lower is better |
| `snapshot(now)` | Returns metrics and current controller state |
| `refresh(now)` | Expires probes and refreshes the derived pacing rate |

Reservations must be dispatched FIFO within one endpoint. Framework adapters
should enforce this by making later response futures wait for earlier
reservations.

The endpoint controller uses the latency estimator's minimum observed RTT as
the Gradient2 reference. This keeps shared queueing visible to incumbents and
newly joined clients alike; the long RTT EWMA remains available in snapshots
and for direct `Gradient2` callers.

## `DispatchState`

| Variant | Meaning |
| --- | --- |
| `Ready` | The reservation is the head of the queue, its GCRA slot is due, and inflight safety capacity exists |
| `WaitUntil(instant)` | GCRA has not reached the reservation's slot |
| `WaitForPrevious` | An earlier reservation must dispatch or be cancelled first |
| `InflightLimit` | The emergency inflight cap is full |
| `Cancelled` | The reservation no longer exists |

## `Outcome`

`Outcome::Success` updates RTT and Gradient2 feedback. `Outcome::Failure`
reduces the operating point without treating a fast failure as a healthy RTT
sample.

## `ControllerSnapshot`

The snapshot includes RTT estimates, target/effective concurrency, derived
rates, committed and virtual TAT, queue and inflight depth, completion/failure
counters, sample count, and active probe state.

## `ProbeSchedule`

`ProbeSchedule` describes caller-driven randomized probes. The positive and
negative probabilities choose the probe kind at each scheduled decision; the
remaining probability performs no probe. `min_interval` and `max_interval`
bound the time until the next decision, so checking the schedule more often
does not increase probe frequency. The default intervals are one to five
seconds, and the default duration is one second.
