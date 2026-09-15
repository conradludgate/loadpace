# Controller and configuration reference

This is the lookup-oriented reference for the current public controller API.
For reasons behind the design, see [the explanation](../explanation/design.md).

The crate targets the Rust 2024 Edition and requires Rust 1.85 or newer. It
has no async-runtime or framework dependency. Framework adapters are published
separately; see the [Tower adapter reference](tower.md).

## Deployment contract

The controller assumes that clients sharing an endpoint are trusted and run
compatible congestion-control behavior. It provides cooperative pacing and
backpressure, not protection against clients that ignore the algorithm. It is
not a server-enforced public API rate limit, quota, or abuse-prevention
boundary.

## `EndpointConfig`

`EndpointConfig::new(expected_rtt, initial_concurrency)` requires the two
workload assumptions needed to establish an initial rate. There is no
`Default` implementation: endpoint latency and capacity are deployment facts,
and silently assuming a concurrency of one is too restrictive for many
services.

| Setting | Initial value | Builder | Getter |
| --- | ---: | --- | --- |
| Expected RTT | required | constructor only | `expected_rtt()` |
| Initial concurrency | required | constructor only | `initial_concurrency()` |
| Tolerated queueing delay | half the expected RTT | `with_queue_tolerance(value)` | `queue_tolerance()` |
| Scheduling horizon | `4` | `with_queue_capacity(value)` | `queue_capacity()` |
| Emergency inflight cap | `max(1024, 4 × initial concurrency)` | `with_max_inflight(value)` | `max_inflight()` |

The endpoint config intentionally contains no Gradient2, EWMA, or probe
types. Those parameters are derived internally from workload assumptions so a
future controller implementation can preserve this API.

## `EndpointController`

| Method | Effect |
| --- | --- |
| `new(config, now)` | Creates fresh endpoint state and an immediately available first pacing slot |
| `new_with_seed(config, now, seed)` | Creates fresh state with deterministic probe entropy for simulations and tests |
| `reserve(now)` | Appends a bounded virtual queue reservation |
| `dispatch_state(reservation, now)` | Refreshes time-driven policy, then reports `Ready`, a recheck deadline, FIFO wait, inflight limit, or cancellation |
| `on_dispatched(reservation, now)` | Atomically commits a ready reservation, or returns its current `DispatchState` |
| `on_complete(request, outcome, latency, now)` | Releases inflight state and updates feedback; returns whether the token belonged to an active request |
| `on_abandoned(request, now)` | Releases and penalizes dispatched work that ended without endpoint feedback, without resetting the silence clock |
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
and for direct `Gradient2` callers. Before the first successful sample, the
configured initial RTT is used as the temporary baseline; the first sample
then establishes the observed minimum even when it is slower than that initial
estimate. The minimum is maintained over a bounded eight-bucket
`LatencyEstimatorConfig::baseline_window`, allowing stale topology or routing
observations to age out without allocating per-request history.

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
sample. Both represent actual endpoint feedback. Use `on_abandoned` for a
client-side timeout or cancellation where no response was received.

Healthy Gradient2 updates are time-gated by `Gradient2Config::update_interval`.
RTT samples continue to update the latency estimator, but a higher response
rate cannot cause proportionally faster operating-point growth.

## `ControllerSnapshot`

The snapshot includes RTT estimates, target/effective concurrency, base,
probed and effective rates, feedback silence and decay factor, committed and
virtual TAT, queue and inflight depth, completion/failure counters, sample
count, and active probe state.

While requests are inflight, the controller measures time since the most
recent real endpoint feedback. It allows one expected RTT of grace, then
halves the paced rate for every additional expected RTT of silence. This
continues dispatching at a progressively lower rate instead of hard-pausing.
Any real completion resets the decay epoch; abandoning a request does not
pretend feedback arrived while other requests remain inflight.

## `ProbeSchedule`

`ProbeSchedule` describes randomized probes. The positive and negative
probabilities choose the probe kind at each scheduled decision; the remaining
probability performs no probe. `min_interval` and `max_interval` bound the time
until the next decision, so request rate does not affect probe frequency. The
default intervals are one to five seconds, and the default duration is one
second. With the default 10% positive and 5% negative probabilities, a probe
opportunity occurs every twenty seconds on average.

The controller owns the random source and consults the schedule as normal
dispatch and load-selection operations refresh endpoint state. This lets an
endpoint that has become less attractive still receive an opportunity to
recover; applications do not need a timer, background task, or probe callback.
Use `new_with_seed` when a deterministic simulation or test needs reproducible
probe decisions.
