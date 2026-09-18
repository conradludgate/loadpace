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
| `finish(request, completion, now)` | Releases dispatched work using success, failure feedback, or abandonment; derives RTT from the dispatch token |
| `on_complete(request, outcome, latency, now)` | Releases inflight state and updates feedback; returns whether the token belonged to an active request |
| `on_abandoned(request, now)` | Releases and penalizes dispatched work that ended without endpoint feedback, without resetting the silence clock |
| `cancel(reservation, now)` | Removes a queued reservation and rebuilds the virtual tail |
| `load(now)` | Returns predicted completion delay in seconds; lower is better |
| `snapshot(now)` | Returns metrics and current controller state |
| `refresh(now)` | Expires probes and refreshes the derived pacing rate |
| `take_changes()` | Takes and clears accumulated admission and dispatch wakeup effects without advancing policy |

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

## `Completion` and `finish`

For an integration that measures RTT from actual dispatch, call
`finish(request, completion, now)`. The controller derives elapsed time from
the `InFlightRequest`, excluding the reservation's local waiting time.

| Classification | Meaning |
| --- | --- |
| `Completion::Success` | Healthy endpoint feedback; record elapsed RTT and reset feedback silence |
| `Completion::Failure` | Unhealthy endpoint feedback; reduce the operating point and reset feedback silence without an RTT sample |
| `Completion::Abandoned` | No endpoint feedback, such as a local timeout or cancellation; reduce the operating point without resetting silence while other work remains inflight |

The caller must distinguish a local timeout from an unhealthy response.
An error result alone does not establish that the endpoint provided feedback.
The existing `on_complete` and `on_abandoned` methods remain available;
`on_complete` accepts an explicit RTT when it is measured separately.

## `ControllerChanges` and wakeups

`take_changes()` returns two coalesced flags: `admission` means queue space was
released, and `dispatch` means queued requests should recheck FIFO order,
deadlines, or inflight capacity. These are wakeup hints, not readiness grants.
Flags remain set until taken, even if later operations consume released space.
Calling `take_changes()` again without further changes returns both flags unset.

An adapter should register its waiter before checking state, perform controller
operations under its lock, and take the changes before releasing that lock.
It then notifies the indicated waiters after unlocking. This applies to load
and snapshot reads too: they can change pacing through feedback decay or probes.
When selecting between two locked controllers, drain both controllers and
release both locks before notifying either endpoint.

The FIFO head checking its own dispatch state already observes its refreshed
deadline. It can consume those refresh effects without broadcasting a wakeup
to itself; subsequent reservations remain blocked behind it. Broadcasting on
every refresh can create a busy loop when missing-feedback decay changes the
rate continuously. Dispatch, cancellation, and completion must still notify
other affected waiters. The core stores no wakers or runtime-specific state.

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

## `reserve_pair` and `PairChoice`

`reserve_pair(&mut first, &mut second, now)` compares two sampled controllers
at the same instant and reserves the one with the lower predicted completion
cost. Ties prefer the first argument. If the preferred controller rejects
admission, it tries the other; if both reject, it returns the fallback error.
Success returns `(PairChoice, DispatchReservation)`, identifying the controller
that owns the token. Only that controller gains a reservation.

Applications retain ownership of endpoint discovery, random sampling, and
locking. Sample distinct endpoints and acquire locks in a consistent order,
but pass controllers in sampled order so lock order does not bias ties. Use
`reserve` directly when there is only one endpoint. A pair rejection does not
mean every endpoint in the pool is full.

Load comparison can refresh both controllers even on rejection. Drain changes
from both while locked, then unlock both before notifying waiters. The helper
does not drain effects, create a queue, or depend on an RNG or async runtime.
