# Tokio adapter reference

`loadpace-tokio` provides owned guards for ordinary async operations. It requires
Rust 1.85 or newer. It depends on `loadpace` and Tokio's synchronization and timer
facilities; the core crate remains runtime-independent.

## `Endpoint`

Clones share one controller. Retain one endpoint per destination and replace it
when that destination's identity changes. Endpoint discovery, sampling, and
transport ownership belong to the application.

| Method | Effect |
| --- | --- |
| `new(config)` | Creates a controller using Tokio's current clock |
| `new_with_seed(config, seed)` | Uses reproducible probe entropy for tests |
| `try_reserve()` | Immediately reserves one bounded queue slot or returns `ScheduleError` |
| `try_reserve_pair(&other)` | Compares and reserves two supplied candidates atomically; returns `(PairChoice, Reservation)` |
| `ready().await` | Waits for queue capacity without claiming it |
| `load()` | Returns predicted completion cost in seconds and delivers refresh wakeups |
| `snapshot()` | Returns controller diagnostics and delivers refresh wakeups |

Pair selection uses the core `reserve_pair` helper. Lower cost wins, equal cost
prefers `self`, and rejection tries the other candidate. If both reject, the
fallback candidate's error is returned. Locks are acquired in stable address
order independently of sampled order. Passing two clones of one endpoint
reserves only once and returns `PairChoice::First`. Both controllers' effects
are drained while locked and delivered after releasing both locks.

A rejected pair does not establish that an entire fleet is full. Applications
choose whether to sample again, try other candidates, or propagate rejection.
`ready()` is an advisory capacity wait, not FIFO admission or a readiness grant.
Another caller can consume capacity before a subsequent `try_reserve()`.
The application must bound the number of tasks waiting outside the controller
and enforce its own request deadlines. The adapter spawns no tasks.

## `Reservation`

A reservation owns an accepted, not-yet-dispatched queue slot. Dropping it cancels
that slot without endpoint failure feedback, including when a `dispatch` or
`run` future was never polled or is cancelled while waiting.

`dispatch(self).await` waits for FIFO order, GCRA pacing, and the emergency
inflight cap, then returns an `ActiveRequest`. Start endpoint work immediately
after dispatch. Holding an unpolled head reservation blocks later reservations.

`run(self, operation, classify).await` awaits dispatch, invokes the `FnOnce`
operation to create its future, awaits that future, then classifies a reference
to its output. It records completion and returns the output unchanged. Neither
closure runs under a controller lock. Panicking during operation creation,
execution, or classification drops the active guard and records abandonment.

## `ActiveRequest`

| Method | Effect |
| --- | --- |
| `dispatched_at()` | Returns a `std::time::Instant` in Tokio's clock domain |
| `finish(self, completion)` | Consumes the guard and records completion exactly once |
| Drop | Records `Completion::Abandoned` if not already finished |

Successful completion measures RTT from dispatch, excluding local queue waits.
`Completion::Failure` means unhealthy endpoint feedback. `Completion::Abandoned`
means no feedback, such as a local timeout; it preserves feedback silence while
other requests remain inflight. See the [controller reference](controller.md).

Guards track controller accounting, not the lifetime of separately spawned
operations. Dropping a `JoinHandle`, for example, does not stop its task. Callers
must arrange cancellation and retain the guard for the actual operation's life.

## Runtime and wakeups

Dispatch waits require a Tokio runtime with time enabled. All controller times
come from Tokio's clock, so paused-time tests use the same domain as timers.
Owned handles and dispatch futures can move between tasks; `run` also accepts
borrowed or non-`Send` operations when used within a compatible task.

Waiters register before checking state. Dispatch and cancellation wake admission
and dispatch waiters; completion wakes dispatch waiters. Load reads, snapshots,
and paired selection also deliver controller refresh effects. A waiting FIFO
head consumes its own refresh effects without waking itself repeatedly as
missing-feedback decay advances.

## Deployment contract

The adapter coordinates trusted, cooperative microservice clients. It does not
provide server-enforced public API rate limits, quotas, or abuse protection.
