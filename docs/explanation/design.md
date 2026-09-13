# How Loadpace controls and routes work

Loadpace combines admission control and load balancing. A conventional client
load balancer asks which endpoint should receive a request. Loadpace also asks
whether the client should accept another request yet.

## Per-endpoint control

Every endpoint owns independent state:

```text
RTT estimator → Gradient2 operating point → Little's Law → GCRA pacer
       ▲                                      │
       └──────── response outcome ───────────┘
```

This separation matters because endpoints can have different latency,
capacity, failure modes, and current load. A service-wide controller would
blur those differences.

## Fractional concurrency is useful

Gradient2's operating point is deliberately continuous. A value such as
`C* = 1.3` is not rounded into a semaphore limit of one or two.

Little's Law gives the derived rate:

```text
concurrency ≈ throughput × latency
rate ≈ C* / expected RTT
```

GCRA then spaces requests at that rate. Over time, the endpoint can average
around 1.3 concurrent requests even though instantaneous request counts are
integers.

The current `Gradient2` type is a small continuous Gradient2-style controller,
with intentionally conservative defaults. Its constants and exact update rule
are still candidates for simulation-driven tuning.

## GCRA is the normal actuator

GCRA keeps a theoretical arrival time (TAT). A newly accepted request receives
a virtual slot after the existing queue tail. It waits until that slot is due
before competing for inner-service readiness.

There are two related pacing positions:

- **Committed TAT:** pacing state created by requests that actually dispatched.
- **Virtual queue tail:** the predicted pacing position after accepted queued work.

Keeping them separate means P2C can see the cost of scheduled work without
turning a request that is later cancelled into permanent pacing debt.

## Queue delay is not endpoint RTT

The controller measures approximately:

```text
actual dispatch → response completion
```

It excludes time spent waiting for local admission, GCRA, or the bounded
queue. Including those delays would create a feedback loop in which pacing
causes the RTT estimate to rise, which reduces the rate, which causes more
pacing delay.

## Backpressure stays visible

The endpoint queue is a small scheduling horizon, not an overload buffer. A
queued request has not been sent to the server. Once the endpoint's horizon is
full, `poll_ready` is pending.

A framework adapter such as `loadpace-tower` also coordinates readiness
reservations across clones. This preserves the Service readiness contract
while preventing two clones from both believing they own the final local slot.

The generous inflight cap handles pathological cases such as stuck requests,
severe latency jumps, stale estimates, broken transports, or pacing bugs. It
is not intended to replace GCRA in normal operation.

## Predicted completion cost and P2C

Tower's P2C balancer samples two ready endpoints. `loadpace-tower` supplies a
metric that approximates the completion time of appending one more request:

```text
predicted completion = virtual queue-tail dispatch + expected RTT
```

This accounts for concurrent execution better than `RTT × queue length`, and
it avoids a global sort across all discovered endpoints. The endpoint's
controller owns the metric; Tower remains responsible for sampling and
selection.

## Probing and fairness

Independent clients can settle into a stable but unfair allocation. Loadpace
provides temporary randomized perturbations:

- positive probes are additive (`8 → 9`, `1 → 2`);
- negative probes are multiplicative (`8 → 6.4`, `1 → 0.8`).

The asymmetry gives a small client a meaningful opportunity to grow while
causing a dominant client to yield more absolute capacity. A probe does not
permanently assign ownership; normal feedback decides what remains sustainable.

`ProbeSchedule` accepts a caller-provided RNG. This makes production entropy
and deterministic simulation equally possible. The current framework adapter
exposes probe control but does not run a hidden background probe task.

## Discovery lifecycle

When a service is inserted, the Tower adapter's `AdaptiveDiscovery` creates
fresh controller state.
When it is removed, Tower stops selecting it. Existing response futures hold
their shared endpoint state long enough to complete or cancel, while a later
re-insertion starts with clean estimates.

This conservative state policy avoids carrying stale capacity assumptions
across endpoint identity reuse. Retaining state briefly across churn is a
future optimization.

## Failures and retries

Fast failures are not healthy observations. The controller penalizes classified
failures without using their low latency as a good RTT sample. The Tower
adapter treats inner-service errors as failures; applications that need richer
classification can drive the controller directly.

Retries must pass through normal admission and pacing. A retry path that sends
directly to the transport can bypass the protection that the original request
was relying on.

## Current boundaries

The first implementation intentionally leaves several decisions open:

- Gradient2 constants and smoothing policy need workload validation.
- The best RTT estimate for Little's Law may differ by service class.
- Probe duration, backoff, and interaction with Gradient2 need more simulation.
- Transport readiness is handled correctly by the adapter but is not yet part of the load prediction.
- The simulator uses fixed service times rather than a full network model.
- Automatic response classification beyond inner errors is application-specific.

The deterministic simulator is part of the crate so these questions can be
answered with repeatable scenarios rather than intuition alone.
