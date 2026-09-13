# Loadpace

Loadpace is an experiment in treating client-side load balancing as a control problem rather than just a routing problem.

The central idea is:

- learn how much load each endpoint can safely sustain;
- convert that learned operating point into a smooth request rate;
- route requests toward endpoints with the lowest predicted completion cost;
- preserve strict backpressure instead of hiding overload behind large client queues;
- use randomized positive and negative probing so independent clients can escape unfair equilibria without coordinating with each other.

The intended Rust integration is a `tower::Service`, with Tower's dynamic discovery and `p2c::Balance` reused where possible.

This README describes the current design. It is intentionally more detailed than a typical early-project README so that it can also act as an implementation guide.

---

## Motivation

A conventional client-side load balancer answers:

> Which server should receive this request?

That is only part of the problem.

In a distributed system, a client also needs to answer:

> Should I send another request at all?

If every client forwards work as quickly as it arrives, load balancing can still overwhelm otherwise healthy servers. Static concurrency or rate limits are not sufficient when:

- server capacity changes over time;
- endpoints have different performance characteristics;
- clients and servers scale independently;
- latency changes with queue depth;
- requests have variable service times;
- multiple independent clients compete for the same endpoint.

Loadpace therefore combines **routing** and **admission control**.

Each endpoint has its own adaptive controller. The controller estimates a useful operating point from observed latency, converts that operating point into a paced request rate, and exposes only a small bounded amount of scheduling slack to the outer load balancer.

---

## Design overview

At a high level:

```text
                         dynamic discovery
                                │
                                ▼
                     discovered endpoint services
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │ AdaptiveEndpoint<Service>   │
                 │                             │
                 │  latency estimator          │
                 │  Gradient2                  │
                 │  Little's Law               │
                 │  stochastic probing         │
                 │  GCRA pacer                 │
                 │  small bounded queue        │
                 │  generous safety cap        │
                 └──────────────┬──────────────┘
                                │
                                ▼
                     tower::balance::p2c::Balance
                                │
                                ▼
                        Service<Request>
```

The responsibilities are deliberately separated:

- **Gradient2** estimates a per-endpoint fractional operating point.
- **Little's Law** converts that operating point into a rate.
- **GCRA** turns the rate into smooth dispatch timing.
- **Power of two choices** selects among endpoints that still have bounded scheduling capacity.
- **Stochastic positive and negative probes** help independent clients redistribute capacity fairly and discover unused capacity.
- **A tiny bounded per-endpoint queue** gives P2C enough lookahead to consider an endpoint even when it cannot dispatch immediately.
- **Tower backpressure** propagates upstream when every endpoint has exhausted that small scheduling horizon.

The queue is not intended to absorb overload. It is a bounded scheduling horizon.

---

## Per-endpoint control

Congestion control is **per endpoint**, not service-wide.

Every client maintains independent state for every endpoint it currently knows about:

```rust
struct EndpointState {
    latency: LatencyEstimator,
    gradient: Gradient2,
    pacer: Gcra,
    probe: ProbeState,

    inflight: usize,

    // Very small and bounded.
    queued: usize,
    queue_capacity: usize,
}
```

This is important because endpoints may differ in capacity, latency, failure mode, or current load.

A single service-wide controller would blur those differences and make it harder for the load balancer to react correctly.

---

## Gradient2 as a fractional operating point

Although Gradient2 is usually described in terms of concurrency limits, Loadpace should not primarily implement its output as an integer semaphore.

Instead, treat the result as a **continuous target concurrency**:

```text
C* = 1.3
```

That value is useful even though "1.3 concurrent requests" cannot exist at an instant.

Using Little's Law:

```text
concurrency ≈ throughput × latency
```

so:

```text
rate ≈ C* / RTT
```

For example:

```text
target concurrency = 1.3
expected RTT       = 1 second

rate = 1.3 req/s
```

GCRA can then pace requests at roughly one request every 769 ms.

Over time, the endpoint naturally averages around 1.3 concurrent requests: sometimes one request will be active, sometimes two. This preserves the fractional signal instead of forcing the controller to choose between an integer concurrency limit of 1 or 2.

The same model works at larger operating points. If Gradient2 estimates `100.7`, the exact instantaneous inflight count matters much less than maintaining an appropriate long-term sending rate.

---

## GCRA is the primary actuator

Gradient2 learns the operating point; GCRA enforces it.

Conceptually:

```text
observed endpoint latency
          │
          ▼
      Gradient2
          │
          ▼
 fractional target concurrency C*
          │
          │ Little's Law
          ▼
      rate = C* / RTT
          │
          ▼
         GCRA
          │
          ▼
   paced request dispatch
```

This avoids bursty integer-window behavior, especially for endpoints with high latency or low throughput.

The GCRA state also gives the balancer a useful notion of **theoretical arrival time (TAT)**: when the endpoint would be able to dispatch future queued work if current conditions remain stable.

### Latency used by the controller

Latency measurements should describe the endpoint, not the local scheduler.

Measure approximately:

```text
actual dispatch → response completion
```

Do **not** include time spent:

- waiting in Loadpace's bounded queue;
- waiting for GCRA;
- waiting for client-side admission.

Otherwise local pacing would increase measured RTT, which would reduce the calculated rate, which would increase pacing delay again: an undesirable self-reinforcing feedback loop.

---

## Concurrency is a safety valve, not the control mechanism

Loadpace should not normally enforce `ceil(C*)` or `floor(C*)` as a hard concurrency limit.

That throws away the useful fractional operating point and performs particularly poorly around low concurrency.

There should still be a **generous hard inflight safety cap** to protect the process against pathological conditions such as:

- requests that never complete;
- severe latency jumps;
- stale RTT estimates;
- broken transports;
- bugs in pacing;
- endpoints accepting work while making no progress.

The safety cap should be much larger than the current Gradient2 operating point and should almost never participate in normal control.

The exact policy is intentionally undecided. It should be selected through simulation and failure testing rather than baked into the initial algorithm.

---

## The bounded endpoint queue

Each adaptive endpoint exposes a small configurable queue to Tower.

For example:

```text
queue_capacity = 4

Endpoint A: [request][request][       ][       ]  → Ready
Endpoint B: [request][request][request][request]  → Pending
```

The important semantic distinction is:

> An accepted queue slot is permission to schedule the request, not permission to send it immediately.

Requests leave the queue only when:

```text
GCRA permits dispatch
&& safety inflight cap permits dispatch
&& inner Service::poll_ready() permits dispatch
```

This allows Tower's P2C balancer to continue considering an endpoint that will become dispatchable shortly, while still keeping all client-side buffering explicitly bounded.

### Why keep the queue tiny?

Large client-side queues are dangerous:

- they hide overload from callers;
- they make requests sticky to endpoints too early;
- they increase tail latency;
- they make endpoint recovery slower;
- they undermine end-to-end backpressure.

The queue exists only to provide a little scheduling lookahead.

A likely starting range is on the order of `1..=4`, configurable by the application. The correct default should come from simulation and real workloads.

Once every endpoint queue is full, Tower's balancer becomes `Pending` and backpressure propagates to the caller.

---

## Load metric: predicted completion cost

Tower's P2C balancer requires each ready service to expose a comparable `Load::Metric`.

The metric should approximate:

> If one more request were appended to this endpoint's bounded queue now, when would it finish?

A useful conceptual model is:

```text
predicted_completion =
    predicted_dispatch_time
    + expected_RTT
```

The endpoint's virtual GCRA/TAT state already captures much of the queueing cost.

For example:

```text
RTT                  = 10 ms
target concurrency   = 10
rate                  = 1000 req/s
pacing interval       = 1 ms
```

If three queued requests already occupy future pacing slots, the new request might be predicted to dispatch in approximately 3 ms and finish in approximately 13 ms.

This is better than simply multiplying RTT by queue length because requests may execute concurrently.

### Virtual TAT versus committed TAT

It is useful to distinguish:

```text
committed TAT
    pacing state produced by requests actually dispatched

virtual queue-tail TAT
    predicted pacing position after all currently queued requests
```

The P2C metric can use the virtual queue tail.

The committed pacer should only advance according to actual dispatch semantics.

Keeping these separate makes cancellation, queue removal, pacing changes, and Gradient2 updates easier to reason about.

### Concurrency and transport readiness

The eventual prediction may need to consider more than GCRA:

```text
predicted_dispatch =
    max(
        virtual_gcra_slot,
        estimated_inflight_release,
        transport_readiness,
        now,
    )
```

Initially, however, the model should remain deliberately simple.

GCRA plus expected RTT should do most of the work. The safety concurrency cap is intentionally generous, and arbitrary inner `Service::poll_ready()` latency may not be predictable at all.

---

## Power of two choices

Routing is based on power of two choices.

Tower's `p2c::Balance` is a strong candidate for reuse rather than implementing a custom balancer.

The basic selection becomes:

```text
sample endpoint A
sample endpoint B

compare:
    A.load()
    B.load()

pick the lower predicted completion cost
```

This retains the low overhead and strong balancing properties of P2C without requiring a global sort over the entire endpoint set.

The adaptive endpoint is responsible for making its load metric meaningful.

---

## Tower integration

The intended public shape is approximately:

```text
dynamic discovery
        │
        ▼
map each discovered Service
        │
        ▼
AdaptiveEndpoint<Service>
        │
        ▼
tower::balance::p2c::Balance
        │
        ▼
Service<Request>
```

Tower's existing ready cache is useful here.

Endpoints with queue capacity report `Ready` and participate in P2C.

Endpoints whose bounded scheduling queue is full report `Pending`.

When no endpoint has capacity, the outer service reports `Pending`, preserving backpressure.

### Adaptive endpoint semantics

Conceptually:

```rust
impl<S, Request> Service<Request> for AdaptiveEndpoint<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        if self.queue_is_full() {
            self.queue_capacity_waker.register(cx.waker());
            return Poll::Pending;
        }

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Insert into the bounded scheduling queue.
        //
        // The worker later dispatches it when:
        // - GCRA allows it
        // - the safety inflight cap allows it
        // - the underlying service is ready
        todo!()
    }
}
```

The real implementation will need to respect Tower's readiness reservation contract carefully: once `poll_ready` returns ready, capacity for the following `call` must genuinely be available.

A small internal permit/reservation mechanism is likely appropriate.

### Do not split the controller into middleware soup

Gradient2, Little's Law, GCRA, probing, RTT estimation, and queue prediction are tightly coupled.

They should be implemented as one coherent endpoint controller, not as:

```text
Gradient2Layer
LittleLawLayer
GcraLayer
ProbeLayer
QueueLayer
...
```

Tower should be an integration boundary, not the internal algorithmic architecture.

---

## Dynamic discovery

Discovery is assumed to be dynamic.

The system should support insertion and removal of endpoints over time:

```text
Change::Insert(endpoint_id, service)
Change::Remove(endpoint_id)
```

When an endpoint appears:

1. create fresh endpoint controller state;
2. wrap the discovered transport/service in `AdaptiveEndpoint`;
3. insert it into Tower's balancing/discovery path.

When an endpoint disappears:

1. immediately stop assigning new requests to it;
2. allow already dispatched requests to complete where practical;
3. discard or eventually expire its controller state.

A later optimization may retain controller state briefly across discovery churn, but fresh state should be the safe initial behavior.

Discovery should not be tied to a particular mechanism. Possible sources include:

- DNS;
- Kubernetes;
- Consul;
- xDS;
- static endpoint lists;
- custom application discovery.

---

## Stochastic probing

Independent adaptive clients can settle into a stable but unfair allocation.

For an endpoint capable of roughly 10 requests per second, three clients might converge to:

```text
client A: 8 req/s
client B: 1 req/s
client C: 1 req/s
```

The endpoint is fully utilized, so a controller focused only on congestion may have no reason to disturb this equilibrium.

Loadpace therefore adds **randomized probing** per client × endpoint.

The goals are:

1. escape unfair equilibria;
2. allow clients to discover newly available capacity;
3. avoid synchronized probing across many clients.

Probes should perturb the normal controller temporarily rather than replace it.

---

## Positive probing

Positive probes temporarily attempt to claim additional capacity.

For fairness, they should be primarily **additive**, not proportional.

Good:

```text
8 → 9
1 → 2
1 → 2
```

A `+1` probe gives the underrepresented client a much larger proportional opportunity.

A percentage-based probe would favor incumbents:

```text
8 × 1.10 = +0.8
1 × 1.10 = +0.1
```

which is the opposite of the desired fairness behavior.

Probe timing should be randomized so clients do not all increase simultaneously.

The exact probe unit may ultimately be expressed in pacing rate, virtual concurrency, or another normalized quantity. That should be evaluated experimentally.

---

## Negative probing

Negative probes temporarily relinquish capacity.

They answer:

> If I voluntarily use less of this endpoint for a short time, will another client occupy the released capacity?

For negative probing, a **multiplicative** reduction is preferable:

```text
8 → 6.4
1 → 0.8
1 → 0.8
```

A dominant client releases more absolute capacity than a small client.

An absolute negative probe such as `-1` would disproportionately punish small clients.

This creates an intentionally asymmetric policy:

```text
positive probe: additive increase
negative probe: multiplicative decrease
```

The combination has useful AIMD-like fairness properties without requiring explicit coordination between clients.

### Capacity transfer

A particularly useful behavior is:

```text
dominant client yields temporarily
            │
            ▼
another client occupies the freed capacity
            │
            ▼
dominant client's probe ends
            │
            ▼
attempting to reclaim everything increases congestion
            │
            ▼
normal Gradient2 feedback settles it lower
```

The probe itself does not need to permanently encode ownership.

The underlying congestion controller makes the changed allocation persist if the endpoint cannot sustain everyone returning to the old rate.

---

## Probing and Gradient2 interaction

Probes should be represented explicitly.

For example:

```rust
enum Probe {
    None,
    Positive {
        delta: f64,
        until: Instant,
    },
    Negative {
        factor: f64,
        until: Instant,
    },
}
```

Care is required so that Gradient2 does not immediately "learn away" an intentional probe.

For example, during a negative probe:

1. the client reduces its rate;
2. endpoint latency improves;
3. Gradient2 might conclude there is abundant spare capacity;
4. it increases the base operating point;
5. the probe ends;
6. the client overshoots.

The exact interaction policy is not yet settled.

Possible approaches include:

- damping upward Gradient2 adaptation during a negative probe;
- tagging probe samples;
- letting Gradient2 observe congestion but filtering probe-induced improvements;
- using separate base and effective operating points.

A useful conceptual split is:

```text
base operating point
    owned by Gradient2

temporary probe perturbation
    owned by the probing controller

effective paced rate
    derived from both
```

This is an area that deserves simulation before committing to an API.

---

## Failures and classification

Not every fast response is a healthy response.

An endpoint returning immediate failures must not appear excellent merely because its observed latency is low.

The controller will need a policy for classifying outcomes such as:

- successful responses;
- transport errors;
- connection failures;
- overload responses;
- application errors;
- cancellations;
- deadlines/timeouts;
- retries.

Likewise, retries must pass through normal admission control. A retry path that bypasses GCRA can recreate exactly the overload behavior Loadpace is intended to prevent.

Failure handling is protocol/application dependent, so the API should likely allow callers to classify response outcomes rather than hard-code HTTP-specific semantics.

---

## Cold start and recovery

New endpoints have no trustworthy RTT or congestion history.

They still need enough traffic to become measurable.

The initial implementation needs an explicit cold-start strategy, potentially including:

- conservative initial RTT;
- conservative initial operating point;
- temporary exploration priority;
- inherited defaults from the endpoint class;
- aggressive decay of cold-start assumptions after real observations arrive.

Similarly, an endpoint that was unhealthy and therefore receives little traffic must eventually get opportunities to demonstrate recovery.

Stochastic probing helps, but the exact cold/recovery behavior should be tested independently.

---

## Expected internal architecture

A likely internal decomposition is:

```rust
struct AdaptiveEndpoint<S> {
    inner: S,
    state: Arc<EndpointState>,
    queue: BoundedQueue<Request>,
}

struct EndpointState {
    latency: LatencyEstimator,
    gradient: Gradient2,
    pacing: PacingState,
    probing: ProbeState,
    inflight: AtomicUsize,
    config: EndpointConfig,
}

struct PacingState {
    base_rate: Rate,
    committed_tat: Instant,
    virtual_tail_tat: Instant,
}

struct EndpointConfig {
    queue_capacity: usize,

    // Emergency only.
    max_inflight: usize,

    // Gradient2 / latency / probing parameters...
}
```

The exact synchronization model is open.

Potential approaches include:

- one endpoint worker task with messages;
- atomics plus lightweight locks around controller updates;
- a single mutex protecting the relatively infrequent control-plane state;
- split fast-path pacing state from slower latency/controller state.

Correctness and control behavior should come before micro-optimizing synchronization.

---

## Public API

The lower-level controller is usable without Tower:

```rust
let mut controller = EndpointController::new(config, now);
let reservation = controller.reserve(now)?;

if controller.dispatch_state(reservation, now) == DispatchState::Ready {
    let request = controller.on_dispatched(reservation, now).unwrap();
    controller.on_complete(request, Outcome::Success, latency, now);
}
```

The Tower integration wraps a discovered service directly:

```rust
let endpoint = AdaptiveEndpoint::new(service, EndpointConfig::default());
```

This keeps the algorithms testable and reusable while Tower remains the primary integration.

## Current crate implementation

The repository now contains a usable first implementation of this design.

`EndpointController` is the deterministic core. It owns the latency estimator,
continuous Gradient2-style operating point, Little's Law rate conversion,
GCRA state, temporary probes, failure counters, and bounded virtual queue. It
can be driven without an async runtime, which makes control behavior easy to
test and simulate.

The optional `tower` feature provides `AdaptiveEndpoint<S>`. It implements
`tower::Service` and `tower::load::Load`: `poll_ready` reserves a bounded
readiness slot, `call` reserves a virtual pacing slot, and the returned future
waits for GCRA and inner-service readiness before recording actual dispatch.
Dropping that future cancels the virtual reservation or records a dispatched
request as failed, so stuck work cannot leak controller capacity.

For dynamic discovery, wrap a stream yielding Tower `Change` values and pass
it to Tower's existing P2C balancer:

```rust
use loadpace::{AdaptiveDiscovery, EndpointConfig};
use tower::balance::p2c::Balance;

let adaptive = AdaptiveDiscovery::<_, Request>::new(discovery, EndpointConfig::default());
let client = Balance::new(adaptive);
```

The discovery wrapper creates fresh controller state for each insertion and
preserves removal events. Tower's balancer then performs P2C over each
endpoint's predicted completion cost (`virtual queue tail + expected RTT`).

The `simulate` function is a deterministic event-driven fixed-service-time
simulator. It is intentionally part of the public crate so changes to pacing,
backpressure, or endpoint selection can be compared against repeatable
scenarios. The integration tests cover the single-endpoint backpressure case,
unequal endpoint latencies, cancellation, concurrent responses, dynamic
discovery, and Tower P2C integration.

---

## Core invariants

The implementation should make the following properties explicit and testable.

### Backpressure is bounded

The total amount of locally accepted-but-not-dispatched work is bounded by:

```text
number_of_endpoints × queue_capacity
```

plus any request already reserved through Tower's readiness contract.

No hidden unbounded queue is permitted.

### The queue does not define server load

Requests in the local endpoint queue have not yet been sent.

Server load is determined by actual paced dispatch.

### GCRA is the normal actuator

The fractional Gradient2 operating point is translated into a steady rate.

The hard inflight limit is an emergency safety mechanism.

### Endpoint control is independent

Every endpoint has its own:

- Gradient2 state;
- latency estimate;
- pacing state;
- probe state;
- bounded scheduling queue.

### Probing is temporary

Positive and negative probes perturb the base controller.

They do not directly assign long-term ownership of capacity.

### P2C operates on predicted cost

Selection should approximate the completion cost of adding one more request to an endpoint, not merely use raw latency or raw inflight count.

---

## Simulation plan

This design has several interacting feedback loops, so simulation should be treated as part of the implementation rather than an optional benchmark.

At minimum, test:

### Basic endpoint utilization

- one client;
- one endpoint;
- fixed service time;
- verify convergence to stable utilization and pacing.

### Fractional low-concurrency case

Example:

```text
RTT = 1 s
desired virtual concurrency ≈ 1.3
```

Verify that pacing produces approximately 1.3 req/s without oscillating between integer concurrency limits.

### Multiple endpoints

- endpoints with equal capacity;
- endpoints with unequal capacity;
- endpoints with unequal RTT;
- one endpoint becoming slower;
- one endpoint recovering;
- endpoint addition/removal.

Verify P2C routing and per-endpoint convergence.

### Fairness between clients

The canonical case:

```text
endpoint capacity ≈ 10 req/s

initial allocation:
8 + 1 + 1
```

Compare:

- no probing;
- positive probing only;
- positive + negative probing.

Measure convergence toward a fairer allocation.

### Many clients

Test tens or hundreds of independently adapting clients against one endpoint.

Look for:

- synchronized oscillation;
- probe collisions;
- starvation;
- stable unfair equilibria;
- excessive latency.

### Autoscaling / capacity change

Change endpoint capacity:

```text
10 req/s → 20 req/s
20 req/s → 10 req/s
```

Measure how quickly spare capacity is discovered and overload is shed.

### Variable request duration

Mix short and long requests.

Ensure rate control remains safe and verify that the generous inflight safety cap protects against accumulated stuck work.

### Failures

Inject:

- fast overload errors;
- timeouts;
- connection failures;
- hanging requests;
- retries.

Verify that failures cannot appear as attractive low-latency endpoints.

### Queue sizing

Compare queue capacities such as:

```text
1, 2, 4, 8
```

Measure:

- throughput;
- client queue delay;
- P2C quality;
- tail latency;
- backpressure responsiveness.

The intention is to keep this number as small as practical.

---

## Metrics

Useful observability per endpoint should include at least:

```text
observed RTT
latency baseline / short / long estimates
Gradient2 virtual concurrency
derived base rate
effective probed rate
GCRA TAT
virtual queue-tail TAT
inflight requests
queued requests
queue capacity
positive probe state
negative probe state
completed request rate
error rate
```

At the aggregate client level:

```text
requests dispatched
requests backpressured
endpoint selection distribution
local queue delay
end-to-end latency
```

For simulations, also compute a fairness metric such as Jain's fairness index across clients sharing an endpoint.

---

## Open questions

The current design deliberately leaves several choices unresolved.

### Exact Gradient2 implementation

Which constants, smoothing windows, and minimum/maximum behavior should be used?

### RTT estimate used for Little's Law

Candidates include:

- current smoothed RTT;
- a slower RTT estimator;
- baseline/minimum RTT;
- a combination of baseline and current latency.

Using a highly reactive RTT in both Gradient2 and `C*/RTT` may create excessive double feedback.

### Probe schedule

Need to determine:

- positive probe size;
- negative probe factor;
- duration;
- randomized interval distribution;
- backoff after congestion;
- whether probing should depend on observed allocation or rate.

### Probe/Gradient2 interaction

How should intentional perturbations influence the base controller?

### Safety inflight cap

It should be generous enough not to interfere with normal pacing but finite enough to protect against stuck requests.

### Queue default

Likely small. Needs empirical validation.

### Load metric details

`virtual_tail_TAT + expected_RTT` is the initial model.

It may later include:

- estimated inflight release;
- transport readiness history;
- failure penalties;
- uncertainty/cold-start penalties.

### Cancellation

If a queued request is cancelled, its speculative virtual pacing slot should not unnecessarily distort future load predictions.

### Dynamic discovery state retention

Should quickly removed/reinserted endpoints inherit recent controller state?

The first implementation should probably prefer correctness and fresh state.

---

## Initial implementation roadmap

A reasonable implementation order is:

1. **Build a deterministic simulator.**
   Implement one endpoint, a synthetic service-time model, GCRA, and a simple fractional target concurrency.

2. **Add Gradient2.**
   Verify convergence and the fractional concurrency → rate translation.

3. **Add multiple endpoints and P2C.**
   Start with a standalone simulator implementation of predicted completion cost.

4. **Add bounded scheduling queues.**
   Verify that queue size remains small and overload still produces backpressure.

5. **Add stochastic positive probing.**
   Test the `8 + 1 + 1` fairness case.

6. **Add stochastic negative probing.**
   Compare convergence and latency against positive-only probing.

7. **Introduce the Tower adapter.**
   Implement `AdaptiveEndpoint<S>: Service<Request> + Load`.

8. **Reuse Tower dynamic discovery and `p2c::Balance`.**
   Validate readiness semantics carefully.

9. **Add failure classification and retry integration.**

10. **Benchmark and stress test.**
    Especially many-client convergence, endpoint churn, cancellation, and transport readiness.

The simulator should remain in the repository permanently. The interactions between pacing, congestion control, load balancing, and probing are important enough that future algorithm changes should be evaluated against the same scenarios.

---

## Non-goals

At least initially, Loadpace is not intended to provide:

- server-coordinated global fairness;
- mathematically strict bandwidth allocation between mutually untrusted clients;
- large client-side request buffers;
- distributed consensus over endpoint capacity;
- protocol-specific retry or health-check policy;
- a replacement for server-side overload protection.

The library aims for useful **emergent fairness and adaptive utilization from independent clients**, while remaining bounded and safe under overload.

Server-side admission control is still valuable.

---

## Summary

Loadpace combines four main ideas:

```text
Gradient2
    learns a fractional per-endpoint operating point

Little's Law + GCRA
    convert that operating point into smooth paced dispatch

Power of two choices
    routes bounded scheduled work toward the lowest predicted cost

Stochastic positive/negative probing
    destabilizes unfair client allocations and discovers spare capacity
```

The most important invariant is **backpressure**.

Each endpoint exposes only a tiny bounded scheduling queue. Once those queues are full, the client stops accepting work. GCRA—not a large queue and not an integer concurrency semaphore—is responsible for feeding each endpoint at the learned rate.

The resulting system should adapt independently to every endpoint, make good routing choices with little global coordination, preserve fractional low-throughput operating points, and give competing clients a way to converge away from unfair but otherwise stable allocations.
