# loadpace

The runtime-independent core for adaptive client-side load balancing and
bounded backpressure.

## Why this crate exists

A client talking to a changing fleet needs to answer two questions: which
endpoint should receive a request, and should the client accept that request
yet? Static rate or concurrency limits cannot keep up with endpoints that have
different capacities, changing latency, or autoscaling workloads. An unbounded
local queue only hides the overload until it becomes harder to recover from.

Loadpace gives each endpoint a small feedback controller. It learns from
observed latency and outcomes, converts the learned operating point into a
smooth request rate, and keeps local admission bounded. A higher-level load
balancer can then choose the endpoint with the lowest predicted completion
cost. The core is independent of async runtimes and service frameworks so the
same control logic can be used by Tower, Rama, or a custom client.

The `loadpace` crate provides the controller and deterministic simulator. It
combines RTT estimation, fractional Gradient2 feedback, Little's Law, GCRA
pacing, virtual queue prediction, and controller-driven temporary fairness
probes without depending on an async runtime or service framework.

## Intended deployment model

This crate is for trusted microservice clients sharing private endpoints. Its
feedback and fairness behavior assumes those clients cooperate and run
compatible control logic. An unpaced or malicious client can consume capacity
without participating in the algorithm.

Do not use Loadpace as the primary rate limit, quota system, abuse-prevention
boundary, or DDoS defense for a general-purpose public API. Enforce those
policies at the server or another trusted ingress boundary.

```toml
[dependencies]
loadpace = "0.1"
```

The project documentation explains how to configure and use the controller:

- [Tutorial](https://github.com/conradludgate/loadpace/blob/main/docs/tutorial.md)
- [Controller reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/controller.md)
- [Design explanation](https://github.com/conradludgate/loadpace/blob/main/docs/explanation/design.md)

For Tower integration, see [`loadpace-tower`](https://crates.io/crates/loadpace-tower).
