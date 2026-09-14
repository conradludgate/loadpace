# loadpace-tower

Adaptive Tower services for clients balancing requests across a changing
fleet.

## Why this adapter exists

Tower's balancers decide which ready endpoint should receive a request, but a
ready endpoint can still be the wrong place to send more work: it may already
have a queue, its capacity may have changed, or its requests may be arriving
faster than its service can complete them. A static limit either wastes spare
capacity or applies the wrong limit to different endpoints.

`loadpace-tower` wraps each endpoint with the [`loadpace`](https://crates.io/crates/loadpace)
controller. It preserves Tower backpressure, paces dispatch with a per-endpoint
GCRA schedule, and exposes predicted completion cost for P2C selection. This
lets a dynamic discovery stream add and remove endpoints while each endpoint
learns its own safe operating point.

`loadpace-tower` provides:

- `AdaptiveEndpoint<S>`, a bounded and paced `tower::Service` adapter;
- `AdaptiveLayer`, for wrapping services in a `tower::ServiceBuilder` stack;
- `AdaptiveDiscovery`, which wraps discovered services with fresh controller
  state; and
- `LoadMetric`, a predicted completion-cost metric for Tower's P2C balancer.

```toml
[dependencies]
loadpace = "0.1"
loadpace-tower = "0.1"
```

See the [repository documentation](https://github.com/conradludgate/loadpace/tree/main/docs)
for integration guidance and design details.
