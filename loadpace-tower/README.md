# loadpace-tower

Tower adapters for [`loadpace`](https://crates.io/crates/loadpace), the
runtime-independent adaptive client-side load-balancing controller.

`loadpace-tower` provides:

- `AdaptiveEndpoint<S>`, a bounded and paced `tower::Service` adapter;
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
