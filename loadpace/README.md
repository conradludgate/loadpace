# loadpace

Runtime-independent adaptive client-side load balancing and backpressure.

The `loadpace` crate provides the controller and deterministic simulator. It
combines RTT estimation, fractional Gradient2 feedback, Little's Law, GCRA
pacing, virtual queue prediction, and temporary fairness probes without
depending on an async runtime or service framework.

```toml
[dependencies]
loadpace = "0.1"
```

The project documentation explains how to configure and use the controller:

- [Tutorial](https://github.com/conradludgate/loadpace/blob/main/docs/tutorial.md)
- [Controller reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/controller.md)
- [Design explanation](https://github.com/conradludgate/loadpace/blob/main/docs/explanation/design.md)

For Tower integration, see [`loadpace-tower`](https://crates.io/crates/loadpace-tower).
