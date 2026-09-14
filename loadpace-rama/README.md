# loadpace-rama

Rama integration for the [`loadpace`](https://crates.io/crates/loadpace)
adaptive client-side load-balancing and backpressure controller.

The adapter wraps a Rama `Service`, reserving a bounded virtual scheduling
slot before dispatch and recording the actual service latency afterwards.

This crate is experimental. See the [Loadpace repository](https://github.com/conradludgate/loadpace)
for the design and current documentation.
