# loadpace-tokio

Adaptive pacing and bounded admission for ordinary async functions on Tokio.
Keep one `Endpoint` per destination; clones share its learned state and queue.

```rust
use loadpace::{Completion, EndpointConfig};
use loadpace_tokio::Endpoint;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
let endpoint = Endpoint::new(EndpointConfig::new(Duration::from_millis(20), 32));
let reservation = endpoint.try_reserve().expect("propagate backpressure if full");
let output = reservation.run(
    || async { 42 },
    |_| Completion::Success,
).await;
assert_eq!(output, 42);
}
```

`try_reserve` rejects when the scheduling horizon is full. `run` waits for FIFO
order and pacing before creating the operation. For manual integration, await
`reservation.dispatch()` and hold the returned `ActiveRequest` until calling
`finish(Completion)`.

Dropping queued work cancels its reservation. Dropping active work records
abandonment. Classify unhealthy endpoint feedback as `Failure` and local
timeouts without feedback as `Abandoned`. No background tasks are spawned.

`try_reserve_pair` compares two supplied endpoints and reserves one under both
controller locks. Applications retain discovery and random sampling. Optional
`ready().await` waits for queue capacity but does not reserve it; callers must
bound their own waiting tasks and timeouts.

This crate requires Rust 1.85 or newer and a Tokio runtime with time enabled
for dispatch waits. It uses Tokio's clock, including paused time in tests.

Loadpace assumes trusted, cooperative microservice clients. It does not replace
server-enforced public API quotas, admission control, or abuse prevention.

- [Tokio integration guide](https://github.com/conradludgate/loadpace/blob/main/docs/how-to/integrate-with-tokio.md)
- [Tokio API reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/tokio.md)
- [TCP connect example](https://github.com/conradludgate/loadpace/blob/main/loadpace-tokio/examples/tcp_connect.rs)
