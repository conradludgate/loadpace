# Pace ordinary async operations with Tokio

Use `loadpace-tokio` when your client calls async functions directly, such as
opening compute connections, without a Tower or Rama service abstraction.
The crate is part of this workspace; until its first publication, use local
paths or pin both dependencies to the same repository revision:

```toml
[dependencies]
loadpace = { path = "../loadpace/loadpace" }
loadpace-tokio = { path = "../loadpace/loadpace-tokio" }
tokio = { version = "1", features = ["macros", "rt", "time", "net"] }
```

## Retain a controller per destination

Create an `Endpoint` from your expected RTT and initial sustainable concurrency.
Store it alongside the destination and clone it for concurrent calls. Recreating
it per request loses learned state and defeats shared backpressure.

```rust
use loadpace::EndpointConfig;
use loadpace_tokio::Endpoint;
use std::time::Duration;

let endpoint = Endpoint::new(EndpointConfig::new(Duration::from_millis(20), 32));
```

## Reserve before starting work

`try_reserve()` returns a reservation immediately or a `ScheduleError`. Return
backpressure upstream when full, or wait with a bounded caller deadline:

```rust
let reservation = loop {
    endpoint.ready().await;
    match endpoint.try_reserve() {
        Ok(reservation) => break reservation,
        Err(loadpace::ScheduleError::QueueFull) => continue,
        Err(error) => return Err(error),
    }
};
```

This loop is only an admission fragment: apply your operation's overall timeout
around it, and bound the number of calling tasks. `ready()` does not reserve a
slot, so concurrent callers must recheck. It also does not grant dispatch.

Await `reservation.dispatch()` before invoking the operation and keep the active
guard until it ends. Alternatively, `reservation.run(operation, classify)` does
both and creates the operation only after dispatch. The operation closure can
borrow local state; it does not need to be boxed or spawned.

## Classify feedback and local timeouts

For a TCP connect attempt, an outer timeout can cover queueing and connection
setup together:

```rust
use loadpace::Completion;
use std::io;
use tokio::{net::TcpStream, time::timeout};

let result = timeout(
    Duration::from_secs(2),
    reservation.run(
        || TcpStream::connect(address),
        |result| match result {
            Ok(_) => Completion::Success,
            Err(error) if error.kind() == io::ErrorKind::TimedOut => Completion::Abandoned,
            Err(_) => Completion::Failure,
        },
    ),
).await;
```

If the timeout fires while queued, dropping the future cancels admission without
penalizing the endpoint. If it fires after dispatch, the active guard records
abandonment. A successful connection records only dispatch-to-connect RTT.
Adjust error classification for your protocol: local cancellation or timeouts
without feedback must not be reported as endpoint responses.

For manual integration, call `active.finish(Completion::Success)` on healthy
feedback, `Failure` on unhealthy feedback, or `Abandoned` when no feedback
arrived. Dropping the active guard also records abandonment. Guards do not stop
separately spawned work; arrange that work's cancellation yourself.

## Select between sampled candidates

For two sampled `(destination, Endpoint)` entries, call:

```rust
let (choice, reservation) = first_endpoint.try_reserve_pair(&second_endpoint)?;
let destination = match choice {
    loadpace::PairChoice::First => first_destination,
    loadpace::PairChoice::Second => second_destination,
};
```

The returned reservation already owns capacity on the selected endpoint. Use it
for the selected destination; do not reserve a second time. Selection refreshes
both loads, prefers lower predicted completion cost, and tries the other endpoint
if the preferred one rejects. Equal costs favor the first sample.

For a fleet, sample distinct candidates with your application's RNG. The adapter
handles locking and clone aliases, while your application keeps discovery,
sampling, and retry policy. The core helper has no RNG type in its signature,
so this also works with applications using a different rand version.

See the runnable [TCP connect example](../../loadpace-tokio/examples/tcp_connect.rs)
for a complete two-destination client, and the [reference](../reference/tokio.md)
for ownership and wakeup semantics.
