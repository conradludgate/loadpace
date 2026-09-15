# Run a deterministic simulation

Use the simulator when changing controller constants or comparing endpoint
selection behavior. It uses fixed service times, configurable endpoint worker
pools, and a seeded P2C choice so a scenario can be repeated.

## Create a simulation

```rust
use std::time::Duration;

use loadpace::{
    simulate, EndpointConfig, SimulatedEndpoint, SimulationConfig,
};

let endpoint = SimulatedEndpoint {
    config: EndpointConfig::new(Duration::from_millis(20), 32)
        .with_queue_capacity(4),
    workers: 2,
    service_time: Duration::from_millis(20),
};

let report = simulate(SimulationConfig {
    duration: Duration::from_secs(30),
    offered_rate: 100.0,
    endpoints: vec![endpoint],
    seed: 7,
});

println!("offered: {}", report.offered);
println!("accepted: {}", report.accepted);
println!("backpressured: {}", report.backpressured);
println!("completed: {}", report.completed);
```

## Compare endpoints

Add endpoints with different fixed service times:

```rust
let report = simulate(SimulationConfig {
    duration: Duration::from_secs(30),
    offered_rate: 100.0,
    endpoints: vec![
        SimulatedEndpoint {
            config: EndpointConfig::new(Duration::from_millis(10), 10),
            workers: 1,
            service_time: Duration::from_millis(10),
        },
        SimulatedEndpoint {
            config: EndpointConfig::new(Duration::from_millis(100), 10),
            workers: 1,
            service_time: Duration::from_millis(100),
        },
    ],
    seed: 42,
});
```

Inspect `report.endpoints[n].snapshot` to compare expected RTT, target
concurrency, effective rate, queue depth, and failure counts.

## Use the results

At minimum, check these invariants for a scenario:

- `max_queued` does not exceed the configured queue capacity;
- `accepted` and `backpressured` account for offered requests;
- faster endpoints receive more work when their predicted completion cost is lower;
- changing capacity or service time produces recovery rather than an unbounded queue.

The simulator intentionally does not model a protocol, network jitter, or
application-specific failure responses. Worker pools do model endpoint
saturation: when all workers are busy, dispatched requests wait in the
endpoint's service queue and their measured RTT increases. Add richer models
only when a test needs them; keep the basic scenarios small enough to explain
when they fail.
