//! Adaptive client-side load balancing for changing service fleets, with
//! bounded backpressure.
//!
//! A client may need to balance work across endpoints whose capacities and
//! latencies differ or change as the fleet scales. Static rate and concurrency
//! limits cannot adapt to those differences, while unbounded local queues hide
//! overload. Loadpace learns an operating point for each endpoint, paces work
//! toward it, predicts completion cost for endpoint selection, and stops local
//! admission when the bounded scheduling horizon is full.
//!
//! This crate contains the runtime-independent controller and deterministic
//! simulator. Framework integrations live in separate crates such as
//! `loadpace-tower`, `loadpace-rama`, and `loadpace-tokio`.
//!
//! # Deployment scope
//!
//! Loadpace is intended for trusted microservice deployments where you control
//! the clients sharing an endpoint and can deploy a compatible congestion
//! controller to all of them. Its fairness mechanism assumes those clients
//! cooperate: an unpaced or malicious client can take capacity from paced
//! clients and invalidate the controller's feedback assumptions.
//!
//! Do not use Loadpace as the primary protection for a general-purpose public
//! API. It is not an authorization mechanism, quota system, abuse-prevention
//! boundary, or DDoS defense. Public traffic still needs server-enforced rate
//! limits, quotas, and admission control.
//!
//! # Where to start
//!
//! - Use [`EndpointController`] when integrating with a custom runtime or
//!   service abstraction.
//! - Use `loadpace-tokio` for ordinary async functions on Tokio.
//! - Use [`simulate`] to test controller settings against deterministic
//!   workloads before deploying them.
//! - Use the `loadpace-tower` or `loadpace-rama` adapter when your client is
//!   already built on one of those frameworks.
//!
//! The repository also contains a
//! [tutorial](https://github.com/conradludgate/loadpace/blob/main/docs/tutorial.md),
//! task-oriented [how-to guides](https://github.com/conradludgate/loadpace/tree/main/docs/how-to),
//! a [controller reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/controller.md),
//! and a [design explanation](https://github.com/conradludgate/loadpace/blob/main/docs/explanation/design.md).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod controller;
mod gcra;
mod gradient;
mod latency;
mod p2c;
mod probe;
mod simulator;

pub use controller::{
    Completion, ControllerChanges, ControllerSnapshot, DispatchReservation, DispatchState,
    EndpointConfig, EndpointController, InFlightRequest, Outcome,
};
pub use gcra::Gcra;
pub use gradient::{Gradient2, Gradient2Config};
pub use latency::{LatencyEstimator, LatencyEstimatorConfig};
pub use p2c::{PairChoice, reserve_pair};
pub use probe::{Probe, ProbeKind, ProbeSchedule};
pub use simulator::{
    EndpointReport, SimulatedEndpoint, SimulationConfig, SimulationReport, simulate,
};

/// A small error used by callers that want to model rejected scheduling
/// explicitly in a simulator or their own adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// The endpoint's bounded scheduling horizon is full.
    QueueFull,
    /// The controller exhausted its request identity space.
    IdExhausted,
}
