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
//! `loadpace-tower` and `loadpace-rama`.

#![forbid(unsafe_code)]

mod controller;
mod gcra;
mod gradient;
mod latency;
mod probe;
mod simulator;

pub use controller::{
    ControllerSnapshot, DispatchReservation, DispatchState, EndpointConfig, EndpointController,
    InFlightRequest, Outcome,
};
pub use gcra::Gcra;
pub use gradient::{Gradient2, Gradient2Config};
pub use latency::{LatencyEstimator, LatencyEstimatorConfig};
pub use probe::{Probe, ProbeKind, ProbeSchedule, ProbeState};
pub use simulator::{
    EndpointReport, SimulatedEndpoint, SimulationConfig, SimulationReport, simulate,
};

/// A small error used by callers that want to model rejected scheduling
/// explicitly in a simulator or their own adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// The endpoint's bounded scheduling horizon is full.
    QueueFull,
    /// The emergency inflight safety cap is full.
    InflightLimit,
    /// The controller exhausted its request identity space.
    IdExhausted,
}
