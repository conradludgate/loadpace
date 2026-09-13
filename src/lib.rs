//! Runtime-independent adaptive client-side load balancing and backpressure.
//!
//! This crate contains the deterministic controller and simulator. Framework
//! integrations live in separate crates such as `loadpace-tower`.

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
pub use gcra::{Gcra, GcraReservation};
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
}
