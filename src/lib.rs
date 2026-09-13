//! Adaptive client-side load balancing and backpressure.
//!
//! The crate is split into a deterministic controller and an optional Tower
//! adapter. The controller can be simulated without an async runtime; the
//! adapter turns its decisions into a bounded `tower::Service`.

mod controller;
mod gcra;
mod gradient;
mod latency;
mod probe;

#[cfg(feature = "tower")]
mod service;

pub use controller::{
    ControllerSnapshot, DispatchReservation, DispatchState, EndpointConfig, EndpointController,
    InFlightRequest, Outcome,
};
pub use gcra::{Gcra, GcraReservation};
pub use gradient::{Gradient2, Gradient2Config};
pub use latency::{LatencyEstimator, LatencyEstimatorConfig};
pub use probe::{Probe, ProbeKind, ProbeSchedule, ProbeState};

#[cfg(feature = "tower")]
pub use service::{AdaptiveEndpoint, LoadMetric, ResponseFuture};

/// A small error used by callers that want to model rejected scheduling
/// explicitly in a simulator or their own adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// The endpoint's bounded scheduling horizon is full.
    QueueFull,
    /// The emergency inflight safety cap is full.
    InflightLimit,
}
