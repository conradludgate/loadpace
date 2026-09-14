//! Tower adapters for the [`loadpace`] adaptive controller.
//!
//! The adapter keeps Tower-specific readiness, discovery, and load-balancing
//! concerns out of the runtime-independent `loadpace` crate.

#![forbid(unsafe_code)]

mod service;

pub use service::{AdaptiveDiscovery, AdaptiveEndpoint, LoadMetric, ResponseFuture};
