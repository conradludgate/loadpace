//! Tower integration for adaptive client-side load balancing and backpressure.
//!
//! Use this crate when a Tower client balances requests across a changing fleet
//! and needs each endpoint to adapt to its own capacity and latency. The
//! adapter preserves Tower's backpressure, paces dispatch using a per-endpoint
//! GCRA schedule, and exposes predicted completion cost for P2C selection.
//!
//! The adapter keeps Tower-specific readiness, discovery, and load-balancing
//! concerns out of the runtime-independent `loadpace` crate.

#![forbid(unsafe_code)]

mod service;

pub use service::{AdaptiveDiscovery, AdaptiveEndpoint, AdaptiveLayer, LoadMetric, ResponseFuture};
