//! Tower integration for adaptive client-side load balancing and backpressure.
//!
//! Use this crate when a Tower client balances requests across a changing fleet
//! and needs each endpoint to adapt to its own capacity and latency. The
//! adapter preserves Tower's backpressure, paces dispatch using a per-endpoint
//! GCRA schedule, and exposes predicted completion cost for P2C selection.
//!
//! The adapter keeps Tower-specific readiness, discovery, and load-balancing
//! concerns out of the runtime-independent `loadpace` crate.
//!
//! # Deployment scope
//!
//! This crate is intended for trusted microservice clients sharing private
//! service endpoints. Its congestion-control and fairness behavior assumes
//! that the other clients are cooperative and run compatible control logic.
//! It cannot protect paced clients from an unpaced or malicious peer.
//!
//! Do not use it as the primary rate limit, quota, abuse-prevention mechanism,
//! or DDoS defense for a general-purpose public API. Enforce those policies at
//! the server or another trusted ingress boundary.
//!
//! # Where to start
//!
//! - Wrap one service with [`AdaptiveEndpoint`] or [`AdaptiveLayer`].
//! - Wrap a dynamic discovery stream with [`AdaptiveDiscovery`] and pass it to
//!   `tower::balance::p2c::Balance`.
//! - See the repository's
//!   [Tower how-to guide](https://github.com/conradludgate/loadpace/blob/main/docs/how-to/integrate-with-tower.md)
//!   and [adapter reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/tower.md)
//!   for complete integration details.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod service;

pub use service::{AdaptiveDiscovery, AdaptiveEndpoint, AdaptiveLayer, LoadMetric, ResponseFuture};
