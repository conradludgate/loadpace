//! Rama integration for adaptive client-side load balancing and backpressure.
//!
//! Use this crate when Rama services need to balance requests across endpoints
//! with different or changing capacities. The adapter keeps admission bounded,
//! paces dispatch using each endpoint's learned operating point, and exposes
//! predicted completion cost for a higher-level balancer.
//!
//! The implementation fits Rama's direct `serve` model rather than relying on
//! Tower's readiness phase.
//!
//! Rama's [`Service`] trait has no `poll_ready` phase. This adapter therefore
//! reserves a slot synchronously when [`AdaptiveEndpoint::serve`] is called.
//! If the bounded scheduling horizon is full, the returned future resolves to
//! [`ServiceError::Rejected`]. Accepted requests retain their reservation
//! until they are dispatched or the returned future is dropped.
//!
//! # Deployment scope
//!
//! This crate is intended for trusted microservice clients sharing private
//! service endpoints. Its congestion-control and fairness behavior assumes
//! that the other clients are cooperative and run compatible control logic.
//! An unpaced or malicious peer can consume capacity without participating in
//! the feedback loop.
//!
//! Do not use Loadpace as the primary rate limit, quota, abuse-prevention
//! mechanism, or DDoS defense for a general-purpose public API. Those controls
//! must be enforced by the server or another trusted ingress boundary.
//!
//! # Where to start
//!
//! - Wrap one service with [`AdaptiveEndpoint`] or [`AdaptiveLayer`].
//! - Enable the `dns` feature and use `dns::AdaptiveDnsLayer` when connector
//!   targets should be resolved and balanced automatically.
//! - See the repository's
//!   [Rama how-to guide](https://github.com/conradludgate/loadpace/blob/main/docs/how-to/integrate-with-rama.md)
//!   and [adapter reference](https://github.com/conradludgate/loadpace/blob/main/docs/reference/rama.md)
//!   for complete integration details.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "dns")]
pub mod dns;

use loadpace::{
    ControllerSnapshot, DispatchReservation, DispatchState, EndpointConfig, EndpointController,
    InFlightRequest, Outcome, ScheduleError,
};
use rama::{Layer, Service};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::Notify;

/// A comparable predicted completion cost for endpoint selection.
///
/// Lower values are better. The value is measured in seconds from the moment
/// the metric was read and includes the endpoint's expected RTT.
#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
pub struct LoadMetric(pub f64);

impl LoadMetric {
    /// Returns the predicted completion cost in seconds.
    pub fn as_secs(self) -> f64 {
        self.0
    }
}

/// Errors produced by an [`AdaptiveEndpoint`].
#[derive(Debug)]
pub enum ServiceError<E> {
    /// The request could not enter the endpoint's bounded scheduling horizon.
    ///
    /// This is transient endpoint backpressure, not an endpoint health
    /// failure. A higher-level balancer should try another endpoint or
    /// propagate backpressure upstream rather than ejecting this endpoint.
    Rejected(ScheduleError),
    /// The wrapped Rama service returned an error.
    Inner(E),
}

impl<E> ServiceError<E> {
    /// Returns the scheduling error when admission was rejected.
    pub fn rejected(&self) -> Option<ScheduleError> {
        match self {
            Self::Rejected(error) => Some(*error),
            Self::Inner(_) => None,
        }
    }

    /// Returns the wrapped service error when the request was dispatched.
    pub fn inner(&self) -> Option<&E> {
        match self {
            Self::Rejected(_) => None,
            Self::Inner(error) => Some(error),
        }
    }
}

impl<E: fmt::Display> fmt::Display for ServiceError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(formatter, "request rejected: {error:?}"),
            Self::Inner(error) => write!(formatter, "inner service error: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for ServiceError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected(_) => None,
            Self::Inner(error) => Some(error),
        }
    }
}

struct Shared<S> {
    inner: S,
    endpoint: EndpointState,
}

pub(crate) struct EndpointState {
    controller: Mutex<EndpointController>,
    dispatch: Notify,
}

impl EndpointState {
    pub(crate) fn new(config: EndpointConfig, now: Instant) -> Self {
        Self {
            controller: Mutex::new(EndpointController::new(config, now)),
            dispatch: Notify::new(),
        }
    }
}

pub(crate) trait EndpointOwner: Send + Sync + 'static {
    fn endpoint_state(&self) -> &EndpointState;
}

impl<S: Send + Sync + 'static> EndpointOwner for Shared<S> {
    fn endpoint_state(&self) -> &EndpointState {
        &self.endpoint
    }
}

// Core deliberately uses `std::time::Instant`; Rama waits use Tokio's
// runtime clock. Converting here keeps controller deadlines and Tokio timers
// in the same clock domain, including when Tokio time is paused in tests.
pub(crate) fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("loadpace-rama internal mutex was poisoned")
}

/// A Rama service with per-endpoint adaptive pacing and bounded admission.
///
/// The wrapped service and controller are held in one shared allocation, so
/// concurrent requests can call the inner service when it supports concurrent
/// `serve` calls.
/// Requests wait in a bounded virtual scheduling horizon; dropping a returned
/// future cancels its reservation, or records a failure if it was dispatched.
pub struct AdaptiveEndpoint<S> {
    shared: Arc<Shared<S>>,
}

impl<S> AdaptiveEndpoint<S> {
    /// Wraps a Rama service using the current Tokio runtime time.
    ///
    /// # Panics
    ///
    /// Panics when `config` contains invalid controller settings.
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, now())
    }

    /// Wraps a Rama service using an explicit controller start time.
    ///
    /// This is useful for deterministic simulations and tests.
    ///
    /// # Panics
    ///
    /// Panics when `config` contains invalid controller settings.
    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        Self {
            shared: Arc::new(Shared {
                inner,
                endpoint: EndpointState::new(config, now),
            }),
        }
    }

    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changed) = {
            let mut controller = lock(&self.shared.endpoint.controller);
            let before = controller.active_probe();
            let result = operation(&mut controller);
            (result, before != controller.active_probe())
        };
        if changed {
            self.shared.endpoint.dispatch.notify_waiters();
        }
        result
    }

    /// Returns a current snapshot of this endpoint's controller.
    ///
    /// Reading a snapshot refreshes time-driven probe state and may update the
    /// effective pacing rate shared by all endpoint clones.
    pub fn snapshot(&self) -> ControllerSnapshot {
        self.with_controller(|controller| controller.snapshot(now()))
    }

    /// Returns the endpoint's predicted completion cost for load balancing.
    ///
    /// Lower values are preferred. Reading the metric refreshes time-driven
    /// controller state, including probes.
    pub fn load_metric(&self) -> LoadMetric {
        self.with_controller(|controller| {
            let current = now();
            LoadMetric(controller.load(current))
        })
    }
}

impl<S> Clone for AdaptiveEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<S, Request> Service<Request> for AdaptiveEndpoint<S>
where
    S: Service<Request> + Send + Sync + 'static,
    S::Output: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    type Output = S::Output;
    type Error = ServiceError<S::Error>;

    fn serve(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        let reservation = lock(&self.shared.endpoint.controller).reserve(now());

        match reservation {
            Ok(reservation) => {
                let guard = RequestGuard::new(Arc::clone(&self.shared), reservation);
                Box::pin(dispatch_request(
                    Arc::clone(&self.shared),
                    request,
                    reservation,
                    guard,
                ))
                    as Pin<Box<dyn Future<Output = Result<S::Output, Self::Error>> + Send>>
            }
            Err(error) => Box::pin(async move { Err(ServiceError::Rejected(error)) })
                as Pin<Box<dyn Future<Output = Result<S::Output, Self::Error>> + Send>>,
        }
    }
}

/// A Rama layer that wraps each inner service in an [`AdaptiveEndpoint`].
#[derive(Clone, Debug)]
pub struct AdaptiveLayer {
    config: EndpointConfig,
}

impl AdaptiveLayer {
    /// Creates a layer with the supplied endpoint configuration.
    pub fn new(config: EndpointConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration cloned into each wrapped endpoint.
    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }
}

impl Default for AdaptiveLayer {
    fn default() -> Self {
        Self::new(EndpointConfig::default())
    }
}

impl<S> Layer<S> for AdaptiveLayer {
    type Service = AdaptiveEndpoint<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AdaptiveEndpoint::new(inner, self.config.clone())
    }
}

enum RequestState {
    Reserved(DispatchReservation),
    Dispatched(InFlightRequest),
    Finished,
}

pub(crate) struct RequestGuard<O: EndpointOwner> {
    owner: Arc<O>,
    state: RequestState,
}

impl<O: EndpointOwner> RequestGuard<O> {
    pub(crate) fn new(owner: Arc<O>, reservation: DispatchReservation) -> Self {
        Self {
            owner,
            state: RequestState::Reserved(reservation),
        }
    }

    pub(crate) fn mark_dispatched(&mut self, active: InFlightRequest) {
        self.state = RequestState::Dispatched(active);
    }

    pub(crate) fn finish(&mut self, outcome: Outcome, now: Instant) {
        let endpoint = self.owner.endpoint_state();
        match std::mem::replace(&mut self.state, RequestState::Finished) {
            RequestState::Dispatched(active) => {
                let latency = now.saturating_duration_since(active.dispatched_at());
                lock(&endpoint.controller).on_complete(active, outcome, latency, now);
            }
            RequestState::Reserved(reservation) => {
                lock(&endpoint.controller).cancel(reservation, now);
            }
            RequestState::Finished => return,
        }
        endpoint.dispatch.notify_waiters();
    }

    fn abandon(&mut self, now: Instant) {
        let endpoint = self.owner.endpoint_state();
        match std::mem::replace(&mut self.state, RequestState::Finished) {
            RequestState::Dispatched(active) => {
                lock(&endpoint.controller).on_abandoned(active, now);
            }
            RequestState::Reserved(reservation) => {
                lock(&endpoint.controller).cancel(reservation, now);
            }
            RequestState::Finished => return,
        }
        endpoint.dispatch.notify_waiters();
    }
}

impl<O: EndpointOwner> Drop for RequestGuard<O> {
    fn drop(&mut self) {
        self.abandon(now());
    }
}

fn dispatch_state(
    endpoint: &EndpointState,
    reservation: DispatchReservation,
    current: Instant,
) -> DispatchState {
    let (state, probe_changed) = {
        let mut controller = lock(&endpoint.controller);
        let previous_probe = controller.active_probe();
        let state = controller.dispatch_state(reservation, current);
        (state, previous_probe != controller.active_probe())
    };

    // Wake waiters after releasing the controller lock so they observe the
    // complete state transition when they re-check their reservations.
    if probe_changed {
        endpoint.dispatch.notify_waiters();
    }
    state
}

pub(crate) async fn wait_for_dispatch(endpoint: &EndpointState, reservation: DispatchReservation) {
    loop {
        let notified = endpoint.dispatch.notified();
        let mut notified = std::pin::pin!(notified);
        // Register before inspecting controller state. Otherwise a
        // notification between the state check and the first poll could be
        // lost, leaving this request asleep indefinitely.
        notified.as_mut().enable();

        match dispatch_state(endpoint, reservation, now()) {
            DispatchState::Ready => return,
            DispatchState::WaitUntil(deadline) => {
                let delay = deadline.saturating_duration_since(now());
                let _ = tokio::time::timeout(delay, notified.as_mut()).await;
            }
            DispatchState::WaitForPrevious | DispatchState::InflightLimit => {
                notified.as_mut().await;
            }
            DispatchState::Cancelled => {
                unreachable!("a live request reservation cannot be cancelled externally");
            }
        }
    }
}

pub(crate) async fn begin_dispatch(
    endpoint: &EndpointState,
    reservation: DispatchReservation,
) -> InFlightRequest {
    loop {
        wait_for_dispatch(endpoint, reservation).await;
        let dispatch = lock(&endpoint.controller).on_dispatched(reservation, now());
        if let Ok(active) = dispatch {
            // Removing the FIFO head can make the next reservation eligible.
            endpoint.dispatch.notify_waiters();
            return active;
        }
    }
}

async fn dispatch_request<S, Request>(
    shared: Arc<Shared<S>>,
    request: Request,
    reservation: DispatchReservation,
    mut guard: RequestGuard<Shared<S>>,
) -> Result<S::Output, ServiceError<S::Error>>
where
    S: Service<Request> + Send + Sync + 'static,
    S::Output: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    let active = begin_dispatch(&shared.endpoint, reservation).await;
    guard.mark_dispatched(active);

    let result = shared
        .inner
        .serve(request)
        .await
        .map_err(ServiceError::Inner);
    let outcome = if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    };
    guard.finish(outcome, now());
    result
}
