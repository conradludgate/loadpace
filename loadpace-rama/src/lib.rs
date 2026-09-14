//! Rama integration for the [`loadpace`] adaptive controller.
//!
//! Rama's [`Service`] trait has no `poll_ready` phase. This adapter therefore
//! reserves a slot synchronously when [`AdaptiveEndpoint::serve`] is called.
//! If the bounded scheduling horizon is full, the returned future resolves to
//! [`ServiceError::Rejected`]. Accepted requests retain their reservation
//! until they are dispatched or the returned future is dropped.

#![forbid(unsafe_code)]

use loadpace::{
    ControllerSnapshot, DispatchReservation, DispatchState, EndpointConfig, EndpointController,
    InFlightRequest, Outcome, Probe, ProbeSchedule, ScheduleError,
};
use rama::{Layer, Service};
use rand::Rng;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
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
    inner: Arc<S>,
    controller: Mutex<EndpointController>,
    dispatch: Notify,
}

// Core deliberately uses `std::time::Instant`; Rama waits use Tokio's
// runtime clock. Converting here keeps controller deadlines and Tokio timers
// in the same clock domain, including when Tokio time is paused in tests.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// A Rama service with per-endpoint adaptive pacing and bounded admission.
///
/// The wrapped service is shared through an [`Arc`], so concurrent requests
/// can be served when the inner service supports concurrent `serve` calls.
/// Requests wait in a bounded virtual scheduling horizon; dropping a returned
/// future cancels its reservation, or records a failure if it was dispatched.
pub struct AdaptiveEndpoint<S> {
    shared: Arc<Shared<S>>,
}

impl<S> AdaptiveEndpoint<S> {
    /// Wraps a Rama service using the current Tokio runtime time.
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, now())
    }

    /// Wraps a Rama service using an explicit controller start time.
    ///
    /// This is useful for deterministic simulations and tests.
    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        Self {
            shared: Arc::new(Shared {
                inner: Arc::new(inner),
                controller: Mutex::new(EndpointController::new(config, now)),
                dispatch: Notify::new(),
            }),
        }
    }

    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changed) = {
            let mut controller = self
                .shared
                .controller
                .lock()
                .expect("controller mutex poisoned");
            let before = controller.probe().current();
            let result = operation(&mut controller);
            (result, before != controller.probe().current())
        };
        if changed {
            self.shared.dispatch.notify_waiters();
        }
        result
    }

    /// Returns a current snapshot of this endpoint's controller.
    pub fn snapshot(&self) -> ControllerSnapshot {
        self.with_controller(|controller| controller.snapshot(now()))
    }

    /// Starts a temporary positive concurrency probe.
    pub fn start_positive_probe(&self, delta: f64, until: Instant) {
        self.with_controller(|controller| {
            controller.start_positive_probe(delta, until, now());
        });
    }

    /// Starts a temporary negative concurrency probe.
    pub fn start_negative_probe(&self, factor: f64, until: Instant) {
        self.with_controller(|controller| {
            controller.start_negative_probe(factor, until, now());
        });
    }

    /// Gives a caller-provided RNG a time-gated chance to start a probe.
    ///
    /// The method is intentionally caller-driven: applications can choose
    /// where to run the check and simulations can provide deterministic RNGs.
    pub fn maybe_start_probe<R: Rng + ?Sized>(
        &self,
        schedule: &ProbeSchedule,
        rng: &mut R,
    ) -> Option<Probe> {
        self.with_controller(|controller| controller.maybe_start_probe(schedule, rng, now()))
    }

    /// Returns the endpoint's predicted completion cost for load balancing.
    pub fn load_metric(&self) -> LoadMetric {
        self.with_controller(|controller| {
            let current = now();
            controller.refresh(current);
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
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send + 'static {
        let reservation = self
            .shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .reserve(now());

        match reservation {
            Ok(reservation) => {
                let guard = RequestGuard::new(Arc::clone(&self.shared), reservation);
                Box::pin(dispatch_request(Arc::clone(&self.shared), request, guard))
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

struct RequestGuard<S> {
    shared: Arc<Shared<S>>,
    reservation: Option<DispatchReservation>,
    active: Option<InFlightRequest>,
    finished: bool,
}

impl<S> RequestGuard<S> {
    fn new(shared: Arc<Shared<S>>, reservation: DispatchReservation) -> Self {
        Self {
            shared,
            reservation: Some(reservation),
            active: None,
            finished: false,
        }
    }

    fn mark_dispatched(&mut self, active: InFlightRequest) {
        self.reservation = None;
        self.active = Some(active);
    }

    fn finish(&mut self, outcome: Outcome, now: Instant) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(active) = self.active.take() {
            let latency = now.saturating_duration_since(active.dispatched_at());
            self.shared
                .controller
                .lock()
                .expect("controller mutex poisoned")
                .on_complete(active, outcome, latency, now);
        } else if let Some(reservation) = self.reservation.take() {
            self.shared
                .controller
                .lock()
                .expect("controller mutex poisoned")
                .cancel(reservation, now);
        }
        self.shared.dispatch.notify_waiters();
    }
}

impl<S> Drop for RequestGuard<S> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Outcome::Failure, now());
        }
    }
}

async fn dispatch_request<S, Request>(
    shared: Arc<Shared<S>>,
    request: Request,
    mut guard: RequestGuard<S>,
) -> Result<S::Output, ServiceError<S::Error>>
where
    S: Service<Request> + Send + Sync + 'static,
    S::Output: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    let reservation = guard
        .reservation
        .expect("request guard must begin with a reservation");

    loop {
        let notified = shared.dispatch.notified();
        let (decision, probe_until) = {
            let mut controller = shared.controller.lock().expect("controller mutex poisoned");
            let current = now();
            controller.refresh(current);
            (
                controller.dispatch_state(reservation, current),
                controller.probe().current().map(|probe| probe.until),
            )
        };

        match decision {
            DispatchState::Ready => break,
            DispatchState::WaitUntil(deadline) => {
                let wake_at = probe_until.map_or(deadline, |until| deadline.min(until));
                let delay = wake_at.saturating_duration_since(now());
                let _ = tokio::time::timeout(delay, notified).await;
            }
            DispatchState::WaitForPrevious | DispatchState::InflightLimit => {
                notified.await;
            }
            DispatchState::Cancelled => {
                panic!("an AdaptiveEndpoint request was cancelled while being polled");
            }
        }
    }

    let current = now();
    let active = shared
        .controller
        .lock()
        .expect("controller mutex poisoned")
        .on_dispatched(reservation, current)
        .expect("dispatch state changed unexpectedly");
    guard.mark_dispatched(active);
    // Removing the FIFO head can make the next reservation eligible. Wake it
    // even when its old timer has not elapsed because the committed TAT may
    // have changed its effective deadline.
    shared.dispatch.notify_waiters();

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
