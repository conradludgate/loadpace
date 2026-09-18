use crate::ServiceError;
use futures_core::stream::{Stream, TryStream};
use loadpace::{
    Completion, DispatchReservation, DispatchState, EndpointConfig, EndpointController,
    InFlightRequest,
};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use tower::discover::Change;
use tower::load::Load;
use tower::{BoxError, Layer, Service};

/// A comparable predicted completion cost for P2C selection.
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

struct Shared<S> {
    service: tokio::sync::Mutex<S>,
    controller: Mutex<ControllerState>,
    dispatch: tokio::sync::Notify,
}

impl<S> Shared<S> {
    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changes, admission_waker) = {
            let mut state = lock(&self.controller);
            let result = operation(&mut state.controller);
            let changes = state.controller.take_changes();
            let admission_waker = if changes.admission {
                state.take_admission_waker()
            } else {
                None
            };
            (result, changes, admission_waker)
        };
        if let Some(waker) = admission_waker {
            waker.wake();
        }
        if changes.dispatch {
            self.dispatch.notify_waiters();
        }
        result
    }
}

struct ControllerState {
    controller: EndpointController,
    admission_waker: Option<Waker>,
    failed: Option<ServiceError>,
}

impl ControllerState {
    fn poll_admission(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.controller.may_schedule() {
            self.admission_waker = None;
            return Poll::Ready(());
        }

        if self
            .admission_waker
            .as_ref()
            .is_none_or(|waker| !waker.will_wake(cx.waker()))
        {
            self.admission_waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    fn take_admission_waker(&mut self) -> Option<Waker> {
        self.admission_waker.take()
    }
}

// Core deliberately uses `std::time::Instant`; Tower waits use Tokio's
// runtime clock. Converting here keeps controller deadlines and Tokio timers
// in the same clock domain, including when Tokio time is paused in tests.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("loadpace-tower internal mutex was poisoned")
}

/// A Tower service with per-endpoint adaptive pacing and bounded admission.
///
/// `poll_ready` reports whether another request can enter the endpoint's
/// bounded scheduling horizon. `call` reserves a virtual GCRA slot; the
/// returned future waits until that slot is due, waits for the inner service to
/// be ready, and only then records the actual dispatch. Queue delay therefore
/// never contaminates the RTT sample.
///
/// Errors are returned as [`BoxError`]. An inner readiness error permanently
/// closes admission and is shared as a [`ServiceError`] by queued requests and
/// subsequent readiness checks. Already dispatched requests finish normally;
/// errors from their response futures do not close admission.
///
/// The endpoint is intentionally single-owner: Tower's P2C balancer owns one
/// service per discovered backend and does not require endpoint services to be
/// cloneable. This lets readiness use the controller's queue state directly,
/// without a second shared allocation for coordinating cloned handles.
pub struct AdaptiveEndpoint<S> {
    shared: Arc<Shared<S>>,
}

impl<S> AdaptiveEndpoint<S> {
    /// Wraps a Tower endpoint using the current Tokio runtime time.
    ///
    /// The returned service starts with an immediately available pacing slot.
    ///
    /// # Panics
    ///
    /// Panics when `config` contains invalid controller settings.
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, now())
    }

    /// Wraps a Tower endpoint using an explicit controller start time.
    ///
    /// This constructor is useful in deterministic tests. Production code
    /// should normally use [`Self::new`] so Tokio and controller deadlines
    /// share the runtime's clock domain.
    ///
    /// # Panics
    ///
    /// Panics when `config` contains invalid controller settings.
    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        Self {
            shared: Arc::new(Shared {
                service: tokio::sync::Mutex::new(inner),
                controller: Mutex::new(ControllerState {
                    controller: EndpointController::new(config, now),
                    admission_waker: None,
                    failed: None,
                }),
                dispatch: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Returns a current snapshot of this endpoint's controller.
    ///
    /// Reading a snapshot refreshes time-driven probe state and may update the
    /// effective pacing rate.
    pub fn snapshot(&self) -> loadpace::ControllerSnapshot {
        self.shared
            .with_controller(|controller| controller.snapshot(now()))
    }

    /// Returns the endpoint's predicted completion cost for load balancing.
    ///
    /// Lower values are preferred. Reading the metric refreshes time-driven
    /// controller state, including probes.
    pub fn load_metric(&self) -> LoadMetric {
        self.shared.with_controller(|controller| {
            let current = now();
            LoadMetric(controller.load(current))
        })
    }
}

/// A Tower layer that wraps a service in an [`AdaptiveEndpoint`].
#[derive(Clone, Debug)]
pub struct AdaptiveLayer {
    config: EndpointConfig,
}

impl AdaptiveLayer {
    /// Creates a layer using the supplied endpoint configuration.
    pub fn new(config: EndpointConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration cloned into wrapped services.
    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }
}

impl<S> Layer<S> for AdaptiveLayer {
    type Service = AdaptiveEndpoint<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AdaptiveEndpoint::new(inner, self.config.clone())
    }
}

pin_project! {
    #[doc = "Maps a Tower discovery stream into freshly initialized adaptive endpoints."]
    #[doc = ""]
    #[doc = "The wrapper intentionally creates new controller state for every insert."]
    #[doc = "This is the safe behavior when discovery removes and later reuses an"]
    #[doc = "endpoint key; state retention can be added without changing the discovery"]
    #[doc = "contract once churn behavior is better understood."]
    pub struct AdaptiveDiscovery<D> {
        #[pin]
        inner: D,
        config: EndpointConfig,
    }
}

impl<D> AdaptiveDiscovery<D> {
    /// Wraps a discovery stream and clones `config` into every inserted service.
    pub fn new(inner: D, config: EndpointConfig) -> Self {
        Self { inner, config }
    }

    /// Consumes the wrapper and returns the original discovery stream.
    pub fn into_inner(self) -> D {
        self.inner
    }
}

impl<D, K, S> Stream for AdaptiveDiscovery<D>
where
    D: TryStream<Ok = Change<K, S>>,
{
    type Item = Result<Change<K, AdaptiveEndpoint<S>>, D::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.try_poll_next(cx).map(|change| {
            change.map(|result| {
                result.map(|change| match change {
                    Change::Insert(key, service) => {
                        Change::Insert(key, AdaptiveEndpoint::new(service, this.config.clone()))
                    }
                    Change::Remove(key) => Change::Remove(key),
                })
            })
        })
    }
}

impl<S> Load for AdaptiveEndpoint<S> {
    type Metric = LoadMetric;

    fn load(&self) -> Self::Metric {
        self.load_metric()
    }
}

impl<S, Request> Service<Request> for AdaptiveEndpoint<S>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = ResponseFuture<S::Response, BoxError>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = lock(&self.shared.controller);
        if let Some(error) = &state.failed {
            return Poll::Ready(Err(error.clone().into()));
        }
        match state.poll_admission(cx) {
            Poll::Ready(()) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // With no endpoint clones, nothing can add another reservation between
        // this service reporting readiness and receiving the corresponding
        // call. Tower permits a panic if callers skip `poll_ready`.
        let reservation = {
            let mut state = lock(&self.shared.controller);
            // An inner readiness failure can arrive after outer readiness was
            // granted, so `call` must observe closure before reserving a slot.
            if let Some(error) = state.failed.clone() {
                return ResponseFuture {
                    inner: Box::pin(async move { Err(error.into()) }),
                };
            }
            state.controller.reserve(now())
        };
        let reservation =
            reservation.expect("AdaptiveEndpoint::call invoked without available readiness");

        let pending = PendingRequest::new(Arc::clone(&self.shared), reservation);
        ResponseFuture {
            inner: Box::pin(pending.execute(request)),
        }
    }
}

/// The future returned by [`AdaptiveEndpoint::call`].
pub struct ResponseFuture<T, E> {
    inner: Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>,
}

impl<T, E> Future for ResponseFuture<T, E> {
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(cx)
    }
}

enum RequestState {
    Queued(DispatchReservation),
    InFlight(InFlightRequest),
    Finished,
}

struct PendingRequest<S> {
    shared: Arc<Shared<S>>,
    state: RequestState,
}

impl<S> PendingRequest<S> {
    fn new(shared: Arc<Shared<S>>, reservation: DispatchReservation) -> Self {
        Self {
            shared,
            state: RequestState::Queued(reservation),
        }
    }

    fn reservation(&self) -> DispatchReservation {
        match &self.state {
            RequestState::Queued(reservation) => *reservation,
            RequestState::InFlight(_) | RequestState::Finished => {
                panic!("only a queued request has a dispatch reservation")
            }
        }
    }

    fn mark_dispatched(&mut self, active: InFlightRequest) {
        // The controller has already removed the request from its virtual
        // queue, so the next admitted caller observes the updated capacity.
        self.state = RequestState::InFlight(active);
    }

    fn finish(&mut self, completion: Completion, now: Instant) {
        match std::mem::replace(&mut self.state, RequestState::Finished) {
            RequestState::InFlight(active) => {
                self.shared
                    .with_controller(|controller| controller.finish(active, completion, now));
            }
            RequestState::Queued(reservation) => {
                self.shared
                    .with_controller(|controller| controller.cancel(reservation, now));
            }
            RequestState::Finished => {}
        }
    }

    async fn wait_until_dispatchable(&self) -> Result<(), ServiceError> {
        let reservation = self.reservation();
        loop {
            let notified = self.shared.dispatch.notified();
            let mut notified = std::pin::pin!(notified);
            // Register before inspecting controller state so a transition
            // between the check and the await cannot be lost.
            notified.as_mut().enable();

            let state = {
                let mut shared_state = lock(&self.shared.controller);
                if let Some(error) = &shared_state.failed {
                    return Err(error.clone());
                }
                let state = shared_state.controller.dispatch_state(reservation, now());
                // The FIFO head already observes its refreshed deadline;
                // waking itself on each rate change would cause a busy loop.
                let _ = shared_state.controller.take_changes();
                state
            };

            match state {
                DispatchState::Ready => return Ok(()),
                DispatchState::WaitUntil(deadline) => {
                    let delay = deadline.saturating_duration_since(now());
                    let _ = tokio::time::timeout(delay, notified.as_mut()).await;
                }
                DispatchState::WaitForPrevious | DispatchState::InflightLimit => {
                    notified.as_mut().await;
                }
                DispatchState::Cancelled => {
                    panic!("a live request reservation was cancelled externally")
                }
            }
        }
    }

    async fn start<Request>(&mut self, request: Request) -> Result<S::Future, BoxError>
    where
        S: Service<Request>,
        S::Error: Into<BoxError>,
    {
        let shared = Arc::clone(&self.shared);
        let mut service = shared.service.lock().await;
        if let Some(error) = lock(&shared.controller).failed.clone() {
            return Err(error.into());
        }
        if let Err(error) = std::future::poll_fn(|cx| service.poll_ready(cx)).await {
            let error = ServiceError::new(error.into());
            // Publish closure while holding the service lock so no queued
            // caller can poll the failed service again, then wake all waiters.
            let admission_waker = {
                let mut state = lock(&shared.controller);
                state.failed = Some(error.clone());
                state.controller.on_admission_failure(now());
                let _ = state.controller.take_changes();
                state.take_admission_waker()
            };
            if let Some(waker) = admission_waker {
                waker.wake();
            }
            shared.dispatch.notify_waiters();
            return Err(error.into());
        }

        // Controller state may have changed while the inner service was
        // becoming ready. Keep its readiness claim and wait for the revised
        // pacing deadline instead of assuming the earlier decision is stable.
        loop {
            let dispatch = self
                .shared
                .with_controller(|controller| controller.on_dispatched(self.reservation(), now()));
            match dispatch {
                Ok(active) => {
                    self.mark_dispatched(active);
                    break;
                }
                Err(_) => self.wait_until_dispatchable().await?,
            }
        }
        Ok(service.call(request))
    }

    async fn execute<Request>(mut self, request: Request) -> Result<S::Response, BoxError>
    where
        S: Service<Request>,
        S::Error: Into<BoxError>,
    {
        self.wait_until_dispatchable().await?;

        let response = match self.start(request).await {
            Ok(response) => response,
            Err(error) => {
                self.finish(Completion::Failure, now());
                return Err(error);
            }
        };
        let response = response.await;
        let outcome = if response.is_ok() {
            Completion::Success
        } else {
            Completion::Failure
        };
        self.finish(outcome, now());
        response.map_err(Into::into)
    }
}

impl<S> Drop for PendingRequest<S> {
    fn drop(&mut self) {
        self.finish(Completion::Abandoned, now());
    }
}
