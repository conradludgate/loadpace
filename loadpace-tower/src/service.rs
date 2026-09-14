use futures_core::stream::{Stream, TryStream};
use loadpace::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tokio_util::sync::PollSemaphore;
use tower::discover::Change;
use tower::load::Load;
use tower::{Layer, Service};

/// A comparable predicted completion cost for P2C selection.
///
/// Lower values are better. The value is measured in seconds from the moment
/// the metric was read and includes the endpoint's expected RTT.
#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
pub struct LoadMetric(pub f64);

impl LoadMetric {
    pub fn as_secs(self) -> f64 {
        self.0
    }
}

struct Shared<S> {
    service: tokio::sync::Mutex<S>,
    controller: Mutex<EndpointController>,
    dispatch: tokio::sync::Notify,
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
pub struct AdaptiveEndpoint<S> {
    shared: Arc<Shared<S>>,
    admission_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    admission: PollSemaphore,
}

impl<S> AdaptiveEndpoint<S> {
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, now())
    }

    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        let queue_capacity = config.queue_capacity;
        Self {
            shared: Arc::new(Shared {
                service: tokio::sync::Mutex::new(inner),
                controller: Mutex::new(EndpointController::new(config, now)),
                dispatch: tokio::sync::Notify::new(),
            }),
            admission_permit: None,
            admission: PollSemaphore::new(Arc::new(tokio::sync::Semaphore::new(queue_capacity))),
        }
    }

    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changed) = {
            let mut controller = lock(&self.shared.controller);
            let before = controller.active_probe();
            let result = operation(&mut controller);
            (result, before != controller.active_probe())
        };
        if changed {
            self.shared.dispatch.notify_waiters();
        }
        result
    }

    pub fn snapshot(&self) -> loadpace::ControllerSnapshot {
        self.with_controller(|controller| controller.snapshot(now()))
    }

    pub fn load_metric(&self) -> LoadMetric {
        self.with_controller(|controller| {
            let current = now();
            controller.refresh(current);
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
    pub fn new(inner: D, config: EndpointConfig) -> Self {
        Self { inner, config }
    }

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

impl<S> Clone for AdaptiveEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            admission_permit: None,
            // `PollSemaphore::clone` resets the per-handle acquire future but
            // retains the same underlying `Arc<Semaphore>`.
            admission: self.admission.clone(),
        }
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
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.admission_permit.is_some() {
            return Poll::Ready(Ok(()));
        }

        // The adapter owns every handle to this semaphore and never closes
        // it, so closure would mean an internal invariant was violated.
        let permit = ready!(self.admission.poll_acquire(cx))
            .expect("loadpace-tower admission semaphore was closed");
        self.admission_permit = Some(permit);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Tower permits services to panic when `call` does not follow a
        // successful `poll_ready` invocation.
        let admission_permit = self
            .admission_permit
            .take()
            .expect("AdaptiveEndpoint::call invoked without available readiness");
        let reservation = lock(&self.shared.controller).reserve(now());
        let reservation =
            reservation.expect("admission permit was not reflected in controller queue capacity");

        let pending = PendingRequest::new(Arc::clone(&self.shared), reservation, admission_permit);
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
    Queued {
        reservation: DispatchReservation,
        admission_permit: tokio::sync::OwnedSemaphorePermit,
    },
    InFlight(InFlightRequest),
    Finished,
}

struct PendingRequest<S> {
    shared: Arc<Shared<S>>,
    state: RequestState,
}

impl<S> PendingRequest<S> {
    fn new(
        shared: Arc<Shared<S>>,
        reservation: DispatchReservation,
        admission_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            shared,
            state: RequestState::Queued {
                reservation,
                admission_permit,
            },
        }
    }

    fn reservation(&self) -> DispatchReservation {
        match &self.state {
            RequestState::Queued { reservation, .. } => *reservation,
            RequestState::InFlight(_) | RequestState::Finished => {
                panic!("only a queued request has a dispatch reservation")
            }
        }
    }

    fn mark_dispatched(&mut self, active: InFlightRequest) {
        // Replacing the queued state drops its admission permit. The
        // controller has already removed the request from its virtual queue,
        // so the next admitted caller observes the updated capacity.
        self.state = RequestState::InFlight(active);
    }

    fn finish(&mut self, outcome: Outcome, now: Instant) {
        match std::mem::replace(&mut self.state, RequestState::Finished) {
            RequestState::InFlight(active) => {
                let latency = now.saturating_duration_since(active.dispatched_at());
                lock(&self.shared.controller).on_complete(active, outcome, latency, now);
            }
            RequestState::Queued {
                reservation,
                admission_permit,
            } => {
                lock(&self.shared.controller).cancel(reservation, now);
                // Keep the permit until after cancellation updates the
                // controller's queue count.
                drop(admission_permit);
            }
            RequestState::Finished => return,
        }
        self.shared.dispatch.notify_waiters();
    }

    async fn wait_until_dispatchable(&self) {
        let reservation = self.reservation();
        loop {
            let notified = self.shared.dispatch.notified();
            let mut notified = std::pin::pin!(notified);
            // Register before inspecting controller state so a transition
            // between the check and the await cannot be lost.
            notified.as_mut().enable();

            let (state, probe_changed) = {
                let mut controller = lock(&self.shared.controller);
                let previous_probe = controller.active_probe();
                let state = controller.dispatch_state(reservation, now());
                (state, previous_probe != controller.active_probe())
            };
            if probe_changed {
                self.shared.dispatch.notify_waiters();
            }

            match state {
                DispatchState::Ready => return,
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

    async fn start<Request>(&mut self, request: Request) -> Result<S::Future, S::Error>
    where
        S: Service<Request>,
    {
        let shared = Arc::clone(&self.shared);
        let mut service = shared.service.lock().await;
        std::future::poll_fn(|cx| service.poll_ready(cx)).await?;

        // Controller state may have changed while the inner service was
        // becoming ready. Keep its readiness claim and wait for the revised
        // pacing deadline instead of assuming the earlier decision is stable.
        loop {
            let dispatch = {
                let mut controller = lock(&self.shared.controller);
                controller.on_dispatched(self.reservation(), now())
            };
            match dispatch {
                Ok(active) => {
                    self.mark_dispatched(active);
                    break;
                }
                Err(_) => self.wait_until_dispatchable().await,
            }
        }
        // Committing the FIFO head changes the next reservation's deadline.
        self.shared.dispatch.notify_waiters();

        Ok(service.call(request))
    }

    async fn execute<Request>(mut self, request: Request) -> Result<S::Response, S::Error>
    where
        S: Service<Request>,
    {
        self.wait_until_dispatchable().await;

        let response = match self.start(request).await {
            Ok(response) => response,
            Err(error) => {
                lock(&self.shared.controller).on_admission_failure(now());
                self.finish(Outcome::Failure, now());
                return Err(error);
            }
        };
        let response = response.await;
        let outcome = if response.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        };
        self.finish(outcome, now());
        response
    }
}

impl<S> Drop for PendingRequest<S> {
    fn drop(&mut self) {
        self.finish(Outcome::Failure, now());
    }
}
