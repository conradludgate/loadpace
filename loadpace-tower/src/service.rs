use futures_core::stream::{Stream, TryStream};
use loadpace::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use rand::Rng;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tower::Service;
use tower::discover::Change;
use tower::load::Load;

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
    inner: tokio::sync::Mutex<S>,
    controller: Mutex<EndpointController>,
    ready: Arc<tokio::sync::Semaphore>,
    dispatch: tokio::sync::Notify,
}

type ReadinessFuture = Pin<
    Box<
        dyn Future<Output = Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError>>
            + Send
            + 'static,
    >,
>;

/// A Tower service with per-endpoint adaptive pacing and bounded admission.
///
/// `poll_ready` reports whether another request can enter the endpoint's
/// bounded scheduling horizon. `call` reserves a virtual GCRA slot; the
/// returned future waits until that slot is due, waits for the inner service to
/// be ready, and only then records the actual dispatch. Queue delay therefore
/// never contaminates the RTT sample.
pub struct AdaptiveEndpoint<S> {
    shared: Arc<Shared<S>>,
    readiness_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    readiness: Mutex<Option<ReadinessFuture>>,
}

impl<S> AdaptiveEndpoint<S> {
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, Instant::now())
    }

    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        let queue_capacity = config.queue_capacity;
        Self {
            shared: Arc::new(Shared {
                inner: tokio::sync::Mutex::new(inner),
                controller: Mutex::new(EndpointController::new(config, now)),
                ready: Arc::new(tokio::sync::Semaphore::new(queue_capacity)),
                dispatch: tokio::sync::Notify::new(),
            }),
            readiness_permit: None,
            readiness: Mutex::new(None),
        }
    }

    pub fn snapshot(&self) -> loadpace::ControllerSnapshot {
        self.shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .snapshot(Instant::now())
    }

    pub fn start_positive_probe(&self, delta: f64, until: Instant) {
        let now = Instant::now();
        self.shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .start_positive_probe(delta, until, now);
    }

    pub fn start_negative_probe(&self, factor: f64, until: Instant) {
        let now = Instant::now();
        self.shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .start_negative_probe(factor, until, now);
    }

    /// Gives a caller-provided RNG a time-gated chance to start a probe.
    ///
    /// The method is intentionally caller-driven: applications can choose
    /// where to run the check and simulations can provide deterministic RNGs.
    pub fn maybe_start_probe<R: Rng + ?Sized>(
        &self,
        schedule: &loadpace::ProbeSchedule,
        rng: &mut R,
    ) -> Option<loadpace::Probe> {
        let now = Instant::now();
        self.shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .maybe_start_probe(schedule, rng, now)
    }

    pub fn load_metric(&self) -> LoadMetric {
        let mut controller = self
            .shared
            .controller
            .lock()
            .expect("controller mutex poisoned");
        let now = Instant::now();
        controller.refresh(now);
        LoadMetric(controller.load(now))
    }
}

/// Maps a Tower discovery stream into freshly initialized adaptive endpoints.
///
/// The wrapper intentionally creates new controller state for every insert.
/// This is the safe behavior when discovery removes and later reuses an
/// endpoint key; state retention can be added without changing the discovery
/// contract once churn behavior is better understood.
pub struct AdaptiveDiscovery<D, Request> {
    inner: D,
    config: EndpointConfig,
    _request: PhantomData<fn() -> Request>,
}

impl<D, Request> AdaptiveDiscovery<D, Request> {
    pub fn new(inner: D, config: EndpointConfig) -> Self {
        Self {
            inner,
            config,
            _request: PhantomData,
        }
    }

    pub fn into_inner(self) -> D {
        self.inner
    }
}

impl<D, Request, K, S> Stream for AdaptiveDiscovery<D, Request>
where
    D: TryStream<Ok = Change<K, S>> + Unpin,
    K: Eq,
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: 'static,
{
    type Item = Result<Change<K, AdaptiveEndpoint<S>>, D::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).try_poll_next(cx).map(|change| {
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
            readiness_permit: None,
            readiness: Mutex::new(None),
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
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.readiness_permit.is_some() {
            return Poll::Ready(Ok(()));
        }

        let mut readiness = self
            .readiness
            .lock()
            .expect("readiness future mutex poisoned");
        if readiness.is_none() {
            match Arc::clone(&self.shared.ready).try_acquire_owned() {
                Ok(permit) => {
                    drop(readiness);
                    self.readiness_permit = Some(permit);
                    return Poll::Ready(Ok(()));
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    *readiness = Some(Box::pin(Arc::clone(&self.shared.ready).acquire_owned()));
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    panic!("AdaptiveEndpoint readiness semaphore was closed")
                }
            }
        }

        match readiness
            .as_mut()
            .expect("readiness future must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(Ok(permit)) => {
                *readiness = None;
                self.readiness_permit = Some(permit);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                panic!("AdaptiveEndpoint readiness semaphore was closed")
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        assert!(
            self.readiness_permit.is_some(),
            "AdaptiveEndpoint::call invoked without available readiness"
        );
        let readiness_permit = self
            .readiness_permit
            .take()
            .expect("readiness permit must exist after poll_ready");
        let reservation = self
            .shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .reserve(Instant::now())
            .expect("readiness reservation was not reflected in controller capacity");

        let guard = RequestGuard::new(Arc::clone(&self.shared), reservation, readiness_permit);
        let future = dispatch_request(Arc::clone(&self.shared), request, guard);
        ResponseFuture {
            inner: Box::pin(future),
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

struct RequestGuard<S> {
    shared: Arc<Shared<S>>,
    reservation: Option<DispatchReservation>,
    readiness_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    active: Option<InFlightRequest>,
    finished: bool,
}

impl<S> RequestGuard<S> {
    fn new(
        shared: Arc<Shared<S>>,
        reservation: DispatchReservation,
        readiness_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            shared,
            reservation: Some(reservation),
            readiness_permit: Some(readiness_permit),
            active: None,
            finished: false,
        }
    }

    fn mark_dispatched(&mut self, active: InFlightRequest) {
        self.reservation = None;
        self.active = Some(active);
        // The controller has removed the request from its virtual queue, so
        // releasing this permit cannot expose more work than the queue allows.
        self.readiness_permit = None;
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
        // Release capacity only after the controller has been updated. A
        // newly woken caller must observe the cancellation/completion first.
        self.readiness_permit = None;
        self.shared.dispatch.notify_waiters();
    }
}

impl<S> Drop for RequestGuard<S> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Outcome::Failure, Instant::now());
        }
    }
}

async fn dispatch_request<S, Request>(
    shared: Arc<Shared<S>>,
    request: Request,
    mut guard: RequestGuard<S>,
) -> Result<S::Response, S::Error>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    let reservation = guard
        .reservation
        .expect("request guard must begin with a reservation");

    loop {
        let notified = shared.dispatch.notified();
        let decision = {
            let mut controller = shared.controller.lock().expect("controller mutex poisoned");
            let now = Instant::now();
            controller.refresh(now);
            controller.dispatch_state(reservation, now)
        };

        match decision {
            DispatchState::Ready => {
                break;
            }
            DispatchState::WaitUntil(deadline) => {
                let delay = deadline.saturating_duration_since(Instant::now());
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = notified => {},
                }
            }
            DispatchState::WaitForPrevious | DispatchState::InflightLimit => {
                notified.await;
            }
            DispatchState::Cancelled => {
                panic!("an AdaptiveEndpoint request was cancelled while being polled");
            }
        }
    }

    let result = {
        let mut inner = shared.inner.lock().await;
        match std::future::poll_fn(|cx| inner.poll_ready(cx)).await {
            Ok(()) => {
                let now = Instant::now();
                let active = shared
                    .controller
                    .lock()
                    .expect("controller mutex poisoned")
                    .on_dispatched(reservation, now)
                    .expect("dispatch state changed unexpectedly");
                guard.mark_dispatched(active);
                let future = inner.call(request);
                drop(inner);
                future.await
            }
            Err(error) => {
                shared
                    .controller
                    .lock()
                    .expect("controller mutex poisoned")
                    .on_admission_failure(Instant::now());
                Err(error)
            }
        }
    };

    let outcome = if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    };
    let now = Instant::now();
    guard.finish(outcome, now);
    result
}
