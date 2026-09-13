use crate::controller::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use futures_core::stream::{Stream, TryStream};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
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

#[derive(Debug, Default)]
struct ReadyWaker {
    wakers: Mutex<Vec<Waker>>,
}

impl ReadyWaker {
    fn register(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock().expect("ready waker mutex poisoned");
        if !wakers.iter().any(|existing| existing.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn wake(&self) {
        let wakers = self
            .wakers
            .lock()
            .expect("ready waker mutex poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for waker in wakers {
            waker.wake();
        }
    }
}

struct Shared<S> {
    inner: tokio::sync::Mutex<S>,
    controller: Mutex<EndpointController>,
    readiness_reservations: Mutex<usize>,
    ready: ReadyWaker,
    dispatch: tokio::sync::Notify,
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
    readiness_reserved: bool,
}

impl<S> AdaptiveEndpoint<S> {
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, Instant::now())
    }

    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        Self {
            shared: Arc::new(Shared {
                inner: tokio::sync::Mutex::new(inner),
                controller: Mutex::new(EndpointController::new(config, now)),
                readiness_reservations: Mutex::new(0),
                ready: ReadyWaker::default(),
                dispatch: tokio::sync::Notify::new(),
            }),
            readiness_reserved: false,
        }
    }

    pub fn controller(&self) -> &Mutex<EndpointController> {
        &self.shared.controller
    }

    pub fn snapshot(&self) -> crate::ControllerSnapshot {
        self.shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .snapshot(Instant::now())
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
            readiness_reserved: false,
        }
    }
}

impl<S> Drop for AdaptiveEndpoint<S> {
    fn drop(&mut self) {
        if self.readiness_reserved {
            let mut reservations = self
                .shared
                .readiness_reservations
                .lock()
                .expect("readiness mutex poisoned");
            *reservations -= 1;
            self.shared.ready.wake();
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
        if self.readiness_reserved {
            return Poll::Ready(Ok(()));
        }

        self.shared.ready.register(cx.waker());

        // Recheck after registering to avoid missing a completion/cancellation
        // that raced with the first check.
        let available = {
            let mut reservations = self
                .shared
                .readiness_reservations
                .lock()
                .expect("readiness mutex poisoned");
            let mut controller = self
                .shared
                .controller
                .lock()
                .expect("controller mutex poisoned");
            controller.refresh(Instant::now());
            let available =
                *reservations + controller.queued() < controller.config().queue_capacity;
            if available {
                *reservations += 1;
            }
            available
        };
        if available {
            self.readiness_reserved = true;
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        assert!(
            self.readiness_reserved,
            "AdaptiveEndpoint::call invoked without available readiness"
        );
        self.readiness_reserved = false;
        *self
            .shared
            .readiness_reservations
            .lock()
            .expect("readiness mutex poisoned") -= 1;
        let reservation = self
            .shared
            .controller
            .lock()
            .expect("controller mutex poisoned")
            .reserve(Instant::now())
            .expect("readiness reservation was not reflected in controller capacity");

        let guard = RequestGuard::new(Arc::clone(&self.shared), reservation);
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
        // `ResponseFuture` does not move after being pinned, and the boxed
        // future is itself pinned.
        unsafe { self.get_unchecked_mut() }.inner.as_mut().poll(cx)
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
        self.shared.ready.wake();
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
