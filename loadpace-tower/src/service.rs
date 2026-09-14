use futures_core::stream::{Stream, TryStream};
use loadpace::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use pin_project_lite::pin_project;
use rand::Rng;
use rand::rngs::StdRng;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio_util::sync::PollSemaphore;
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
    dispatch: tokio::sync::Notify,
    probe_rng: Mutex<StdRng>,
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
    readiness_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    readiness: PollSemaphore,
}

impl<S> AdaptiveEndpoint<S> {
    pub fn new(inner: S, config: EndpointConfig) -> Self {
        Self::new_at(inner, config, now())
    }

    pub fn new_at(inner: S, config: EndpointConfig, now: Instant) -> Self {
        let queue_capacity = config.queue_capacity;
        let readiness = PollSemaphore::new(Arc::new(tokio::sync::Semaphore::new(queue_capacity)));
        Self {
            shared: Arc::new(Shared {
                inner: tokio::sync::Mutex::new(inner),
                controller: Mutex::new(EndpointController::new(config, now)),
                dispatch: tokio::sync::Notify::new(),
                probe_rng: Mutex::new(rand::make_rng()),
            }),
            readiness_permit: None,
            readiness,
        }
    }

    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changed) = {
            let mut controller = lock(&self.shared.controller);
            let before = controller.probe().current();
            let result = operation(&mut controller);
            (result, before != controller.probe().current())
        };
        if changed {
            self.shared.dispatch.notify_waiters();
        }
        result
    }

    pub fn snapshot(&self) -> loadpace::ControllerSnapshot {
        self.with_controller(|controller| controller.snapshot(now()))
    }

    pub fn start_positive_probe(&self, delta: f64, until: Instant) {
        self.with_controller(|controller| {
            controller.start_positive_probe(delta, until, now());
        });
    }

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
        schedule: &loadpace::ProbeSchedule,
        rng: &mut R,
    ) -> Option<loadpace::Probe> {
        self.with_controller(|controller| controller.maybe_start_probe(schedule, rng, now()))
    }

    pub fn load_metric(&self) -> LoadMetric {
        self.with_controller(|controller| {
            let current = now();
            controller.refresh(current);
            LoadMetric(controller.load(current))
        })
    }
}

pin_project! {
    #[doc = "Maps a Tower discovery stream into freshly initialized adaptive endpoints."]
    #[doc = ""]
    #[doc = "The wrapper intentionally creates new controller state for every insert."]
    #[doc = "This is the safe behavior when discovery removes and later reuses an"]
    #[doc = "endpoint key; state retention can be added without changing the discovery"]
    #[doc = "contract once churn behavior is better understood."]
    pub struct AdaptiveDiscovery<D, Request> {
        #[pin]
        inner: D,
        config: EndpointConfig,
        _request: PhantomData<fn() -> Request>,
    }
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
    D: TryStream<Ok = Change<K, S>>,
    K: Eq,
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: 'static,
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
            readiness_permit: None,
            readiness: self.readiness.clone(),
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

        match self.readiness.poll_acquire(cx) {
            Poll::Ready(Some(permit)) => {
                self.readiness_permit = Some(permit);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => {
                // The semaphore is private to this adapter and is never
                // closed. There is no meaningful S::Error to return for this
                // internal-only failure.
                unreachable!("the endpoint readiness semaphore cannot be closed")
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let Some(readiness_permit) = self.readiness_permit.take() else {
            // This is required by Tower's Service contract: call must follow a
            // successful poll_ready, and there is no S::Error conversion for a
            // caller violating that contract.
            panic!("AdaptiveEndpoint::call invoked without available readiness");
        };
        let reservation = lock(&self.shared.controller).reserve(now());
        let Ok(reservation) = reservation else {
            // The semaphore permit and controller queue are acquired as one
            // logical reservation. Failure here indicates an internal state
            // invariant was broken, not ordinary endpoint backpressure.
            panic!("readiness reservation was not reflected in controller capacity");
        };

        let guard = RequestGuard::new(Arc::clone(&self.shared), reservation, readiness_permit);
        let future = dispatch_request(Arc::clone(&self.shared), request, reservation, guard);
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

enum RequestState {
    Reserved(DispatchReservation),
    Dispatched(InFlightRequest),
    Finished,
}

struct RequestGuard<S> {
    shared: Arc<Shared<S>>,
    state: RequestState,
    readiness_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<S> RequestGuard<S> {
    fn new(
        shared: Arc<Shared<S>>,
        reservation: DispatchReservation,
        readiness_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            shared,
            state: RequestState::Reserved(reservation),
            readiness_permit: Some(readiness_permit),
        }
    }

    fn mark_dispatched(&mut self, active: InFlightRequest) {
        self.state = RequestState::Dispatched(active);
        // The controller has removed the request from its virtual queue, so
        // releasing this permit cannot expose more work than the queue allows.
        self.readiness_permit = None;
    }

    fn finish(&mut self, outcome: Outcome, now: Instant) {
        match std::mem::replace(&mut self.state, RequestState::Finished) {
            RequestState::Dispatched(active) => {
                let latency = now.saturating_duration_since(active.dispatched_at());
                lock(&self.shared.controller).on_complete(active, outcome, latency, now);
            }
            RequestState::Reserved(reservation) => {
                lock(&self.shared.controller).cancel(reservation, now);
            }
            RequestState::Finished => return,
        }
        // Release capacity only after the controller has been updated. A
        // newly woken caller must observe the cancellation/completion first.
        self.readiness_permit = None;
        self.shared.dispatch.notify_waiters();
    }
}

impl<S> Drop for RequestGuard<S> {
    fn drop(&mut self) {
        self.finish(Outcome::Failure, now());
    }
}

struct DispatchPlan {
    state: DispatchState,
    next_wakeup: Option<Instant>,
}

impl DispatchPlan {
    fn wake_at(&self, deadline: Instant) -> Instant {
        self.next_wakeup
            .map_or(deadline, |candidate| deadline.min(candidate))
    }
}

fn dispatch_plan<S>(
    shared: &Shared<S>,
    reservation: DispatchReservation,
    current: Instant,
) -> DispatchPlan {
    let (plan, probe_changed) = {
        let mut controller = lock(&shared.controller);
        let previous_probe = controller.probe().current();
        controller.refresh(current);

        let demand_requires_probing = controller.inflight() > 0 && controller.queued() > 0;
        if demand_requires_probing {
            let schedule = controller.config().probe_schedule.clone();
            let mut rng = lock(&shared.probe_rng);
            controller.maybe_start_probe(&schedule, &mut *rng, current);
        }

        let active_probe = controller.probe().current();
        let next_wakeup = match active_probe {
            Some(probe) => Some(probe.until),
            None if demand_requires_probing => controller.probe().next_probe_at(),
            None => None,
        };
        let plan = DispatchPlan {
            state: controller.dispatch_state(reservation, current),
            next_wakeup,
        };
        (plan, previous_probe != active_probe)
    };

    // Wake waiters after releasing the controller lock so they observe the
    // complete state transition when they re-check their reservations.
    if probe_changed {
        shared.dispatch.notify_waiters();
    }
    plan
}

async fn wait_for_dispatch<S>(shared: &Shared<S>, reservation: DispatchReservation) {
    loop {
        let notified = shared.dispatch.notified();
        let mut notified = std::pin::pin!(notified);
        // Register before inspecting controller state. Otherwise a
        // notification between the state check and the first poll could be
        // lost, leaving this request asleep indefinitely.
        notified.as_mut().enable();

        let plan = dispatch_plan(shared, reservation, now());
        match plan.state {
            DispatchState::Ready => return,
            DispatchState::WaitUntil(deadline) => {
                let wake_at = plan.wake_at(deadline);
                let delay = wake_at.saturating_duration_since(now());
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

async fn dispatch_to_inner<S, Request>(
    shared: &Shared<S>,
    request: Request,
    reservation: DispatchReservation,
    guard: &mut RequestGuard<S>,
) -> Result<S::Response, S::Error>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    let mut inner = shared.inner.lock().await;
    match std::future::poll_fn(|cx| inner.poll_ready(cx)).await {
        Ok(()) => {
            let current = now();
            let Some(active) = lock(&shared.controller).on_dispatched(reservation, current) else {
                unreachable!("a ready dispatch reservation must be dispatchable");
            };
            guard.mark_dispatched(active);
            // Removing the FIFO head can make the next reservation eligible.
            // Wake it even when its old timer has not elapsed because the
            // committed TAT may have changed its effective deadline.
            shared.dispatch.notify_waiters();

            let response = inner.call(request);
            drop(inner);
            response.await
        }
        Err(error) => {
            lock(&shared.controller).on_admission_failure(now());
            Err(error)
        }
    }
}

async fn dispatch_request<S, Request>(
    shared: Arc<Shared<S>>,
    request: Request,
    reservation: DispatchReservation,
    mut guard: RequestGuard<S>,
) -> Result<S::Response, S::Error>
where
    S: Service<Request> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    Request: Send + 'static,
{
    wait_for_dispatch(&shared, reservation).await;
    let result = dispatch_to_inner(&shared, request, reservation, &mut guard).await;
    let outcome = match &result {
        Ok(_) => Outcome::Success,
        Err(_) => Outcome::Failure,
    };
    guard.finish(outcome, now());
    result
}
