use loadpace::EndpointConfig;
use loadpace_tower::{AdaptiveEndpoint, AdaptiveLayer};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tower::{Layer, Service, ServiceExt};

type BoxResult<T> = Pin<Box<dyn Future<Output = Result<T, &'static str>> + Send>>;

#[derive(Clone, Default)]
struct Echo;

impl Service<u64> for Echo {
    type Response = u64;
    type Error = &'static str;
    type Future = BoxResult<u64>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        Box::pin(async move { Ok(request) })
    }
}

struct Failing;

impl Service<u64> for Failing {
    type Response = u64;
    type Error = &'static str;
    type Future = BoxResult<u64>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: u64) -> Self::Future {
        Box::pin(async { Err("overloaded") })
    }
}

struct LocalEcho {
    calls: Cell<usize>,
}

struct NonSendOutput;

struct NonSendOutputFuture {
    value: Option<u64>,
}

impl Future for NonSendOutputFuture {
    type Output = Result<Rc<u64>, Rc<&'static str>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Rc::new(
            self.value.take().expect("future polled after completion"),
        )))
    }
}

impl Service<u64> for NonSendOutput {
    type Response = Rc<u64>;
    type Error = Rc<&'static str>;
    type Future = NonSendOutputFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        NonSendOutputFuture {
            value: Some(request),
        }
    }
}

impl Service<u64> for LocalEcho {
    type Response = u64;
    type Error = &'static str;
    type Future = BoxResult<u64>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        self.calls.set(self.calls.get() + 1);
        Box::pin(async move { Ok(request) })
    }
}

struct ReadyGate {
    open: AtomicBool,
    waker: futures_util::task::AtomicWaker,
}

struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

impl ReadyGate {
    fn new() -> Self {
        Self {
            open: AtomicBool::new(false),
            waker: futures_util::task::AtomicWaker::new(),
        }
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.waker.wake();
    }
}

struct ReadinessGated {
    gate: Arc<ReadyGate>,
}

impl Service<u64> for ReadinessGated {
    type Response = u64;
    type Error = &'static str;
    type Future = BoxResult<u64>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.gate.open.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        self.gate.waker.register(cx.waker());
        if self.gate.open.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn call(&mut self, request: u64) -> Self::Future {
        Box::pin(async move { Ok(request) })
    }
}

struct Held {
    started: Arc<Notify>,
    release: Arc<Notify>,
    starts: Arc<AtomicUsize>,
}

impl Service<u64> for Held {
    type Response = u64;
    type Error = &'static str;
    type Future = BoxResult<u64>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let starts = Arc::clone(&self.starts);
        Box::pin(async move {
            starts.fetch_add(1, Ordering::Relaxed);
            started.notify_one();
            release.notified().await;
            Ok(request)
        })
    }
}

fn config(queue_capacity: usize, initial_rtt: Duration) -> EndpointConfig {
    EndpointConfig::new(initial_rtt, 1)
        .with_queue_capacity(queue_capacity)
        .with_max_inflight(16)
}

#[tokio::test(start_paused = true)]
async fn layer_wraps_services_that_are_send_but_not_sync() {
    let layer = AdaptiveLayer::new(config(2, Duration::from_millis(1)));
    assert_eq!(layer.config().queue_capacity(), 2);

    let endpoint = layer.layer(LocalEcho {
        calls: Cell::new(0),
    });
    assert_eq!(endpoint.oneshot(42).await.unwrap(), 42);
}

#[tokio::test(start_paused = true)]
async fn endpoint_does_not_require_send_response_or_error_types() {
    let endpoint = AdaptiveEndpoint::new_at(
        NonSendOutput,
        config(1, Duration::from_millis(1)),
        Instant::now(),
    );

    let response = endpoint.oneshot(42).await.unwrap();
    assert_eq!(*response, 42);
}

#[tokio::test(start_paused = true)]
async fn endpoint_dispatches_and_records_a_success() {
    let now = Instant::now();
    let mut endpoint = AdaptiveEndpoint::new_at(Echo, config(2, Duration::from_millis(1)), now);

    let response = endpoint.ready().await.unwrap().call(42).await.unwrap();
    assert_eq!(response, 42);

    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.latency_samples, 1);
}

#[tokio::test(start_paused = true)]
async fn endpoint_exposes_only_a_small_scheduling_horizon() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let mut endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(1, Duration::from_secs(1)),
        Instant::now(),
    );

    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
    let first = endpoint.call(1);
    let first_task = tokio::spawn(first);
    started.notified().await;

    // The first request is in flight, while the second occupies the one-slot
    // virtual scheduling horizon because its GCRA slot is in the future.
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
    let second = endpoint.call(2);
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Pending
    ));

    drop(second);
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));

    release.notify_one();
    assert_eq!(first_task.await.unwrap().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn endpoint_does_not_hold_the_inner_lock_while_a_response_is_running() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let mut endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(2, Duration::from_micros(1)),
        Instant::now(),
    );

    let first = tokio::spawn(endpoint.ready().await.unwrap().call(1));
    started.notified().await;

    let second = tokio::spawn(endpoint.ready().await.unwrap().call(2));
    while starts.load(Ordering::Relaxed) < 2 {
        tokio::time::advance(Duration::from_micros(1)).await;
        tokio::task::yield_now().await;
    }

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn endpoint_failures_are_not_treated_as_fast_healthy_work() {
    let mut endpoint = AdaptiveEndpoint::new_at(
        Failing,
        config(2, Duration::from_millis(10)),
        Instant::now(),
    );

    assert_eq!(
        endpoint.ready().await.unwrap().call(1).await,
        Err("overloaded")
    );
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.failures, 1);
    assert!(snapshot.target_concurrency < 1.0);
}

#[tokio::test(start_paused = true)]
async fn dropping_a_dispatched_request_records_abandonment_without_feedback() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release,
            starts: Arc::new(AtomicUsize::new(0)),
        },
        config(1, Duration::from_millis(1)),
        Instant::now(),
    );

    let request = tokio::spawn(endpoint.ready().await.unwrap().call(1));
    started.notified().await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());

    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.completed, 0);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.inflight, 0);
}

#[tokio::test(start_paused = true)]
async fn dropping_a_queued_response_releases_its_slot() {
    let mut endpoint =
        AdaptiveEndpoint::new_at(Echo, config(1, Duration::from_secs(1)), Instant::now());

    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
    let queued = endpoint.call(1);
    let wake_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Arc::clone(&wake_flag).into();
    assert!(matches!(
        Service::poll_ready(&mut endpoint, &mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    drop(queued);
    assert!(wake_flag.0.load(Ordering::Acquire));
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_fifo_head_unblocks_the_next_request() {
    let gate = Arc::new(ReadyGate::new());
    let mut endpoint = AdaptiveEndpoint::new_at(
        ReadinessGated {
            gate: Arc::clone(&gate),
        },
        config(2, Duration::from_micros(1)),
        Instant::now(),
    );

    let first = tokio::spawn(endpoint.ready().await.unwrap().call(1));
    while endpoint.snapshot().queued != 1 {
        tokio::task::yield_now().await;
    }
    let second = tokio::spawn(endpoint.ready().await.unwrap().call(2));
    while endpoint.snapshot().queued != 2 {
        tokio::task::yield_now().await;
    }

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    gate.open();

    assert_eq!(second.await.unwrap().unwrap(), 2);
    assert_eq!(endpoint.snapshot().queued, 0);
}
