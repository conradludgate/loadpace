use loadpace::EndpointConfig;
use loadpace_tower::{AdaptiveEndpoint, AdaptiveLayer, ServiceError};
use std::cell::Cell;
use std::error::Error;
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
    type Output = Result<Rc<u64>, &'static str>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Rc::new(
            self.value.take().expect("future polled after completion"),
        )))
    }
}

impl Service<u64> for NonSendOutput {
    type Response = Rc<u64>;
    type Error = &'static str;
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
async fn endpoint_does_not_require_send_response_types() {
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
async fn metric_reads_wake_dispatch_waiters_when_feedback_decays() {
    for read_snapshot in [false, true] {
        let mut endpoint = AdaptiveEndpoint::new(
            Held {
                started: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
                starts: Arc::new(AtomicUsize::new(0)),
            },
            config(2, Duration::from_millis(20)),
        );
        let mut first = endpoint.ready().await.unwrap().call(1);
        assert!(futures_util::poll!(&mut first).is_pending());
        let mut waiting = endpoint.ready().await.unwrap().call(2);
        let wake_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
        let waker = Arc::clone(&wake_flag).into();
        assert!(
            Pin::new(&mut waiting)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let probe = endpoint.snapshot().active_probe;

        tokio::time::advance(Duration::from_millis(40)).await;
        wake_flag.0.store(false, Ordering::Release);
        if read_snapshot {
            endpoint.snapshot();
        } else {
            endpoint.load_metric();
        }
        assert!(wake_flag.0.load(Ordering::Acquire));
        assert_eq!(endpoint.snapshot().active_probe, probe);
        drop(waiting);
        drop(first);
    }
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
        endpoint
            .ready()
            .await
            .unwrap()
            .call(1)
            .await
            .unwrap_err()
            .to_string(),
        "overloaded"
    );
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.failures, 1);
    assert!(snapshot.target_concurrency < 1.0);
    assert!(endpoint.ready().await.is_ok());
}

struct ReadinessFailing {
    gate: Arc<ReadyGate>,
    failures: Arc<AtomicUsize>,
    successful_calls: usize,
    release: Arc<Notify>,
}

impl Service<u64> for ReadinessFailing {
    type Response = u64;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<u64, std::io::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        assert_eq!(
            self.failures.load(Ordering::Relaxed),
            0,
            "polled after terminal failure"
        );
        if self.successful_calls > 0 {
            return Poll::Ready(Ok(()));
        }
        self.gate.waker.register(cx.waker());
        if self.gate.open.load(Ordering::Acquire) {
            self.failures.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Err(std::io::Error::other("transport closed")))
        } else {
            Poll::Pending
        }
    }

    fn call(&mut self, request: u64) -> Self::Future {
        assert!(self.successful_calls > 0);
        self.successful_calls -= 1;
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            release.notified().await;
            Ok(request)
        })
    }
}

fn terminal_source(error: &tower::BoxError) -> &std::io::Error {
    let shared = error.downcast_ref::<ServiceError>().unwrap();
    let source = shared
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.to_string(), "transport closed");
    source
}

#[tokio::test(start_paused = true)]
async fn terminal_readiness_failure_wakes_and_fails_all_queued_requests() {
    let gate = Arc::new(ReadyGate::new());
    let failures = Arc::new(AtomicUsize::new(0));
    let mut endpoint = AdaptiveEndpoint::new(
        ReadinessFailing {
            gate: Arc::clone(&gate),
            failures: Arc::clone(&failures),
            successful_calls: 0,
            release: Arc::new(Notify::new()),
        },
        config(3, Duration::from_secs(1)),
    );
    let mut head = endpoint.ready().await.unwrap().call(1);
    let mut waiting = endpoint.ready().await.unwrap().call(2);
    let unpolled = endpoint.ready().await.unwrap().call(3);
    assert!(futures_util::poll!(&mut head).is_pending());

    let dispatch_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
    let dispatch_waker = Arc::clone(&dispatch_wake).into();
    assert!(
        Pin::new(&mut waiting)
            .poll(&mut Context::from_waker(&dispatch_waker))
            .is_pending()
    );
    let admission_wake = Arc::new(WakeFlag(AtomicBool::new(false)));
    let admission_waker = Arc::clone(&admission_wake).into();
    assert!(
        endpoint
            .poll_ready(&mut Context::from_waker(&admission_waker))
            .is_pending()
    );

    gate.open();
    let head_error = head.await.unwrap_err();
    assert!(dispatch_wake.0.load(Ordering::Acquire));
    assert!(admission_wake.0.load(Ordering::Acquire));
    let waiting_error = waiting.await.unwrap_err();
    let unpolled_error = unpolled.await.unwrap_err();
    for error in [&waiting_error, &unpolled_error] {
        assert!(std::ptr::eq(
            terminal_source(&head_error),
            terminal_source(error)
        ));
    }
    for _ in 0..2 {
        let Poll::Ready(Err(error)) =
            endpoint.poll_ready(&mut Context::from_waker(&admission_waker))
        else {
            panic!("terminal failure must be exposed through readiness");
        };
        assert!(std::ptr::eq(
            terminal_source(&head_error),
            terminal_source(&error)
        ));
    }
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.completed, 0);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.latency_samples, 0);
    assert_eq!(failures.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn terminal_failure_handles_an_outstanding_readiness_grant_and_inflight_response() {
    let gate = Arc::new(ReadyGate::new());
    gate.open();
    let release = Arc::new(Notify::new());
    let mut endpoint = AdaptiveEndpoint::new(
        ReadinessFailing {
            gate,
            failures: Arc::new(AtomicUsize::new(0)),
            successful_calls: 1,
            release: Arc::clone(&release),
        },
        config(2, Duration::from_micros(1)),
    );
    let mut inflight = endpoint.ready().await.unwrap().call(1);
    assert!(futures_util::poll!(&mut inflight).is_pending());
    assert_eq!(endpoint.snapshot().inflight, 1);
    let failing = endpoint.ready().await.unwrap().call(2);
    endpoint.ready().await.unwrap();

    tokio::time::advance(Duration::from_micros(10)).await;
    let failure = failing.await.unwrap_err();
    let late = endpoint.call(3);
    assert_eq!(endpoint.snapshot().queued, 0);
    let late_error = late.await.unwrap_err();
    assert!(std::ptr::eq(
        terminal_source(&failure),
        terminal_source(&late_error)
    ));

    release.notify_one();
    assert_eq!(inflight.await.unwrap(), 1);
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 1);
    assert!(endpoint.ready().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn balance_evicts_a_failed_endpoint_and_accepts_its_replacement() {
    let gate = Arc::new(ReadyGate::new());
    gate.open();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender
        .try_send(Ok::<_, tower::BoxError>(tower::discover::Change::Insert(
            1,
            ReadinessFailing {
                gate: Arc::clone(&gate),
                failures: Arc::new(AtomicUsize::new(0)),
                successful_calls: 0,
                release: Arc::new(Notify::new()),
            },
        )))
        .unwrap();
    let discovery = loadpace_tower::AdaptiveDiscovery::new(
        futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)),
        config(2, Duration::from_millis(1)),
    );
    let mut balance = tower::balance::p2c::Balance::new(discovery);
    let error = balance.ready().await.unwrap().call(1).await.unwrap_err();
    terminal_source(&error);
    assert!(futures_util::poll!(balance.ready()).is_pending());
    assert!(balance.is_empty());

    let release = Arc::new(Notify::new());
    release.notify_one();
    sender
        .try_send(Ok(tower::discover::Change::Insert(
            1,
            ReadinessFailing {
                gate,
                failures: Arc::new(AtomicUsize::new(0)),
                successful_calls: 1,
                release,
            },
        )))
        .unwrap();
    assert_eq!(balance.ready().await.unwrap().call(2).await.unwrap(), 2);
    assert_eq!(balance.len(), 1);
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
