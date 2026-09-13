use loadpace::{EndpointConfig, LatencyEstimatorConfig};
use loadpace_tower::AdaptiveEndpoint;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tower::{Service, ServiceExt};

type BoxResult<T> = Pin<Box<dyn Future<Output = Result<T, &'static str>> + Send>>;

fn assert_send_sync<T: Send + Sync>() {}

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
    EndpointConfig {
        queue_capacity,
        max_inflight: 16,
        latency: LatencyEstimatorConfig {
            initial_rtt,
            short_alpha: 1.0,
            long_alpha: 1.0,
            min_rtt: initial_rtt,
        },
        ..EndpointConfig::default()
    }
}

#[tokio::test(start_paused = true)]
async fn endpoint_dispatches_and_records_a_success() {
    let now = Instant::now();
    let endpoint = AdaptiveEndpoint::new_at(Echo, config(2, Duration::from_millis(1)), now);

    let response = endpoint.clone().oneshot(42).await.unwrap();
    assert_eq!(response, 42);

    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.latency_samples, 1);
}

#[test]
fn endpoint_remains_send_and_sync() {
    assert_send_sync::<AdaptiveEndpoint<Echo>>();
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
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(2, Duration::from_micros(1)),
        Instant::now(),
    );

    let first = tokio::spawn(endpoint.clone().oneshot(1));
    started.notified().await;

    let second = tokio::spawn(endpoint.oneshot(2));
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
    let endpoint = AdaptiveEndpoint::new_at(
        Failing,
        config(2, Duration::from_millis(10)),
        Instant::now(),
    );

    assert_eq!(endpoint.clone().oneshot(1).await, Err("overloaded"));
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.failures, 1);
    assert!(snapshot.target_concurrency < 1.0);
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
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Pending
    ));

    drop(queued);
    assert!(matches!(
        Service::poll_ready(
            &mut endpoint,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
}

#[tokio::test(start_paused = true)]
async fn readiness_capacity_is_shared_across_clones() {
    let mut first =
        AdaptiveEndpoint::new_at(Echo, config(1, Duration::from_secs(1)), Instant::now());
    let mut second = first.clone();

    assert!(matches!(
        Service::poll_ready(
            &mut first,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        Service::poll_ready(
            &mut second,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Pending
    ));

    // Dropping a readiness reservation returns the same permit that wakes
    // other clones, without requiring a shared list of task wakers.
    drop(first);

    assert!(matches!(
        Service::poll_ready(
            &mut second,
            &mut Context::from_waker(futures_util::task::noop_waker_ref())
        ),
        Poll::Ready(Ok(()))
    ));
}
