use loadpace::{EndpointConfig, LatencyEstimatorConfig, ProbeSchedule};
use loadpace_tower::AdaptiveEndpoint;
use rand::SeedableRng;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tower::{Service, ServiceExt};

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
            baseline_window: Duration::from_secs(60),
        },
        probe_schedule: ProbeSchedule {
            positive_probability: 0.0,
            negative_probability: 0.0,
            ..ProbeSchedule::default()
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
async fn endpoint_exposes_explicit_probe_controls() {
    let now = Instant::now();
    let endpoint = AdaptiveEndpoint::new_at(Echo, config(1, Duration::from_millis(10)), now);

    endpoint.start_positive_probe(1.0, now + Duration::from_secs(1));

    let snapshot = endpoint.snapshot();
    assert_eq!(
        snapshot.effective_concurrency,
        snapshot.target_concurrency + 1.0
    );
}

#[tokio::test(start_paused = true)]
async fn endpoint_probe_checks_are_time_gated() {
    let endpoint =
        AdaptiveEndpoint::new_at(Echo, config(1, Duration::from_millis(10)), Instant::now());
    let schedule = ProbeSchedule {
        positive_probability: 1.0,
        negative_probability: 0.0,
        min_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(1),
        duration: Duration::from_millis(100),
        ..ProbeSchedule::default()
    };
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);

    let first = endpoint
        .maybe_start_probe(&schedule, &mut rng)
        .expect("the first probe should start immediately");
    assert_eq!(endpoint.maybe_start_probe(&schedule, &mut rng), Some(first));

    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(endpoint.maybe_start_probe(&schedule, &mut rng).is_some());
}

#[tokio::test(start_paused = true)]
async fn positive_probe_wakes_a_queued_dispatch() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let now = tokio::time::Instant::now().into_std();
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(2, Duration::from_secs(1)),
        now,
    );

    let first = tokio::spawn(endpoint.clone().oneshot(1));
    started.notified().await;
    tokio::time::advance(Duration::from_millis(100)).await;

    let second = tokio::spawn(endpoint.clone().oneshot(2));
    while endpoint.snapshot().queued != 1 {
        tokio::task::yield_now().await;
    }
    endpoint.start_positive_probe(
        1.0,
        tokio::time::Instant::now().into_std() + Duration::from_secs(1),
    );
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_millis(400)).await;
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::Relaxed), 1);

    tokio::time::advance(Duration::from_millis(101)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::Relaxed), 2);

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn probe_expiry_wakes_a_dispatch_to_recompute_the_slower_rate() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let now = tokio::time::Instant::now().into_std();
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(2, Duration::from_secs(1)),
        now,
    );

    let first = tokio::spawn(endpoint.clone().oneshot(1));
    started.notified().await;
    tokio::time::advance(Duration::from_millis(100)).await;

    let second = tokio::spawn(endpoint.clone().oneshot(2));
    while endpoint.snapshot().queued != 1 {
        tokio::task::yield_now().await;
    }
    endpoint.start_negative_probe(
        0.5,
        tokio::time::Instant::now().into_std() + Duration::from_millis(200),
    );
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_millis(200)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(700)).await;
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::Relaxed), 1);

    tokio::time::advance(Duration::from_millis(201)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::Relaxed), 2);

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn queued_demand_drives_automatic_probing() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let mut endpoint_config = config(2, Duration::from_secs(1));
    endpoint_config.probe_schedule = ProbeSchedule {
        positive_probability: 1.0,
        negative_probability: 0.0,
        min_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(1),
        duration: Duration::from_millis(100),
        ..ProbeSchedule::default()
    };
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        endpoint_config,
        Instant::now(),
    );

    let first = tokio::spawn(endpoint.clone().oneshot(1));
    started.notified().await;
    let second = tokio::spawn(endpoint.clone().oneshot(2));
    tokio::time::advance(Duration::from_secs(1)).await;
    while endpoint.snapshot().active_probe.is_none() {
        tokio::task::yield_now().await;
    }

    assert_eq!(starts.load(Ordering::Relaxed), 2);
    assert!(matches!(
        endpoint.snapshot().active_probe.unwrap().kind,
        loadpace::ProbeKind::Positive { .. }
    ));

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
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
