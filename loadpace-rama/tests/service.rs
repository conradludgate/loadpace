use loadpace::{EndpointConfig, LatencyEstimatorConfig, ProbeSchedule, ScheduleError};
use loadpace_rama::{AdaptiveEndpoint, AdaptiveLayer, ServiceError};
use rama::{Layer, Service};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

fn assert_send_sync<T: Send + Sync>() {}

#[derive(Clone, Default)]
struct Echo;

impl Service<u64> for Echo {
    type Output = u64;
    type Error = &'static str;

    async fn serve(&self, request: u64) -> Result<Self::Output, Self::Error> {
        Ok(request)
    }
}

struct Failing;

impl Service<u64> for Failing {
    type Output = u64;
    type Error = &'static str;

    async fn serve(&self, _request: u64) -> Result<Self::Output, Self::Error> {
        Err("overloaded")
    }
}

struct Held {
    started: Arc<Notify>,
    release: Arc<Notify>,
    starts: Arc<AtomicUsize>,
}

impl Service<u64> for Held {
    type Output = u64;
    type Error = &'static str;

    fn serve(
        &self,
        request: u64,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let starts = Arc::clone(&self.starts);
        async move {
            starts.fetch_add(1, Ordering::Relaxed);
            started.notify_one();
            release.notified().await;
            Ok(request)
        }
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

    let response = endpoint.serve(42).await.unwrap();
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
async fn endpoint_rejects_beyond_its_scheduling_horizon() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(1, Duration::from_secs(1)),
        Instant::now(),
    );

    let first_endpoint = endpoint.clone();
    let first = tokio::spawn(async move { first_endpoint.serve(1).await });
    started.notified().await;

    // The first request is in flight, while the second occupies the one-slot
    // virtual scheduling horizon because its GCRA slot is in the future.
    let second = endpoint.serve(2);
    assert_eq!(endpoint.snapshot().queued, 1);

    let third = endpoint.serve(3).await;
    assert!(matches!(
        third,
        Err(ServiceError::Rejected(ScheduleError::QueueFull))
    ));

    // Dropping an unpolled future must cancel its reservation immediately.
    drop(second);
    assert_eq!(endpoint.snapshot().queued, 0);

    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn endpoint_paces_the_next_request() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            starts: Arc::clone(&starts),
        },
        config(2, Duration::from_secs(1)),
        Instant::now(),
    );

    let first_endpoint = endpoint.clone();
    let first = tokio::spawn(async move { first_endpoint.serve(1).await });
    started.notified().await;
    let second_endpoint = endpoint.clone();
    let second = tokio::spawn(async move { second_endpoint.serve(2).await });

    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::Relaxed), 1);

    tokio::time::advance(Duration::from_millis(1)).await;
    while starts.load(Ordering::Relaxed) < 2 {
        tokio::task::yield_now().await;
    }

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn endpoint_allows_concurrent_inner_services() {
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

    let first_endpoint = endpoint.clone();
    let first = tokio::spawn(async move { first_endpoint.serve(1).await });
    started.notified().await;
    let second_endpoint = endpoint.clone();
    let second = tokio::spawn(async move { second_endpoint.serve(2).await });
    while starts.load(Ordering::Relaxed) < 2 {
        tokio::time::advance(Duration::from_micros(1)).await;
        tokio::task::yield_now().await;
    }

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn endpoint_maps_inner_errors_and_updates_controller() {
    let endpoint = AdaptiveEndpoint::new_at(
        Failing,
        config(2, Duration::from_millis(10)),
        Instant::now(),
    );

    assert!(matches!(
        endpoint.serve(1).await,
        Err(ServiceError::Inner("overloaded"))
    ));
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.failures, 1);
    assert!(snapshot.target_concurrency < 1.0);
}

#[tokio::test(start_paused = true)]
async fn dropping_a_dispatched_request_records_a_failure() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let endpoint = AdaptiveEndpoint::new_at(
        Held {
            started: Arc::clone(&started),
            release,
            starts: Arc::new(AtomicUsize::new(0)),
        },
        config(1, Duration::from_millis(1)),
        Instant::now(),
    );

    let request_endpoint = endpoint.clone();
    let request = tokio::spawn(async move { request_endpoint.serve(1).await });
    started.notified().await;
    request.abort();
    let _ = request.await;

    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.inflight, 0);
}

#[tokio::test(start_paused = true)]
async fn layer_wraps_a_rama_service() {
    let endpoint = AdaptiveLayer::new(config(2, Duration::from_millis(1))).layer(Echo);

    assert_eq!(endpoint.serve(7).await.unwrap(), 7);
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

    let first_endpoint = endpoint.clone();
    let first = tokio::spawn(async move { first_endpoint.serve(1).await });
    started.notified().await;
    let second_endpoint = endpoint.clone();
    let second = tokio::spawn(async move { second_endpoint.serve(2).await });
    while endpoint.snapshot().active_probe.is_none() {
        tokio::task::yield_now().await;
    }

    assert_eq!(starts.load(Ordering::Relaxed), 1);
    assert!(matches!(
        endpoint.snapshot().active_probe.unwrap().kind,
        loadpace::ProbeKind::Positive { .. }
    ));

    release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}
