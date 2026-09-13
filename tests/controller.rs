use loadpace::{
    EndpointConfig, EndpointController, Gcra, Gradient2, Gradient2Config, LatencyEstimator,
    LatencyEstimatorConfig, Outcome, ProbeKind, ProbeSchedule, ProbeState, ScheduleError,
};
use rand::SeedableRng;
use std::time::{Duration, Instant};

fn at_zero() -> Instant {
    Instant::now()
}

#[test]
fn gcra_spaces_reservations_and_can_cancel_the_tail() {
    let now = at_zero();
    let mut gcra = Gcra::new(10.0, now);

    let first = gcra.reserve(now);
    let second = gcra.reserve(now);

    assert_eq!(first.scheduled_at, now);
    assert_eq!(second.scheduled_at, now + Duration::from_millis(100));
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(200));
    assert!(gcra.cancel_last(second));
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(100));
    assert!(gcra.cancel_last(first));
    assert_eq!(gcra.next_at(now), now);
}

#[test]
fn gcra_rate_changes_preserve_pacing_debt() {
    let now = at_zero();
    let mut gcra = Gcra::new(2.0, now);
    let first = gcra.reserve(now);
    assert_eq!(first.scheduled_at, now);

    gcra.set_rate(10.0, now);
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(500));
    assert_eq!(gcra.interval(), Duration::from_millis(100));
}

#[test]
fn latency_estimator_keeps_short_and_long_views() {
    let mut estimator = LatencyEstimator::new(LatencyEstimatorConfig {
        initial_rtt: Duration::from_millis(100),
        short_alpha: 0.5,
        long_alpha: 0.1,
        min_rtt: Duration::from_millis(1),
    });

    estimator.observe(Duration::from_millis(20));

    assert_eq!(estimator.samples(), 1);
    assert_eq!(estimator.short(), Duration::from_millis(60));
    assert_eq!(estimator.long(), Duration::from_millis(92));
    assert_eq!(estimator.baseline(), Duration::from_millis(20));
}

#[test]
fn gradient2_preserves_fractional_concurrency_and_reacts_to_congestion() {
    let mut gradient = Gradient2::new(Gradient2Config {
        initial_concurrency: 1.3,
        min_concurrency: 0.25,
        max_concurrency: 20.0,
        tolerance: 1.0,
        gain: 0.1,
        failure_factor: 0.5,
    });

    assert_eq!(gradient.concurrency(), 1.3);
    gradient.on_rtt(Duration::from_millis(10), Duration::from_millis(10));
    assert!((gradient.concurrency() - 1.4).abs() < 1e-9);

    gradient.on_rtt(Duration::from_millis(30), Duration::from_millis(10));
    assert!(gradient.concurrency() < 1.4);
    gradient.on_failure();
    assert!(gradient.concurrency() >= 0.25);
}

#[test]
fn probes_are_additive_positive_and_multiplicative_negative() {
    let now = at_zero();
    let mut state = ProbeState::new();

    state.start_positive(1.0, now + Duration::from_secs(1));
    assert_eq!(
        state.effective_concurrency(8.0, now),
        9.0,
        "positive probes must be additive"
    );
    assert_eq!(
        state.effective_concurrency(8.0, now + Duration::from_secs(1)),
        8.0
    );

    state.start_negative(0.8, now + Duration::from_secs(2));
    assert_eq!(
        state.active(now).map(|probe| probe.kind),
        Some(ProbeKind::Negative { factor: 0.8 })
    );
    assert!((state.effective_concurrency(8.0, now) - 6.4).abs() < f64::EPSILON);
}

#[test]
fn controller_can_drive_seeded_stochastic_probes() {
    let now = at_zero();
    let mut controller = EndpointController::new(EndpointConfig::default(), now);
    let schedule = ProbeSchedule {
        positive_probability: 1.0,
        negative_probability: 0.0,
        positive_delta: 1.0,
        ..ProbeSchedule::default()
    };
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);

    assert!(
        controller
            .maybe_start_probe(&schedule, &mut rng, now)
            .is_some()
    );
    assert_eq!(
        controller.snapshot(now).effective_concurrency,
        controller.snapshot(now).target_concurrency + 1.0
    );
}

#[test]
fn controller_bounds_queue_and_releases_cancelled_virtual_slots() {
    let now = at_zero();
    let config = EndpointConfig::default().queue_capacity(2).max_inflight(10);
    let mut controller = EndpointController::new(config, now);

    let first = controller.reserve(now).unwrap();
    let second = controller.reserve(now).unwrap();
    assert_eq!(controller.queued(), 2);
    assert_eq!(controller.reserve(now), Err(ScheduleError::QueueFull));
    assert!(controller.cancel(second, now));
    assert_eq!(controller.queued(), 1);

    let third = controller.reserve(now).unwrap();
    assert_eq!(
        controller.dispatch_state(third, now),
        loadpace::DispatchState::WaitForPrevious
    );
    assert_eq!(
        controller.dispatch_state(first, now),
        loadpace::DispatchState::Ready
    );
}

#[test]
fn controller_uses_little_law_and_records_failures() {
    let now = at_zero();
    let config = EndpointConfig {
        queue_capacity: 4,
        max_inflight: 4,
        latency: LatencyEstimatorConfig {
            initial_rtt: Duration::from_secs(1),
            short_alpha: 1.0,
            long_alpha: 1.0,
            min_rtt: Duration::from_millis(1),
        },
        gradient: Gradient2Config {
            initial_concurrency: 1.3,
            ..Gradient2Config::default()
        },
    };
    let mut controller = EndpointController::new(config, now);
    let reservation = controller.reserve(now).unwrap();
    let active = controller.on_dispatched(reservation, now).unwrap();
    controller.on_complete(
        active,
        Outcome::Failure,
        Duration::from_secs(1),
        now + Duration::from_secs(1),
    );

    let snapshot = controller.snapshot(now + Duration::from_secs(1));
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.completed, 1);
    assert!(snapshot.target_concurrency < 1.3);
    assert!(snapshot.effective_rate > 0.0);
}
