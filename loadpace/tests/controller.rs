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
fn gcra_spaces_committed_dispatches_without_burst() {
    let now = at_zero();
    let mut gcra = Gcra::new(10.0, now);

    assert_eq!(gcra.next_at(now), now);
    gcra.commit(now);
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(100));
    gcra.commit(now + Duration::from_millis(100));
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(200));
}

#[test]
fn gcra_rate_changes_preserve_pacing_phase() {
    let now = at_zero();
    let mut gcra = Gcra::new(2.0, now);
    gcra.commit(now);

    gcra.set_rate(10.0, now);
    assert_eq!(gcra.next_at(now), now + Duration::from_millis(100));
    assert_eq!(gcra.interval(), Duration::from_millis(100));
}

#[test]
fn gcra_rate_changes_scale_remaining_phase() {
    let now = at_zero();
    let mut gcra = Gcra::new(2.0, now);
    gcra.commit(now);

    let changed_at = now + Duration::from_millis(100);
    gcra.set_rate(10.0, changed_at);
    assert_eq!(gcra.next_at(changed_at), now + Duration::from_millis(180));
}

#[test]
#[should_panic(expected = "GCRA rate is too low")]
fn gcra_rejects_an_unrepresentable_interval() {
    Gcra::new(1e-30, at_zero());
}

#[test]
fn latency_estimator_keeps_short_and_long_views() {
    let mut estimator = LatencyEstimator::new(LatencyEstimatorConfig {
        initial_rtt: Duration::from_millis(100),
        short_alpha: 0.5,
        long_alpha: 0.1,
        min_rtt: Duration::from_millis(1),
        baseline_window: Duration::from_secs(60),
    });

    estimator.observe(Duration::from_millis(20));

    assert_eq!(estimator.samples(), 1);
    assert_eq!(estimator.short(), Duration::from_millis(60));
    assert_eq!(estimator.long(), Duration::from_millis(92));
    assert_eq!(estimator.baseline(), Duration::from_millis(20));
}

#[test]
fn latency_estimator_learns_a_first_sample_above_the_initial_estimate() {
    let mut estimator = LatencyEstimator::new(LatencyEstimatorConfig {
        initial_rtt: Duration::from_millis(50),
        short_alpha: 0.25,
        long_alpha: 0.05,
        min_rtt: Duration::from_millis(1),
        baseline_window: Duration::from_secs(60),
    });

    assert_eq!(estimator.baseline(), Duration::from_millis(50));
    estimator.observe(Duration::from_millis(100));

    assert_eq!(estimator.baseline(), Duration::from_millis(100));
}

#[test]
fn latency_estimator_expires_stale_baseline_samples() {
    let now = at_zero();
    let mut estimator = LatencyEstimator::new(LatencyEstimatorConfig {
        initial_rtt: Duration::from_millis(10),
        short_alpha: 1.0,
        long_alpha: 1.0,
        min_rtt: Duration::from_millis(1),
        baseline_window: Duration::from_secs(8),
    });

    estimator.observe_at(Duration::from_millis(10), now);
    estimator.observe_at(Duration::from_millis(100), now + Duration::from_secs(8));

    assert_eq!(estimator.baseline(), Duration::from_millis(100));
}

#[test]
#[should_panic(expected = "minimum RTT must be positive")]
fn latency_estimator_rejects_a_zero_minimum_rtt() {
    LatencyEstimator::new(LatencyEstimatorConfig {
        initial_rtt: Duration::ZERO,
        short_alpha: 1.0,
        long_alpha: 1.0,
        min_rtt: Duration::ZERO,
        baseline_window: Duration::from_secs(60),
    });
}

#[test]
#[should_panic(expected = "bounds must be finite")]
fn gradient2_rejects_an_infinite_upper_bound() {
    Gradient2::new(Gradient2Config {
        max_concurrency: f64::INFINITY,
        ..Gradient2Config::default()
    });
}

#[test]
fn gradient2_preserves_fractional_concurrency_and_reacts_to_congestion() {
    let mut gradient = Gradient2::new(Gradient2Config {
        initial_concurrency: 1.3,
        min_concurrency: 0.25,
        max_concurrency: 20.0,
        tolerance: 1.0,
        gain: 0.1,
        smoothing: 1.0,
        update_interval: Duration::from_secs(1),
        failure_factor: 0.5,
    });

    assert_eq!(gradient.concurrency(), 1.3);
    let now = at_zero();
    assert!(gradient.on_rtt_at(Duration::from_millis(10), Duration::from_millis(10), 1, now,));
    assert!((gradient.concurrency() - 1.4).abs() < 1e-9);

    assert!(gradient.on_rtt_at(
        Duration::from_millis(30),
        Duration::from_millis(10),
        1,
        now + Duration::from_secs(1),
    ));
    assert!(gradient.concurrency() < 1.4);
    gradient.on_failure_at(now + Duration::from_secs(2));
    assert!(gradient.concurrency() >= 0.25);
}

#[test]
fn gradient2_limits_healthy_updates_to_the_configured_interval() {
    let now = at_zero();
    let mut gradient = Gradient2::new(Gradient2Config {
        gain: 0.1,
        smoothing: 1.0,
        update_interval: Duration::from_secs(1),
        ..Gradient2Config::default()
    });

    assert!(gradient.on_rtt_at(Duration::from_millis(10), Duration::from_millis(10), 1, now,));
    assert!(!gradient.on_rtt_at(
        Duration::from_millis(10),
        Duration::from_millis(10),
        1,
        now + Duration::from_millis(999),
    ));
    assert_eq!(gradient.updates(), 1);
    assert_eq!(gradient.concurrency(), 1.1);

    assert!(gradient.on_rtt_at(
        Duration::from_millis(10),
        Duration::from_millis(10),
        1,
        now + Duration::from_secs(1),
    ));
    assert_eq!(gradient.updates(), 2);
    assert_eq!(gradient.concurrency(), 1.2);
}

#[test]
fn gradient2_update_count_is_stable_across_sample_rates() {
    fn run(samples: usize, interval: Duration) -> (f64, u64) {
        let now = at_zero();
        let mut gradient = Gradient2::new(Gradient2Config {
            gain: 0.1,
            smoothing: 1.0,
            update_interval: Duration::from_secs(1),
            ..Gradient2Config::default()
        });

        let mut updates = 0;
        for sample in 0..samples {
            updates += gradient.on_rtt_at(
                Duration::from_millis(10),
                Duration::from_millis(10),
                1,
                now + interval.saturating_mul(sample as u32),
            ) as u64;
        }

        assert_eq!(updates, 3);
        (gradient.concurrency(), gradient.updates())
    }

    assert_eq!(
        run(21, Duration::from_millis(100)),
        run(3, Duration::from_secs(1))
    );
}

#[test]
fn gradient2_does_not_grow_when_application_limited() {
    let mut gradient = Gradient2::new(Gradient2Config::default());
    let initial = gradient.concurrency();

    assert!(!gradient.on_rtt_with_pacing_at(
        Duration::from_millis(10),
        Duration::from_millis(10),
        1,
        false,
        at_zero(),
    ));
    assert_eq!(gradient.concurrency(), initial);
    assert_eq!(gradient.updates(), 0);
}

#[test]
fn controller_marks_future_slots_as_paced() {
    let now = at_zero();
    let mut controller = EndpointController::new(EndpointConfig::default(), now);

    let first_reservation = controller.reserve(now).unwrap();
    let first = controller
        .on_dispatched(first_reservation, now)
        .expect("the first request should dispatch immediately");
    assert!(!first.was_paced());
    assert!(controller.on_complete(
        first,
        Outcome::Success,
        Duration::from_millis(50),
        now + Duration::from_millis(1),
    ));

    let reservation = controller.reserve(now + Duration::from_millis(1)).unwrap();
    let dispatch_at = now + Duration::from_millis(50);
    let second = controller
        .on_dispatched(reservation, dispatch_at)
        .expect("the future slot should become dispatchable");
    assert!(second.was_paced());
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
fn probe_schedule_is_time_gated() {
    let now = at_zero();
    let mut state = ProbeState::new();
    let schedule = ProbeSchedule {
        positive_probability: 1.0,
        negative_probability: 0.0,
        duration: Duration::from_millis(100),
        min_interval: Duration::from_secs(1),
        max_interval: Duration::from_secs(1),
        ..ProbeSchedule::default()
    };
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);

    assert!(schedule.maybe_start(&mut state, &mut rng, now).is_some());
    assert!(
        schedule
            .maybe_start(&mut state, &mut rng, now + Duration::from_millis(100))
            .is_none()
    );
    assert!(
        schedule
            .maybe_start(&mut state, &mut rng, now + Duration::from_secs(1))
            .is_some()
    );
}

#[test]
#[should_panic(expected = "probe probabilities")]
fn probe_schedule_rejects_probabilities_that_exceed_one() {
    let now = at_zero();
    let mut state = ProbeState::new();
    let schedule = ProbeSchedule {
        positive_probability: 0.8,
        negative_probability: 0.3,
        ..ProbeSchedule::default()
    };
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);

    schedule.maybe_start(&mut state, &mut rng, now);
}

#[test]
#[should_panic(expected = "probe interval bounds")]
fn probe_schedule_rejects_invalid_interval_bounds() {
    let now = at_zero();
    let mut state = ProbeState::new();
    let schedule = ProbeSchedule {
        min_interval: Duration::from_secs(2),
        max_interval: Duration::from_secs(1),
        ..ProbeSchedule::default()
    };
    let mut rng = rand::rngs::StdRng::seed_from_u64(5);

    schedule.maybe_start(&mut state, &mut rng, now);
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
            baseline_window: Duration::from_secs(60),
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

#[test]
fn controller_rejects_a_completion_from_another_controller() {
    let now = at_zero();
    let mut first = EndpointController::new(EndpointConfig::default(), now);
    let mut second = EndpointController::new(EndpointConfig::default(), now);

    let first_reservation = first.reserve(now).unwrap();
    let first_request = first.on_dispatched(first_reservation, now).unwrap();
    let second_reservation = second.reserve(now).unwrap();
    let second_request = second.on_dispatched(second_reservation, now).unwrap();

    assert!(!second.on_complete(
        first_request,
        Outcome::Success,
        Duration::from_millis(1),
        now + Duration::from_millis(1),
    ));
    assert_eq!(second.inflight(), 1);

    assert!(second.on_complete(
        second_request,
        Outcome::Success,
        Duration::from_millis(1),
        now + Duration::from_millis(1),
    ));
    assert_eq!(second.inflight(), 0);
    assert_eq!(second.snapshot(now).completed, 1);
}
