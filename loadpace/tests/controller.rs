use loadpace::{
    DispatchState, EndpointConfig, EndpointController, Gcra, Gradient2, Gradient2Config,
    LatencyEstimator, LatencyEstimatorConfig, Outcome, ProbeKind, ProbeSchedule, ScheduleError,
};
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
fn gcra_ignores_an_unchanged_quantized_rate() {
    let now = at_zero();
    let mut gcra = Gcra::new(3.0, now);
    gcra.commit(now);
    let tat = gcra.tat();

    gcra.set_rate(gcra.rate(), now + Duration::from_nanos(1));

    assert_eq!(gcra.tat(), tat);
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
        queue_tolerance: Duration::ZERO,
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
    assert!((gradient.concurrency() - 1.2).abs() < 1e-9);
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
fn gradient2_uses_an_absolute_queue_delay_tolerance() {
    fn update(baseline: Duration) -> f64 {
        let mut gradient = Gradient2::new(Gradient2Config {
            queue_tolerance: Duration::from_millis(25),
            smoothing: 1.0,
            ..Gradient2Config::default()
        });
        assert!(gradient.on_rtt_with_baseline_at(
            baseline + Duration::from_millis(25),
            baseline,
            1,
            true,
            at_zero(),
        ));
        gradient.concurrency()
    }

    assert_eq!(
        update(Duration::from_millis(10)),
        update(Duration::from_millis(100)),
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
    let config = EndpointConfig {
        probe_schedule: ProbeSchedule {
            positive_probability: 0.0,
            negative_probability: 0.0,
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };
    let mut controller = EndpointController::new(config, now);

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
    assert_eq!(
        controller.dispatch_state(reservation, now + Duration::from_millis(1)),
        DispatchState::WaitUntil(dispatch_at),
    );
    let second = controller
        .on_dispatched(reservation, dispatch_at)
        .expect("the future slot should become dispatchable");
    assert!(second.was_paced());
}

#[test]
fn controller_does_not_mark_a_collapsed_virtual_slot_as_paced() {
    let now = at_zero();
    let mut controller = EndpointController::new(EndpointConfig::default(), now);

    let first = controller.reserve(now).unwrap();
    let second = controller.reserve(now).unwrap();
    assert!(controller.cancel(first, now));

    let second = controller
        .on_dispatched(second, now)
        .expect("cancelling the head should make the next reservation ready");
    assert!(!second.was_paced());
}

#[test]
fn controller_drives_probes_when_demand_waits() {
    let now = at_zero();
    let config = EndpointConfig {
        probe_schedule: ProbeSchedule {
            positive_probability: 1.0,
            negative_probability: 0.0,
            positive_rate_delta: 20.0,
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };
    let mut controller = EndpointController::new_with_seed(config, now, 5);

    let first = controller.reserve(now).unwrap();
    controller
        .on_dispatched(first, now)
        .expect("the first request should dispatch immediately");
    controller.reserve(now).unwrap();
    controller.refresh(now);

    assert_eq!(
        controller.active_probe().map(|probe| probe.kind),
        Some(ProbeKind::Positive { delta_rate: 20.0 })
    );
}

#[test]
fn controller_load_refreshes_time_driven_policy() {
    let now = at_zero();
    let config = EndpointConfig {
        probe_schedule: ProbeSchedule {
            positive_probability: 1.0,
            negative_probability: 0.0,
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };
    let mut controller = EndpointController::new_with_seed(config, now, 5);
    let reservation = controller.reserve(now).unwrap();
    let request = controller.on_dispatched(reservation, now).unwrap();
    assert!(controller.on_complete(
        request,
        Outcome::Success,
        Duration::from_millis(50),
        now + Duration::from_millis(50),
    ));
    assert_eq!(controller.active_probe(), None);

    let _ = controller.load(now + Duration::from_millis(50));

    assert_eq!(
        controller.active_probe().map(|probe| probe.kind),
        Some(ProbeKind::Positive { delta_rate: 20.0 })
    );
}

#[test]
fn positive_probe_adds_the_same_rate_across_rtts() {
    fn probe_delta(initial_rtt: Duration) -> f64 {
        let now = at_zero();
        let config = EndpointConfig {
            latency: LatencyEstimatorConfig {
                initial_rtt,
                min_rtt: initial_rtt,
                ..LatencyEstimatorConfig::default()
            },
            probe_schedule: ProbeSchedule {
                positive_probability: 1.0,
                negative_probability: 0.0,
                positive_rate_delta: 20.0,
                ..ProbeSchedule::default()
            },
            ..EndpointConfig::default()
        };
        let mut controller = EndpointController::new_with_seed(config, now, 5);
        let reservation = controller.reserve(now).unwrap();
        let request = controller.on_dispatched(reservation, now).unwrap();
        let completed_at = now + initial_rtt;
        assert!(controller.on_complete(request, Outcome::Success, initial_rtt, completed_at,));

        let snapshot = controller.snapshot(completed_at);
        snapshot.effective_rate - snapshot.base_rate
    }

    let short_rtt_delta = probe_delta(Duration::from_millis(10));
    let long_rtt_delta = probe_delta(Duration::from_millis(100));
    assert!((short_rtt_delta - 20.0).abs() < 1e-9);
    assert!((long_rtt_delta - 20.0).abs() < 1e-9);
}

#[test]
fn dispatch_deadline_includes_controller_probe_transitions() {
    let now = at_zero();
    let config = EndpointConfig {
        latency: LatencyEstimatorConfig {
            initial_rtt: Duration::from_secs(1),
            ..LatencyEstimatorConfig::default()
        },
        probe_schedule: ProbeSchedule {
            positive_probability: 1.0,
            negative_probability: 0.0,
            positive_rate_delta: 0.5,
            duration: Duration::from_millis(100),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(1),
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };
    let mut controller = EndpointController::new_with_seed(config, now, 5);

    let first = controller.reserve(now).unwrap();
    controller.on_dispatched(first, now).unwrap();
    let second = controller.reserve(now).unwrap();

    assert_eq!(
        controller.dispatch_state(second, now),
        DispatchState::WaitUntil(now + Duration::from_millis(100)),
        "the adapter must wake when the active probe expires, before the paced slot"
    );
}

#[test]
#[should_panic(expected = "probe probabilities")]
fn probe_schedule_rejects_probabilities_that_exceed_one() {
    let config = EndpointConfig {
        probe_schedule: ProbeSchedule {
            positive_probability: 0.8,
            negative_probability: 0.3,
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };

    EndpointController::new(config, at_zero());
}

#[test]
#[should_panic(expected = "probe interval bounds")]
fn probe_schedule_rejects_invalid_interval_bounds() {
    let config = EndpointConfig {
        probe_schedule: ProbeSchedule {
            min_interval: Duration::from_secs(2),
            max_interval: Duration::from_secs(1),
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };

    EndpointController::new(config, at_zero());
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
        probe_schedule: ProbeSchedule::default(),
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

#[test]
fn controller_rejects_foreign_and_cancelled_reservations() {
    let now = at_zero();
    let mut first = EndpointController::new(EndpointConfig::default(), now);
    let mut second = EndpointController::new(EndpointConfig::default(), now);

    let foreign = first.reserve(now).unwrap();
    assert!(!second.cancel(foreign, now));
    assert_eq!(
        second.dispatch_state(foreign, now),
        DispatchState::Cancelled
    );

    assert!(first.cancel(foreign, now));
    assert!(!first.cancel(foreign, now));
    assert_eq!(first.dispatch_state(foreign, now), DispatchState::Cancelled);
    assert_eq!(
        first.on_dispatched(foreign, now),
        Err(DispatchState::Cancelled)
    );
}

#[test]
fn controller_requires_fifo_dispatch_and_enforces_the_inflight_cap() {
    let now = at_zero();
    let config = EndpointConfig {
        max_inflight: 1,
        probe_schedule: ProbeSchedule {
            positive_probability: 0.0,
            negative_probability: 0.0,
            ..ProbeSchedule::default()
        },
        ..EndpointConfig::default()
    };
    let mut controller = EndpointController::new(config, now);

    let first = controller.reserve(now).unwrap();
    let second = controller.reserve(now).unwrap();
    assert_eq!(
        controller.on_dispatched(second, now),
        Err(DispatchState::WaitForPrevious)
    );

    let active = controller.on_dispatched(first, now).unwrap();
    let second_ready_at = now + controller.pacer().interval();
    assert_eq!(
        controller.dispatch_state(second, second_ready_at),
        DispatchState::InflightLimit
    );

    assert!(controller.on_complete(
        active,
        Outcome::Success,
        Duration::from_millis(50),
        second_ready_at,
    ));
    assert_eq!(
        controller.dispatch_state(second, second_ready_at),
        DispatchState::Ready
    );
}

#[test]
fn controller_records_admission_failures_without_inflight_work() {
    let now = at_zero();
    let mut controller = EndpointController::new(EndpointConfig::default(), now);
    let initial_target = controller.gradient().concurrency();

    controller.on_admission_failure(now);

    let snapshot = controller.snapshot(now);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.completed, 0);
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.inflight, 0);
    assert!(snapshot.target_concurrency < initial_target);
}

fn no_probe_config(initial_rtt: Duration, initial_concurrency: f64) -> EndpointConfig {
    EndpointConfig {
        queue_capacity: 4,
        max_inflight: 1024,
        latency: LatencyEstimatorConfig {
            initial_rtt,
            min_rtt: initial_rtt,
            ..LatencyEstimatorConfig::default()
        },
        gradient: Gradient2Config {
            initial_concurrency,
            max_concurrency: initial_concurrency.max(100.0),
            ..Gradient2Config::default()
        },
        probe_schedule: ProbeSchedule {
            positive_probability: 0.0,
            negative_probability: 0.0,
            ..ProbeSchedule::default()
        },
    }
}

#[test]
fn controller_halves_its_rate_for_each_expected_rtt_without_feedback() {
    let now = at_zero();
    let initial_rtt = Duration::from_millis(20);
    let mut controller = EndpointController::new(no_probe_config(initial_rtt, 32.0), now);
    let reservation = controller.reserve(now).unwrap();
    let request = controller.on_dispatched(reservation, now).unwrap();

    let at_grace = controller.snapshot(now + initial_rtt);
    assert_eq!(at_grace.feedback_silence, Some(initial_rtt));
    assert_eq!(at_grace.feedback_factor, 1.0);
    assert_eq!(at_grace.effective_rate, 1_600.0);

    let at_one_half_life = controller.snapshot(now + initial_rtt * 2);
    assert_eq!(at_one_half_life.feedback_factor, 0.5);
    assert_eq!(at_one_half_life.effective_rate, 800.0);

    let at_two_half_lives = controller.snapshot(now + initial_rtt * 3);
    assert_eq!(at_two_half_lives.feedback_factor, 0.25);
    assert_eq!(at_two_half_lives.effective_rate, 400.0);

    assert!(controller.on_complete(
        request,
        Outcome::Success,
        initial_rtt * 3,
        now + initial_rtt * 3,
    ));
    let recovered = controller.snapshot(now + initial_rtt * 3);
    assert_eq!(recovered.feedback_silence, None);
    assert_eq!(recovered.feedback_factor, 1.0);
}

#[test]
fn controller_uses_recent_feedback_instead_of_the_oldest_request_age() {
    let now = at_zero();
    let initial_rtt = Duration::from_millis(20);
    let mut controller = EndpointController::new(no_probe_config(initial_rtt, 32.0), now);

    let straggler = controller.reserve(now).unwrap();
    let straggler = controller.on_dispatched(straggler, now).unwrap();
    let later = controller.reserve(now).unwrap();
    let later_at = now + controller.pacer().interval();
    let later = controller.on_dispatched(later, later_at).unwrap();
    let feedback_at = now + Duration::from_millis(50);
    assert!(controller.on_complete(later, Outcome::Success, feedback_at - later_at, feedback_at,));

    let snapshot = controller.snapshot(feedback_at + initial_rtt);
    assert_eq!(snapshot.inflight, 1);
    assert_eq!(snapshot.feedback_silence, Some(initial_rtt));
    assert_eq!(snapshot.feedback_factor, 1.0);

    assert!(controller.on_abandoned(straggler, feedback_at + initial_rtt));
}

#[test]
fn abandonment_does_not_count_as_feedback_while_other_work_is_inflight() {
    let now = at_zero();
    let initial_rtt = Duration::from_millis(20);
    let mut controller = EndpointController::new(no_probe_config(initial_rtt, 32.0), now);

    let first = controller.reserve(now).unwrap();
    let first = controller.on_dispatched(first, now).unwrap();
    let second = controller.reserve(now).unwrap();
    let second_at = now + controller.pacer().interval();
    let second = controller.on_dispatched(second, second_at).unwrap();
    let abandoned_at = now + initial_rtt * 2;

    assert!(controller.on_abandoned(first, abandoned_at));
    let snapshot = controller.snapshot(abandoned_at);
    assert_eq!(snapshot.completed, 0);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.feedback_silence, Some(initial_rtt * 2));
    assert_eq!(snapshot.feedback_factor, 0.5);

    assert!(controller.on_abandoned(second, abandoned_at));
    assert_eq!(controller.snapshot(abandoned_at).feedback_silence, None);
}

#[test]
fn blackholed_endpoint_keeps_dispatching_but_bounds_exposure() {
    let now = at_zero();
    let initial_rtt = Duration::from_millis(20);
    let initial_concurrency = 32.0;
    let mut config = no_probe_config(initial_rtt, initial_concurrency);
    config.queue_capacity = 1;
    let mut controller = EndpointController::new(config, now);
    let end = now + Duration::from_secs(1);
    let mut cursor = now;
    let mut dispatches = Vec::new();

    while cursor <= end {
        let reservation = controller.reserve(cursor).unwrap();
        cursor = match controller.dispatch_state(reservation, cursor) {
            DispatchState::Ready => cursor,
            DispatchState::WaitUntil(at) => at,
            state => panic!("unexpected dispatch state: {state:?}"),
        };
        if cursor > end {
            assert!(controller.cancel(reservation, end));
            break;
        }
        controller.on_dispatched(reservation, cursor).unwrap();
        dispatches.push(cursor);
    }

    assert!(dispatches.len() > initial_concurrency as usize);
    assert!(
        dispatches.len() < 90,
        "exponential decay should bound blackhole exposure: {dispatches:?}"
    );
    assert!(
        dispatches
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .is_sorted(),
        "dispatch spacing should increase monotonically once feedback stops"
    );
    assert!(controller.snapshot(end).effective_rate < 1e-10);
}

#[test]
fn endpoint_recovers_immediately_when_feedback_resumes() {
    let now = at_zero();
    let initial_rtt = Duration::from_millis(20);
    let mut controller = EndpointController::new(no_probe_config(initial_rtt, 32.0), now);
    let mut requests = Vec::new();
    let mut cursor = now;

    for _ in 0..4 {
        let reservation = controller.reserve(cursor).unwrap();
        cursor = match controller.dispatch_state(reservation, cursor) {
            DispatchState::Ready => cursor,
            DispatchState::WaitUntil(at) => at,
            state => panic!("unexpected dispatch state: {state:?}"),
        };
        requests.push(controller.on_dispatched(reservation, cursor).unwrap());
    }

    let feedback_at = now + initial_rtt * 4;
    assert!(controller.snapshot(feedback_at).feedback_factor <= 0.125);
    let completed = requests.pop().unwrap();
    let latency = feedback_at.saturating_duration_since(completed.dispatched_at());
    assert!(controller.on_complete(completed, Outcome::Success, latency, feedback_at,));

    let recovered = controller.snapshot(feedback_at);
    assert_eq!(recovered.inflight, requests.len());
    assert_eq!(recovered.feedback_silence, Some(Duration::ZERO));
    assert_eq!(recovered.feedback_factor, 1.0);
    assert_eq!(recovered.effective_rate, recovered.probed_rate);
}

#[test]
fn controller_exposes_its_configured_components() {
    let now = at_zero();
    let config = EndpointConfig {
        queue_capacity: 7,
        max_inflight: 11,
        latency: LatencyEstimatorConfig {
            initial_rtt: Duration::from_millis(80),
            ..LatencyEstimatorConfig::default()
        },
        gradient: Gradient2Config {
            initial_concurrency: 2.0,
            ..Gradient2Config::default()
        },
        ..EndpointConfig::default()
    };
    let controller = EndpointController::new(config, now);

    assert_eq!(controller.config().queue_capacity, 7);
    assert_eq!(controller.config().max_inflight, 11);
    assert_eq!(controller.pacer().rate(), 25.0);
    assert_eq!(controller.gradient().concurrency(), 2.0);
    assert_eq!(
        controller.latency().expected_rtt(),
        Duration::from_millis(80)
    );
}

#[test]
fn gradient2_convenience_methods_match_the_timed_api() {
    let sample = Duration::from_millis(50);

    let mut gradient = Gradient2::new(Gradient2Config::default());
    assert!(gradient.on_rtt(sample, sample, 1));

    let mut gradient = Gradient2::new(Gradient2Config::default());
    assert!(!gradient.on_rtt_with_pacing(sample, sample, 1, false));

    let mut gradient = Gradient2::new(Gradient2Config::default());
    assert!(gradient.on_rtt_with_baseline(sample, sample, 1, true));

    gradient.on_failure();
    assert_eq!(gradient.last_gradient(), 0.0);
    assert_eq!(gradient.updates(), 2);
}
