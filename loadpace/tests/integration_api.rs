use loadpace::{
    Completion, ControllerChanges, DispatchState, EndpointConfig, EndpointController, ScheduleError,
};
use std::time::{Duration, Instant};

fn controller(now: Instant) -> EndpointController {
    EndpointController::new_with_seed(
        EndpointConfig::new(Duration::from_millis(20), 32).with_queue_capacity(2),
        now,
        42,
    )
}

#[test]
fn changes_coalesce_until_taken_and_do_not_grant_admission() {
    let now = Instant::now();
    let mut endpoint = controller(now);
    assert_eq!(endpoint.take_changes(), ControllerChanges::default());
    let first = endpoint.reserve(now).unwrap();
    let second = endpoint.reserve(now).unwrap();
    assert_eq!(endpoint.reserve(now), Err(ScheduleError::QueueFull));
    assert_eq!(endpoint.take_changes(), ControllerChanges::default());

    assert!(endpoint.cancel(first, now));
    endpoint.reserve(now).unwrap();
    assert!(!endpoint.may_schedule());
    assert_eq!(
        endpoint.take_changes(),
        ControllerChanges {
            admission: true,
            dispatch: true
        }
    );
    assert_eq!(endpoint.take_changes(), ControllerChanges::default());
    assert!(!endpoint.cancel(first, now));
    assert_eq!(endpoint.take_changes(), ControllerChanges::default());

    let active = endpoint.on_dispatched(second, now).unwrap();
    assert_eq!(
        endpoint.take_changes(),
        ControllerChanges {
            admission: true,
            dispatch: true
        }
    );
    endpoint.finish(active, Completion::Abandoned, now);
    assert_eq!(
        endpoint.take_changes(),
        ControllerChanges {
            admission: false,
            dispatch: true
        }
    );
}

#[test]
fn load_and_snapshot_report_feedback_decay_without_admission_changes() {
    let now = Instant::now();
    let mut endpoint = controller(now);
    let first = endpoint.reserve(now).unwrap();
    let active = endpoint.on_dispatched(first, now).unwrap();
    let waiting = endpoint.reserve(now).unwrap();
    assert!(matches!(
        endpoint.dispatch_state(waiting, now),
        DispatchState::WaitUntil(_)
    ));
    let _ = endpoint.take_changes();

    for (offset, snapshot) in [(40, false), (60, true)] {
        let current = now + Duration::from_millis(offset);
        let before = endpoint.pacer().interval();
        if snapshot {
            endpoint.snapshot(current);
        } else {
            endpoint.load(current);
        }
        assert!(endpoint.pacer().interval() > before);
        assert_eq!(
            endpoint.take_changes(),
            ControllerChanges {
                admission: false,
                dispatch: true
            }
        );
        endpoint.load(current);
        assert_eq!(endpoint.take_changes(), ControllerChanges::default());
    }
    endpoint.finish(
        active,
        Completion::Abandoned,
        now + Duration::from_millis(60),
    );
}

#[test]
fn scheduling_a_probe_recheck_is_visible_even_without_a_rate_change() {
    let now = Instant::now();
    let mut endpoint = controller(now);
    let reservation = endpoint.reserve(now).unwrap();
    let active = endpoint.on_dispatched(reservation, now).unwrap();
    let current = now + Duration::from_millis(20);
    endpoint.finish(active, Completion::Success, current);
    let _ = endpoint.take_changes();
    let interval = endpoint.pacer().interval();

    endpoint.load(current);
    assert_eq!(endpoint.active_probe(), None);
    assert_eq!(endpoint.pacer().interval(), interval);
    assert_eq!(
        endpoint.take_changes(),
        ControllerChanges {
            admission: false,
            dispatch: true
        }
    );
    endpoint.load(current);
    assert_eq!(endpoint.take_changes(), ControllerChanges::default());
}

#[test]
fn finish_measures_from_dispatch_instead_of_reservation() {
    let now = Instant::now();
    let mut endpoint = controller(now);
    let reservation = endpoint.reserve(now).unwrap();
    let dispatched = now + Duration::from_secs(1);
    let active = endpoint.on_dispatched(reservation, dispatched).unwrap();
    let completed = dispatched + Duration::from_millis(5);
    assert!(endpoint.finish(active, Completion::Success, completed));

    let snapshot = endpoint.snapshot(completed);
    assert_eq!(snapshot.baseline_rtt, Duration::from_millis(5));
    assert!(snapshot.expected_rtt < Duration::from_millis(20));
    assert_eq!(snapshot.latency_samples, 1);
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.failures, 0);
}

#[test]
fn finish_distinguishes_failure_feedback_from_abandonment() {
    let now = Instant::now();
    for completion in [Completion::Failure, Completion::Abandoned] {
        let mut endpoint = controller(now);
        let first = endpoint.reserve(now).unwrap();
        let first = endpoint.on_dispatched(first, now).unwrap();
        let second = endpoint.reserve(now).unwrap();
        let second = endpoint
            .on_dispatched(second, now + endpoint.pacer().interval())
            .unwrap();
        let current = now + Duration::from_millis(40);
        let _ = endpoint.take_changes();

        assert!(endpoint.finish(first, completion, current));
        assert_eq!(
            endpoint.take_changes(),
            ControllerChanges {
                admission: false,
                dispatch: true
            }
        );
        let snapshot = endpoint.snapshot(current);
        assert_eq!(snapshot.inflight, 1);
        assert_eq!(snapshot.failures, 1);
        assert_eq!(snapshot.latency_samples, 0);
        assert!(snapshot.target_concurrency < 32.0);
        if completion == Completion::Failure {
            assert_eq!(snapshot.completed, 1);
            assert_eq!(snapshot.feedback_silence, Some(Duration::ZERO));
            assert_eq!(snapshot.feedback_factor, 1.0);
        } else {
            assert_eq!(snapshot.completed, 0);
            assert_eq!(snapshot.feedback_silence, Some(Duration::from_millis(40)));
            assert_eq!(snapshot.feedback_factor, 0.5);
        }
        endpoint.finish(second, Completion::Abandoned, current);
        assert_eq!(endpoint.snapshot(current).feedback_silence, None);
    }
}

#[test]
fn foreign_tokens_neither_finish_work_nor_emit_wakeups() {
    let now = Instant::now();
    let mut endpoint = controller(now);
    for completion in [
        Completion::Success,
        Completion::Failure,
        Completion::Abandoned,
    ] {
        let mut other = controller(now);
        let reservation = other.reserve(now).unwrap();
        assert!(!endpoint.cancel(reservation, now));
        assert_eq!(
            endpoint.on_dispatched(reservation, now),
            Err(DispatchState::Cancelled)
        );
        let active = other.on_dispatched(reservation, now).unwrap();
        assert!(!endpoint.finish(active, completion, now));
        assert_eq!(endpoint.take_changes(), ControllerChanges::default());
        assert_eq!(endpoint.snapshot(now).completed, 0);
        assert_eq!(other.inflight(), 1);
    }
}
