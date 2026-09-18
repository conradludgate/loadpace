use loadpace::{
    Completion, EndpointConfig, EndpointController, PairChoice, ScheduleError, reserve_pair,
};
use std::time::{Duration, Instant};

fn endpoint(now: Instant, rtt_ms: u64) -> EndpointController {
    EndpointController::new_with_seed(
        EndpointConfig::new(Duration::from_millis(rtt_ms), 32).with_queue_capacity(1),
        now,
        42,
    )
}

#[test]
fn lower_cost_wins_in_either_argument_order() {
    let now = Instant::now();
    for (first_rtt, second_rtt, expected) in
        [(10, 100, PairChoice::First), (100, 10, PairChoice::Second)]
    {
        let mut first = endpoint(now, first_rtt);
        let mut second = endpoint(now, second_rtt);
        let (choice, reservation) = reserve_pair(&mut first, &mut second, now).unwrap();
        assert_eq!(choice, expected);
        let (selected, other) = match choice {
            PairChoice::First => (&mut first, &mut second),
            PairChoice::Second => (&mut second, &mut first),
        };
        assert_eq!(selected.queued(), 1);
        assert_eq!(other.queued(), 0);
        assert!(!other.cancel(reservation, now));
        let active = selected.on_dispatched(reservation, now).unwrap();
        assert!(selected.finish(active, Completion::Success, now + Duration::from_millis(5)));
    }
}

#[test]
fn equal_cost_prefers_first_sample() {
    let now = Instant::now();
    let mut first = endpoint(now, 20);
    let mut second = endpoint(now, 20);
    let (choice, reservation) = reserve_pair(&mut first, &mut second, now).unwrap();
    assert_eq!(choice, PairChoice::First);
    assert!(first.cancel(reservation, now));
    let (choice, reservation) = reserve_pair(&mut second, &mut first, now).unwrap();
    assert_eq!(choice, PairChoice::First);
    assert!(second.cancel(reservation, now));
}

#[test]
fn full_preferred_endpoint_falls_back_in_either_order() {
    let now = Instant::now();
    for reverse in [false, true] {
        let mut cheap = endpoint(now, 10);
        let mut expensive = endpoint(now, 100);
        let occupied = cheap.reserve(now).unwrap();
        assert!(cheap.load(now) < expensive.load(now));
        let (choice, reservation) = if reverse {
            reserve_pair(&mut expensive, &mut cheap, now)
        } else {
            reserve_pair(&mut cheap, &mut expensive, now)
        }
        .unwrap();
        assert_eq!(
            choice,
            if reverse {
                PairChoice::First
            } else {
                PairChoice::Second
            }
        );
        assert_eq!(cheap.queued(), 1);
        assert!(expensive.cancel(reservation, now));
        assert!(cheap.cancel(occupied, now));
    }
}

#[test]
fn full_pair_rejects_without_changing_queue_depths() {
    let now = Instant::now();
    let mut first = endpoint(now, 20);
    let mut second = endpoint(now, 20);
    let first_reserved = first.reserve(now).unwrap();
    let second_reserved = second.reserve(now).unwrap();
    assert_eq!(
        reserve_pair(&mut first, &mut second, now),
        Err(ScheduleError::QueueFull)
    );
    assert_eq!((first.queued(), second.queued()), (1, 1));
    assert!(first.cancel(first_reserved, now));
    assert!(second.cancel(second_reserved, now));
}

#[test]
fn refresh_effects_remain_on_both_controllers_even_after_rejection() {
    let now = Instant::now();
    for full in [false, true] {
        let mut first = endpoint(now, 20);
        let mut second = endpoint(now, 20);
        for controller in [&mut first, &mut second] {
            let reservation = controller.reserve(now).unwrap();
            controller.on_dispatched(reservation, now).unwrap();
            if full {
                controller.reserve(now).unwrap();
            }
            let _ = controller.take_changes();
        }
        let result = reserve_pair(&mut first, &mut second, now + Duration::from_millis(40));
        assert_eq!(result.is_err(), full);
        for controller in [&mut first, &mut second] {
            let changes = controller.take_changes();
            assert!(changes.dispatch);
            assert!(!changes.admission);
        }
    }
}
