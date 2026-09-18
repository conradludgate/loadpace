use futures_util::{FutureExt, poll};
use loadpace::{Completion, EndpointConfig, PairChoice, ScheduleError};
use loadpace_tokio::{ActiveRequest, Endpoint, Reservation};
use std::cell::Cell;
use std::future::{Future, pending};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Wake};
use std::time::Duration;
use tokio::time::{advance, timeout};

fn endpoint(capacity: usize) -> Endpoint {
    Endpoint::new_with_seed(
        EndpointConfig::new(Duration::from_millis(20), 32).with_queue_capacity(capacity),
        42,
    )
}

#[tokio::test(start_paused = true)]
async fn dropping_unpolled_guards_releases_bounded_admission() {
    let endpoint = endpoint(1);
    let clone = endpoint.clone();
    let reservation = endpoint.try_reserve().unwrap();
    assert!(matches!(clone.try_reserve(), Err(ScheduleError::QueueFull)));
    drop(reservation);
    let dispatch = clone.try_reserve().unwrap().dispatch();
    assert_eq!(endpoint.snapshot().queued, 1);
    drop(dispatch);
    let run = endpoint.try_reserve().unwrap().run(
        || async { panic!("must not start") },
        |_| Completion::Success,
    );
    drop(run);
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.completed, 0);
}

#[tokio::test(start_paused = true)]
async fn fifo_cancellation_wakes_follower_and_delays_operation_creation() {
    let endpoint = endpoint(2);
    let head = endpoint.try_reserve().unwrap();
    let created = Cell::new(false);
    let mut follower = Box::pin(endpoint.try_reserve().unwrap().run(
        || {
            created.set(true);
            async { 42 }
        },
        |_| Completion::Success,
    ));
    assert!(poll!(&mut follower).is_pending());
    advance(Duration::from_millis(30)).await;
    assert!(poll!(&mut follower).is_pending());
    assert!(!created.get());
    drop(head);
    assert_eq!(timeout(Duration::from_secs(1), follower).await.unwrap(), 42);
    assert!(created.get());
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.queued, 0);
}

#[tokio::test(start_paused = true)]
async fn admission_waiters_wake_on_cancellation_and_dispatch() {
    let endpoint = endpoint(1);
    for dispatch in [false, true] {
        let reservation = endpoint.try_reserve().unwrap();
        let mut ready = Box::pin(endpoint.ready());
        assert!(poll!(&mut ready).is_pending());
        let active = if dispatch {
            Some(reservation.dispatch().await)
        } else {
            drop(reservation);
            None
        };
        timeout(Duration::from_secs(1), ready).await.unwrap();
        // Readiness does not reserve capacity for this waiter.
        let replacement = endpoint.try_reserve().unwrap();
        assert!(matches!(
            endpoint.try_reserve(),
            Err(ScheduleError::QueueFull)
        ));
        drop(replacement);
        drop(active);
    }
}

#[tokio::test(start_paused = true)]
async fn dispatch_is_paced_and_progresses_without_a_self_wakeup_loop() {
    let endpoint = Endpoint::new_with_seed(EndpointConfig::new(Duration::from_millis(20), 1), 42);
    let first = endpoint.try_reserve().unwrap().dispatch().await;
    let start = first.dispatched_at();
    let mut second = Box::pin(endpoint.try_reserve().unwrap().dispatch());
    assert!(poll!(&mut second).is_pending());
    advance(Duration::from_millis(10)).await;
    assert!(poll!(&mut second).is_pending());
    let second = timeout(Duration::from_secs(1), second).await.unwrap();
    assert!(second.dispatched_at().duration_since(start) >= Duration::from_millis(20));
    drop(second);
    drop(first);
}

#[tokio::test(start_paused = true)]
async fn completion_and_abandonment_release_the_inflight_limit() {
    for completion in [
        Completion::Success,
        Completion::Failure,
        Completion::Abandoned,
    ] {
        let endpoint = Endpoint::new_with_seed(
            EndpointConfig::new(Duration::from_millis(20), 1).with_max_inflight(1),
            42,
        );
        let first = endpoint.try_reserve().unwrap().dispatch().await;
        let mut second = Box::pin(endpoint.try_reserve().unwrap().dispatch());
        advance(Duration::from_millis(30)).await;
        assert!(poll!(&mut second).is_pending());
        assert_eq!(endpoint.snapshot().inflight, 1);
        first.finish(completion);
        let second = timeout(Duration::from_secs(1), second).await.unwrap();
        assert_eq!(endpoint.snapshot().inflight, 1);
        drop(second);
        assert_eq!(endpoint.snapshot().inflight, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn success_measures_rtt_from_dispatch_and_finishes_once() {
    let endpoint = endpoint(1);
    let reservation = endpoint.try_reserve().unwrap();
    advance(Duration::from_secs(1)).await;
    let active = reservation.dispatch().await;
    advance(Duration::from_millis(5)).await;
    active.finish(Completion::Success);
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.baseline_rtt, Duration::from_millis(5));
    assert_eq!(snapshot.latency_samples, 1);
    assert_eq!(snapshot.completed, 1);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.inflight, 0);
}

#[tokio::test(start_paused = true)]
async fn failure_and_local_timeout_have_distinct_feedback_effects() {
    for completion in [Completion::Failure, Completion::Abandoned] {
        let endpoint = endpoint(2);
        let first = endpoint.try_reserve().unwrap().dispatch().await;
        let second = endpoint.try_reserve().unwrap().dispatch().await;
        advance(Duration::from_millis(40)).await;
        first.finish(completion);
        let snapshot = endpoint.snapshot();
        assert_eq!(snapshot.inflight, 1);
        assert_eq!(snapshot.failures, 1);
        assert_eq!(snapshot.latency_samples, 0);
        match completion {
            Completion::Failure => {
                assert_eq!(snapshot.completed, 1);
                assert_eq!(snapshot.feedback_silence, Some(Duration::ZERO));
            }
            Completion::Abandoned => {
                assert_eq!(snapshot.completed, 0);
                assert!(snapshot.feedback_silence.unwrap() >= Duration::from_millis(40));
            }
            Completion::Success => unreachable!(),
        }
        drop(second);
        assert_eq!(endpoint.snapshot().feedback_silence, None);
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_active_run_abandons_work() {
    let endpoint = endpoint(1);
    let mut run = Box::pin(
        endpoint
            .try_reserve()
            .unwrap()
            .run(pending::<()>, |_| Completion::Success),
    );
    assert!(poll!(&mut run).is_pending());
    assert_eq!(endpoint.snapshot().inflight, 1);
    drop(run);
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.completed, 0);
}

#[tokio::test(start_paused = true)]
async fn run_can_classify_local_timeout_without_response_feedback() {
    let endpoint = endpoint(1);
    let result = endpoint
        .try_reserve()
        .unwrap()
        .run(
            || timeout(Duration::from_millis(10), pending::<()>()),
            |result| {
                if result.is_err() {
                    Completion::Abandoned
                } else {
                    Completion::Success
                }
            },
        )
        .await;
    assert!(result.is_err());
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.failures, 1);
    assert_eq!(snapshot.completed, 0);
}

#[tokio::test(start_paused = true)]
async fn user_panics_abandon_active_work_without_poisoning_controller() {
    for panic_in_classifier in [false, true] {
        let endpoint = endpoint(1);
        let run = endpoint.try_reserve().unwrap().run(
            || {
                assert!(panic_in_classifier, "operation creation panic");
                async { 42 }
            },
            |_| panic!("classification panic"),
        );
        assert!(AssertUnwindSafe(run).catch_unwind().await.is_err());
        let snapshot = endpoint.snapshot();
        assert_eq!(snapshot.inflight, 0);
        assert_eq!(snapshot.failures, 1);
        assert_eq!(snapshot.completed, 0);
        assert!(endpoint.try_reserve().is_ok());
    }
}

#[tokio::test(start_paused = true)]
async fn pair_selection_preserves_sample_order_and_handles_aliases_and_full_queues() {
    let first = endpoint(1);
    let second = endpoint(1);
    for (a, b) in [(&first, &second), (&second, &first)] {
        let (choice, reservation) = a.try_reserve_pair(b).unwrap();
        assert_eq!(choice, PairChoice::First);
        assert_eq!((a.snapshot().queued, b.snapshot().queued), (1, 0));
        let (choice, fallback) = a.try_reserve_pair(b).unwrap();
        assert_eq!(choice, PairChoice::Second);
        assert!(matches!(
            a.try_reserve_pair(b),
            Err(ScheduleError::QueueFull)
        ));
        drop(reservation);
        drop(fallback);
    }
    let (choice, reservation) = first.try_reserve_pair(&first.clone()).unwrap();
    assert_eq!(choice, PairChoice::First);
    assert_eq!(first.snapshot().queued, 1);
    assert!(matches!(
        first.try_reserve_pair(&first.clone()),
        Err(ScheduleError::QueueFull)
    ));
    drop(reservation);
    assert_eq!(first.snapshot().queued, 0);
}

#[tokio::test(start_paused = true)]
async fn pair_prefers_lower_predicted_completion_cost() {
    let slow = Endpoint::new_with_seed(EndpointConfig::new(Duration::from_millis(100), 1), 42);
    let fast = endpoint(1);
    for (a, b, expected) in [
        (&slow, &fast, PairChoice::Second),
        (&fast, &slow, PairChoice::First),
    ] {
        let (choice, reservation) = a.try_reserve_pair(b).unwrap();
        assert_eq!(choice, expected);
        assert_eq!(fast.snapshot().queued, 1);
        assert_eq!(slow.snapshot().queued, 0);
        drop(reservation);
    }
}

struct WakeFlag(AtomicBool);
impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test(start_paused = true)]
async fn metrics_and_rejected_pair_selection_wake_waiters_on_both_endpoints() {
    for operation in 0..3 {
        let config = EndpointConfig::new(Duration::from_millis(20), 1)
            .with_queue_capacity(1)
            .with_max_inflight(1);
        let first = Endpoint::new_with_seed(config.clone(), 42);
        let second = Endpoint::new_with_seed(config, 42);
        let mut held = Vec::new();
        for endpoint in [&first, &second] {
            let active = endpoint.try_reserve().unwrap().dispatch().await;
            let mut waiting = Box::pin(endpoint.try_reserve().unwrap().dispatch());
            let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
            let waker = Arc::clone(&flag).into();
            assert!(
                waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            held.push((active, waiting, flag));
        }
        advance(Duration::from_millis(40)).await;
        for (_, _, flag) in &held {
            flag.0.store(false, Ordering::Release);
        }
        match operation {
            0 => {
                first.load();
                second.load();
            }
            1 => {
                first.snapshot();
                second.snapshot();
            }
            _ => {
                assert!(matches!(
                    first.try_reserve_pair(&second),
                    Err(ScheduleError::QueueFull)
                ));
            }
        }
        for (_, _, flag) in &held {
            assert!(flag.0.load(Ordering::Acquire));
        }
        // A waiter consumes its own refresh effects instead of notifying itself.
        advance(Duration::from_millis(1)).await;
        for (_, waiting, flag) in &mut held {
            flag.0.store(false, Ordering::Release);
            let waker = Arc::clone(flag).into();
            assert!(
                waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            assert!(!flag.0.load(Ordering::Acquire));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_reverse_pair_calls_keep_admission_bounded() {
    let first = endpoint(2);
    let second = endpoint(2);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for (a, b) in [
        (first.clone(), second.clone()),
        (second.clone(), first.clone()),
    ] {
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..200 {
                let (_, reservation) = a.try_reserve_pair(&b).unwrap();
                assert!(a.snapshot().queued <= 2);
                assert!(b.snapshot().queued <= 2);
                tokio::task::yield_now().await;
                drop(reservation);
            }
        }));
    }
    for task in tasks {
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!((first.snapshot().queued, second.snapshot().queued), (0, 0));
}

#[test]
fn handles_and_owned_futures_can_move_between_tasks() {
    fn send_sync<T: Send + Sync>() {}
    fn send_static<T: Send + 'static>(_: T) {}
    send_sync::<Endpoint>();
    send_sync::<Reservation>();
    send_sync::<ActiveRequest>();
    let endpoint = Endpoint::new(EndpointConfig::new(Duration::from_millis(20), 32));
    send_static(endpoint.try_reserve().unwrap().dispatch());
    send_static(
        endpoint
            .try_reserve()
            .unwrap()
            .run(|| async { 42 }, |_| Completion::Success),
    );
}

#[tokio::test(start_paused = true)]
async fn timeout_while_queued_cancels_without_endpoint_penalty() {
    let endpoint = endpoint(2);
    let head = endpoint.try_reserve().unwrap();
    let waiting = endpoint.try_reserve().unwrap().dispatch();
    assert!(timeout(Duration::from_millis(10), waiting).await.is_err());
    let snapshot = endpoint.snapshot();
    assert_eq!(snapshot.queued, 1);
    assert_eq!(snapshot.inflight, 0);
    assert_eq!(snapshot.failures, 0);
    assert_eq!(snapshot.completed, 0);
    drop(head);
    assert_eq!(endpoint.snapshot().queued, 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_notifies_registered_fifo_and_admission_waiters() {
    let endpoint = endpoint(2);
    let head = endpoint.try_reserve().unwrap();
    let mut follower = Box::pin(endpoint.try_reserve().unwrap().dispatch());
    let mut ready = Box::pin(endpoint.ready());
    let fifo_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let admission_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let fifo_waker = Arc::clone(&fifo_flag).into();
    let admission_waker = Arc::clone(&admission_flag).into();
    assert!(
        follower
            .as_mut()
            .poll(&mut Context::from_waker(&fifo_waker))
            .is_pending()
    );
    assert!(
        ready
            .as_mut()
            .poll(&mut Context::from_waker(&admission_waker))
            .is_pending()
    );
    drop(head);
    assert!(fifo_flag.0.load(Ordering::Acquire));
    assert!(admission_flag.0.load(Ordering::Acquire));
    let active = timeout(Duration::from_secs(1), follower).await.unwrap();
    drop(active);
}
