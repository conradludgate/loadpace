use loadpace::{EndpointConfig, SimulatedEndpoint, SimulationConfig, simulate};
use std::time::Duration;

fn endpoint(queue_capacity: usize, service_time: Duration) -> SimulatedEndpoint {
    SimulatedEndpoint {
        config: EndpointConfig::new(service_time, 1)
            .with_queue_capacity(queue_capacity)
            .with_max_inflight(16),
        workers: 1,
        service_time,
    }
}

#[test]
fn simulator_keeps_offered_load_bounded_by_the_endpoint_horizon() {
    let report = simulate(SimulationConfig {
        duration: Duration::from_millis(300),
        offered_rate: 100.0,
        endpoints: vec![endpoint(2, Duration::from_millis(100))],
        seed: 7,
    });

    assert!(report.offered > report.accepted);
    assert!(report.backpressured > 0);
    assert!(report.max_queued <= 2);
    assert!(report.accepted - report.dispatched <= 2);
}

#[test]
fn simulator_uses_predicted_cost_to_shift_work_to_a_faster_endpoint() {
    let report = simulate(SimulationConfig {
        duration: Duration::from_secs(2),
        offered_rate: 20.0,
        endpoints: vec![
            endpoint(4, Duration::from_millis(10)),
            endpoint(4, Duration::from_millis(100)),
        ],
        seed: 42,
    });

    assert!(report.endpoints[0].dispatched > report.endpoints[1].dispatched);
    assert!(report.endpoints[0].snapshot.expected_rtt < report.endpoints[1].snapshot.expected_rtt);
}

#[test]
fn simulator_models_worker_capacity_and_queueing_latency() {
    let report = simulate(SimulationConfig {
        duration: Duration::from_secs(2),
        offered_rate: 500.0,
        endpoints: vec![SimulatedEndpoint {
            config: EndpointConfig::new(Duration::from_millis(50), 1).with_queue_capacity(4),
            workers: 2,
            service_time: Duration::from_millis(10),
        }],
        seed: 11,
    });

    assert!(report.completed > 100);
    assert!(report.endpoints[0].snapshot.expected_rtt > Duration::from_millis(10));
    assert!(report.max_queued <= 4);
}

#[test]
fn simulator_preserves_feedback_decay_between_events() {
    let initial_concurrency = 32;
    let report = simulate(SimulationConfig {
        duration: Duration::from_secs(1),
        offered_rate: 1_600.0,
        endpoints: vec![SimulatedEndpoint {
            config: EndpointConfig::new(Duration::from_millis(20), initial_concurrency),
            workers: 2,
            service_time: Duration::from_secs(2),
        }],
        seed: 42,
    });

    assert_eq!(report.offered, 1_601);
    assert_eq!(report.completed, 0);
    assert!(report.dispatched > initial_concurrency as u64);
    assert!(
        report.dispatched < (initial_concurrency * 3) as u64,
        "missing feedback should slow dispatch well before the inflight cap: {report:?}"
    );
    assert_eq!(report.accepted + report.backpressured, report.offered);
    assert_eq!(
        report.endpoints[0].snapshot.inflight as u64,
        report.dispatched
    );
}

#[test]
fn simulator_makes_progress_above_timer_precision() {
    let report = simulate(SimulationConfig {
        duration: Duration::ZERO,
        offered_rate: 1e20,
        endpoints: vec![endpoint(1, Duration::from_millis(1))],
        seed: 13,
    });

    assert_eq!(report.offered, 1);
    assert_eq!(report.accepted, 1);
}

#[test]
fn simulation_defaults_are_a_runnable_baseline() {
    let config = SimulationConfig::default();

    assert_eq!(config.duration, Duration::from_secs(10));
    assert_eq!(config.offered_rate, 1.0);
    assert_eq!(config.endpoints.len(), 1);
    assert_eq!(config.endpoints[0].workers, 1);
    assert_eq!(config.endpoints[0].service_time, Duration::from_millis(50));
}

#[test]
#[should_panic(expected = "offered rate is too low")]
fn simulator_rejects_an_unrepresentably_low_offered_rate() {
    simulate(SimulationConfig {
        offered_rate: 1e-30,
        ..SimulationConfig::default()
    });
}
