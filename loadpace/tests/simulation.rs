use loadpace::{
    EndpointConfig, Gradient2Config, LatencyEstimatorConfig, ProbeSchedule, SimulatedEndpoint,
    SimulationConfig, simulate,
};
use std::time::Duration;

fn endpoint(queue_capacity: usize, service_time: Duration) -> SimulatedEndpoint {
    SimulatedEndpoint {
        config: EndpointConfig {
            queue_capacity,
            max_inflight: 16,
            latency: LatencyEstimatorConfig {
                initial_rtt: service_time,
                short_alpha: 1.0,
                long_alpha: 1.0,
                min_rtt: service_time,
                baseline_window: Duration::from_secs(60),
            },
            gradient: Gradient2Config {
                initial_concurrency: 1.0,
                max_concurrency: 100.0,
                ..Gradient2Config::default()
            },
            probe_schedule: ProbeSchedule::default(),
        },
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
            config: EndpointConfig::default().queue_capacity(4),
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
