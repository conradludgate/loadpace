//! Behavioral acceptance tests for multi-client fairness and endpoint churn.
//!
//! The harness uses seeded time-based probes and a worker-pooled server model.
//! Each scenario is an executable behavioral assertion about eventual work
//! sharing rather than a production benchmark.

use loadpace::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    LatencyEstimatorConfig, Outcome,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct ClientSpec {
    join_at: Duration,
    offered_rate: f64,
    config: EndpointConfig,
}

#[derive(Clone)]
struct ServerSpec {
    workers: usize,
    service_time: Duration,
}

struct ClientRuntime {
    join_at: Instant,
    arrival_interval: Duration,
    config: EndpointConfig,
    active: bool,
    arrival_phase: Duration,
    next_arrival: Option<Instant>,
    controllers: Vec<EndpointController>,
    pending: Vec<VecDeque<DispatchReservation>>,
    probe_rng: StdRng,
    measurement_completed: u64,
}

struct ServerRuntime {
    available_at: Vec<Instant>,
    service_time: Duration,
    measurement_completed: u64,
}

struct Completion {
    at: Instant,
    dispatched_at: Instant,
    client: usize,
    server: usize,
    request: InFlightRequest,
}

#[derive(Debug, PartialEq, Eq)]
struct FairnessReport {
    client_completed: Vec<u64>,
    server_completed: Vec<u64>,
}

fn config(initial_rtt: Duration) -> EndpointConfig {
    EndpointConfig {
        queue_capacity: 4,
        max_inflight: 1024,
        latency: LatencyEstimatorConfig {
            initial_rtt,
            short_alpha: 0.25,
            long_alpha: 0.05,
            min_rtt: initial_rtt,
        },
        ..EndpointConfig::default()
    }
}

fn run(
    clients: Vec<ClientSpec>,
    initial_servers: Vec<ServerSpec>,
    server_join: Option<(Duration, ServerSpec)>,
    measurement_start: Duration,
    duration: Duration,
) -> FairnessReport {
    assert!(!initial_servers.is_empty());
    assert!(duration > measurement_start);

    let start = Instant::now();
    let end = start + duration;
    let measurement_start = start + measurement_start;
    let mut cursor = start;
    let mut servers: Vec<_> = initial_servers
        .iter()
        .map(|server| ServerRuntime {
            available_at: vec![start; server.workers],
            service_time: server.service_time,
            measurement_completed: 0,
        })
        .collect();
    let probe_schedule = fairness_probe_schedule();
    let mut runtimes: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(client_index, client)| {
            let join_at = start + client.join_at;
            let arrival_phase = Duration::from_millis((client_index % 10) as u64);
            ClientRuntime {
                join_at,
                arrival_interval: Duration::from_secs_f64(1.0 / client.offered_rate),
                config: client.config.clone(),
                active: client.join_at.is_zero(),
                arrival_phase,
                next_arrival: client.join_at.is_zero().then_some(join_at + arrival_phase),
                controllers: initial_servers
                    .iter()
                    .map(|server| {
                        EndpointController::new(
                            endpoint_config(&client.config, server.service_time),
                            start.max(join_at),
                        )
                    })
                    .collect(),
                pending: (0..initial_servers.len())
                    .map(|_| VecDeque::new())
                    .collect(),
                probe_rng: StdRng::seed_from_u64(1000 + client_index as u64),
                measurement_completed: 0,
            }
        })
        .collect();
    let mut completions: Vec<Completion> = Vec::new();
    let mut rng = StdRng::seed_from_u64(42);
    let mut pending_server_join = server_join.map(|(at, spec)| (start + at, spec));

    loop {
        let next_arrival = runtimes
            .iter()
            .filter_map(|client| client.next_arrival)
            .min();
        let next_join = runtimes
            .iter()
            .filter(|client| !client.active)
            .map(|client| client.join_at)
            .min();
        let mut next_dispatch: Option<Instant> = None;
        for client in &mut runtimes {
            for server_index in 0..client.controllers.len() {
                let Some(reservation) = client.pending[server_index].front().copied() else {
                    continue;
                };
                let state = client.controllers[server_index].dispatch_state(reservation, cursor);
                let at = match state {
                    DispatchState::WaitUntil(at) => Some(at),
                    DispatchState::Ready => Some(cursor),
                    DispatchState::WaitForPrevious
                    | DispatchState::InflightLimit
                    | DispatchState::Cancelled => None,
                };
                if let Some(at) = at {
                    next_dispatch = Some(next_dispatch.map_or(at, |current| current.min(at)));
                }
            }
        }
        let next_completion = completions.iter().map(|completion| completion.at).min();
        let next_server = pending_server_join.as_ref().map(|(at, _)| *at);
        let Some(now) = [
            next_arrival,
            next_join,
            next_dispatch,
            next_completion,
            next_server,
        ]
        .into_iter()
        .flatten()
        .min() else {
            break;
        };
        if now > end {
            break;
        }

        if pending_server_join
            .as_ref()
            .is_some_and(|(at, _)| *at <= now)
        {
            let (_, server) = pending_server_join.take().expect("server join exists");
            servers.push(ServerRuntime {
                available_at: vec![now; server.workers],
                service_time: server.service_time,
                measurement_completed: 0,
            });
            for client in &mut runtimes {
                client.controllers.push(EndpointController::new(
                    endpoint_config(&client.config, server.service_time),
                    now,
                ));
                client.pending.push(VecDeque::new());
            }
        }

        for client in &mut runtimes {
            if !client.active && client.join_at <= now {
                client.active = true;
                client.next_arrival = Some(client.join_at + client.arrival_phase);
            }
        }

        let mut remaining = Vec::with_capacity(completions.len());
        for completion in completions.drain(..) {
            if completion.at <= now {
                let client = &mut runtimes[completion.client];
                let controller = &mut client.controllers[completion.server];
                assert!(
                    controller.on_complete(
                        completion.request,
                        Outcome::Success,
                        completion
                            .at
                            .saturating_duration_since(completion.dispatched_at),
                        completion.at,
                    )
                );
                if completion.at >= measurement_start {
                    client.measurement_completed += 1;
                }
                if completion.at >= measurement_start {
                    servers[completion.server].measurement_completed += 1;
                }
            } else {
                remaining.push(completion);
            }
        }
        completions = remaining;

        for client in &mut runtimes {
            let Some(next_arrival) = client.next_arrival else {
                continue;
            };
            if next_arrival > now {
                continue;
            }

            for controller in &mut client.controllers {
                controller.refresh(now);
                controller.maybe_start_probe(&probe_schedule, &mut client.probe_rng, now);
            }

            let candidates: Vec<_> = client
                .controllers
                .iter_mut()
                .enumerate()
                .filter_map(|(index, controller)| controller.may_schedule().then_some(index))
                .collect();
            if !candidates.is_empty() {
                let chosen = if candidates.len() == 1 {
                    candidates[0]
                } else {
                    let first = rng.random_range(0..candidates.len());
                    let mut second = rng.random_range(0..candidates.len() - 1);
                    if second >= first {
                        second += 1;
                    }
                    let first = candidates[first];
                    let second = candidates[second];
                    if client.controllers[first].load(now) <= client.controllers[second].load(now) {
                        first
                    } else {
                        second
                    }
                };
                let reservation = client.controllers[chosen]
                    .reserve(now)
                    .expect("candidate controller must have capacity");
                client.pending[chosen].push_back(reservation);
            }
            client.next_arrival = next_arrival.checked_add(client.arrival_interval);
        }

        for (client_index, client) in runtimes.iter_mut().enumerate() {
            for (server_index, pending) in client.pending.iter_mut().enumerate() {
                while let Some(reservation) = pending.front().copied() {
                    let controller = &mut client.controllers[server_index];
                    controller.refresh(now);
                    if controller.dispatch_state(reservation, now) != DispatchState::Ready {
                        break;
                    }
                    pending.pop_front();
                    let request = controller
                        .on_dispatched(reservation, now)
                        .expect("ready reservation must dispatch");
                    let server = &mut servers[server_index];
                    let worker = server
                        .available_at
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, available_at)| **available_at)
                        .expect("server must have a worker")
                        .0;
                    let dispatched_at = now;
                    let completion_at = server.available_at[worker].max(now) + server.service_time;
                    server.available_at[worker] = completion_at;
                    completions.push(Completion {
                        at: completion_at,
                        dispatched_at,
                        client: client_index,
                        server: server_index,
                        request,
                    });
                }
            }
        }

        if now == end {
            break;
        }
        cursor = now;
    }

    FairnessReport {
        client_completed: runtimes
            .into_iter()
            .map(|client| client.measurement_completed)
            .collect(),
        server_completed: servers
            .into_iter()
            .map(|server| server.measurement_completed)
            .collect(),
    }
}

fn fairness_probe_schedule() -> loadpace::ProbeSchedule {
    loadpace::ProbeSchedule {
        positive_probability: 0.5,
        negative_probability: 0.5,
        positive_delta: 1.0,
        negative_factor: 0.8,
        duration: Duration::from_millis(250),
        min_interval: Duration::from_millis(500),
        max_interval: Duration::from_millis(500),
    }
}

fn endpoint_config(base: &EndpointConfig, service_time: Duration) -> EndpointConfig {
    let mut config = base.clone();
    config.latency.initial_rtt = service_time;
    config.latency.min_rtt = service_time;
    config
}

fn jain(values: &[u64]) -> f64 {
    let sum: f64 = values.iter().map(|value| *value as f64).sum();
    let squared_sum: f64 = values.iter().map(|value| (*value as f64).powi(2)).sum();
    sum * sum / (values.len() as f64 * squared_sum)
}

fn client_specs(count: usize, join_at: Duration) -> Vec<ClientSpec> {
    (0..count)
        .map(|_| ClientSpec {
            join_at,
            offered_rate: 1_000.0,
            config: config(Duration::from_millis(10)),
        })
        .collect()
}

fn server(workers: usize) -> ServerSpec {
    ServerSpec {
        workers,
        service_time: Duration::from_millis(10),
    }
}

#[test]
fn long_running_identical_clients_have_a_fair_measurement() {
    let report = run(
        client_specs(8, Duration::ZERO),
        vec![server(4)],
        None,
        Duration::from_secs(5),
        Duration::from_secs(15),
    );

    assert!(
        report
            .client_completed
            .iter()
            .all(|completed| *completed > 0)
    );
    assert!(jain(&report.client_completed) > 0.95, "{report:?}");
}

#[test]
fn clients_joining_after_warmup_have_a_fair_measurement() {
    let report = run(
        client_specs(4, Duration::ZERO)
            .into_iter()
            .chain(client_specs(4, Duration::from_secs(5)))
            .collect(),
        vec![server(4)],
        None,
        Duration::from_secs(15),
        Duration::from_secs(30),
    );

    assert!(
        report
            .client_completed
            .iter()
            .all(|completed| *completed > 0)
    );
    assert!(jain(&report.client_completed) > 0.95, "{report:?}");
}

#[test]
fn servers_joining_after_warmup_receive_capacity_proportional_work() {
    let report = run(
        client_specs(8, Duration::ZERO),
        vec![server(1)],
        Some((Duration::from_secs(5), server(3))),
        Duration::from_secs(30),
        Duration::from_secs(45),
    );

    let ratio = report.server_completed[1] as f64 / report.server_completed[0] as f64;
    assert!(ratio > 1.5 && ratio < 5.0, "{report:?}");
}

#[test]
fn fairness_measurements_are_reproducible_with_the_same_seed() {
    let clients = client_specs(8, Duration::ZERO);
    let first = run(
        clients.clone(),
        vec![server(4)],
        None,
        Duration::from_secs(5),
        Duration::from_secs(15),
    );
    let second = run(
        clients,
        vec![server(4)],
        None,
        Duration::from_secs(5),
        Duration::from_secs(15),
    );

    assert_eq!(first, second);
}
