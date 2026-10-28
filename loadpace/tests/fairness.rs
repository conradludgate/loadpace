//! Behavioral acceptance tests for multi-client fairness and endpoint churn.
//!
//! The harness uses seeded controller-owned probes and a worker-pooled server
//! model.
//! Each scenario is an executable behavioral assertion about eventual work
//! sharing rather than a production benchmark.

use loadpace::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct ClientSpec {
    join_at: Duration,
    offered_rate: f64,
    network_rtt: Duration,
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
    network_rtt: Duration,
    next_arrival: Option<Instant>,
    controllers: Vec<EndpointController>,
    pending: Vec<VecDeque<DispatchReservation>>,
    measurement_completed: u64,
    completed_by_window: Vec<u64>,
}

struct ServerRuntime {
    workers: usize,
    in_service: usize,
    pending: VecDeque<ServerArrival>,
    service_time: Duration,
    measurement_completed: u64,
}

struct CapacityChange {
    at: Duration,
    server: usize,
    workers: usize,
}

struct ServiceCompletion {
    at: Instant,
    arrival: ServerArrival,
}

struct Completion {
    at: Instant,
    dispatched_at: Instant,
    client: usize,
    server: usize,
    request: InFlightRequest,
}

struct ServerArrival {
    at: Instant,
    dispatched_at: Instant,
    response_delay: Duration,
    client: usize,
    server: usize,
    request: InFlightRequest,
}

#[derive(Debug, PartialEq, Eq)]
struct FairnessReport {
    client_completed: Vec<u64>,
    server_completed: Vec<u64>,
    client_windows: Vec<Vec<u64>>,
    windows: Vec<WindowMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WindowMetrics {
    max_server_queued: usize,
    max_client_queued: usize,
    max_inflight: usize,
    max_queue_delay: Duration,
}

fn config(initial_rtt: Duration) -> EndpointConfig {
    EndpointConfig::new(initial_rtt, 1)
}

fn run(
    clients: Vec<ClientSpec>,
    initial_servers: Vec<ServerSpec>,
    server_join: Option<(Duration, ServerSpec)>,
    measurement_start: Duration,
    duration: Duration,
) -> FairnessReport {
    run_with_seed(
        clients,
        initial_servers,
        server_join,
        measurement_start,
        duration,
        0,
    )
}

fn run_with_seed(
    clients: Vec<ClientSpec>,
    initial_servers: Vec<ServerSpec>,
    server_join: Option<(Duration, ServerSpec)>,
    measurement_start: Duration,
    duration: Duration,
    seed: u64,
) -> FairnessReport {
    run_with_capacity_changes(
        clients,
        initial_servers,
        server_join,
        measurement_start,
        duration,
        seed,
        Vec::new(),
    )
}

fn run_with_capacity_changes(
    clients: Vec<ClientSpec>,
    initial_servers: Vec<ServerSpec>,
    server_join: Option<(Duration, ServerSpec)>,
    measurement_start: Duration,
    duration: Duration,
    seed: u64,
    capacity_changes: Vec<CapacityChange>,
) -> FairnessReport {
    assert!(!initial_servers.is_empty());
    assert!(duration > measurement_start);
    assert!(
        capacity_changes
            .windows(2)
            .all(|pair| pair[0].at < pair[1].at)
    );
    assert!(capacity_changes.iter().all(|change| {
        change.server < initial_servers.len() && change.workers > 0 && change.at < duration
    }));
    let window_count = (duration - measurement_start).as_secs().div_ceil(30) as usize;
    let mut windows = vec![WindowMetrics::default(); window_count];

    let start = Instant::now();
    let end = start + duration;
    let measurement_start = start + measurement_start;
    let mut cursor = start;
    let mut servers: Vec<_> = initial_servers
        .iter()
        .map(|server| ServerRuntime {
            workers: server.workers,
            in_service: 0,
            pending: VecDeque::new(),
            service_time: server.service_time,
            measurement_completed: 0,
        })
        .collect();
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
                network_rtt: client.network_rtt,
                next_arrival: client.join_at.is_zero().then_some(join_at + arrival_phase),
                controllers: initial_servers
                    .iter()
                    .map(|server| {
                        EndpointController::new_with_seed(
                            endpoint_config(
                                &client.config,
                                server.service_time,
                                client.network_rtt,
                            ),
                            start.max(join_at),
                            1000 + client_index as u64 + seed,
                        )
                    })
                    .collect(),
                pending: (0..initial_servers.len())
                    .map(|_| VecDeque::new())
                    .collect(),
                measurement_completed: 0,
                completed_by_window: vec![0; window_count],
            }
        })
        .collect();
    let mut completions: Vec<Completion> = Vec::new();
    let mut service_completions: Vec<ServiceCompletion> = Vec::new();
    let mut server_arrivals: Vec<ServerArrival> = Vec::new();
    let mut rng = StdRng::seed_from_u64(42 + seed);
    let mut pending_server_join = server_join.map(|(at, spec)| (start + at, spec));
    let mut capacity_changes: VecDeque<_> = capacity_changes
        .into_iter()
        .map(|change| (start + change.at, change.server, change.workers))
        .collect();

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
        let next_server_arrival = server_arrivals.iter().map(|arrival| arrival.at).min();
        let next_server = pending_server_join.as_ref().map(|(at, _)| *at);
        let next_capacity_change = capacity_changes.front().map(|(at, _, _)| *at);
        let next_service_completion = service_completions.iter().map(|done| done.at).min();
        let Some(now) = [
            next_arrival,
            next_join,
            next_dispatch,
            next_server_arrival,
            next_completion,
            next_server,
            next_capacity_change,
            next_service_completion,
        ]
        .into_iter()
        .flatten()
        .min() else {
            break;
        };
        if now > end {
            break;
        }

        while capacity_changes
            .front()
            .is_some_and(|(at, _, _)| *at <= now)
        {
            let (_, server, workers) = capacity_changes.pop_front().expect("change exists");
            // Running requests finish; the new limit governs subsequent starts.
            servers[server].workers = workers;
        }

        if pending_server_join
            .as_ref()
            .is_some_and(|(at, _)| *at <= now)
        {
            let (_, server) = pending_server_join.take().expect("server join exists");
            servers.push(ServerRuntime {
                workers: server.workers,
                in_service: 0,
                pending: VecDeque::new(),
                service_time: server.service_time,
                measurement_completed: 0,
            });
            for (client_index, client) in runtimes.iter_mut().enumerate() {
                client.controllers.push(EndpointController::new_with_seed(
                    endpoint_config(&client.config, server.service_time, client.network_rtt),
                    now,
                    1000 + client_index as u64 + seed,
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

        let mut remaining = Vec::with_capacity(service_completions.len());
        for done in service_completions.drain(..) {
            if done.at <= now {
                let arrival = done.arrival;
                servers[arrival.server].in_service -= 1;
                completions.push(Completion {
                    at: done.at + arrival.response_delay,
                    dispatched_at: arrival.dispatched_at,
                    client: arrival.client,
                    server: arrival.server,
                    request: arrival.request,
                });
            } else {
                remaining.push(done);
            }
        }
        service_completions = remaining;

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
                    let window = completion.at.duration_since(measurement_start).as_secs() / 30;
                    // Window counts use half-open intervals, excluding an exact end boundary.
                    if let Some(count) = client.completed_by_window.get_mut(window as usize) {
                        *count += 1;
                    }
                }
                if completion.at >= measurement_start {
                    servers[completion.server].measurement_completed += 1;
                }
            } else {
                remaining.push(completion);
            }
        }
        completions = remaining;

        let mut remaining = Vec::with_capacity(server_arrivals.len());
        for arrival in server_arrivals.drain(..) {
            if arrival.at <= now {
                servers[arrival.server].pending.push_back(arrival);
            } else {
                remaining.push(arrival);
            }
        }
        server_arrivals = remaining;

        let mut window = now
            .checked_duration_since(measurement_start)
            .map(|elapsed| elapsed.as_secs() as usize / 30)
            .and_then(|index| windows.get_mut(index));
        for server in &mut servers {
            while server.in_service < server.workers {
                let Some(arrival) = server.pending.pop_front() else {
                    break;
                };
                if let Some(window) = window.as_mut() {
                    window.max_queue_delay =
                        window.max_queue_delay.max(now.duration_since(arrival.at));
                }
                server.in_service += 1;
                service_completions.push(ServiceCompletion {
                    at: now + server.service_time,
                    arrival,
                });
            }
            if let (Some(window), Some(arrival)) = (window.as_mut(), server.pending.front()) {
                // Include requests still waiting, not just those that started service.
                window.max_queue_delay = window.max_queue_delay.max(now.duration_since(arrival.at));
            }
        }

        for client in &mut runtimes {
            let Some(next_arrival) = client.next_arrival else {
                continue;
            };
            if next_arrival > now {
                continue;
            }

            for controller in &mut client.controllers {
                controller.refresh(now);
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
                    let dispatched_at = now;
                    let one_way_delay = client.network_rtt / 2;
                    server_arrivals.push(ServerArrival {
                        at: now + one_way_delay,
                        dispatched_at,
                        response_delay: one_way_delay,
                        client: client_index,
                        server: server_index,
                        request,
                    });
                }
            }
        }

        if let Some(window) = window {
            window.max_server_queued = window
                .max_server_queued
                .max(servers.iter().map(|server| server.pending.len()).sum());
            window.max_client_queued = window.max_client_queued.max(
                runtimes
                    .iter()
                    .flat_map(|client| &client.pending)
                    .map(VecDeque::len)
                    .sum(),
            );
            window.max_inflight = window.max_inflight.max(
                runtimes
                    .iter()
                    .flat_map(|client| &client.controllers)
                    .map(EndpointController::inflight)
                    .sum(),
            );
        }

        if now == end {
            break;
        }
        cursor = now;
    }

    // Every dispatched request remains in exactly one stage until its response.
    let inflight: usize = runtimes
        .iter()
        .flat_map(|client| &client.controllers)
        .map(EndpointController::inflight)
        .sum();
    assert_eq!(
        inflight,
        server_arrivals.len()
            + service_completions.len()
            + completions.len()
            + servers
                .iter()
                .map(|server| server.pending.len())
                .sum::<usize>()
    );

    let (client_completed, client_windows) = runtimes
        .into_iter()
        .map(|client| (client.measurement_completed, client.completed_by_window))
        .unzip();
    FairnessReport {
        client_completed,
        client_windows,
        windows,
        server_completed: servers
            .into_iter()
            .map(|server| server.measurement_completed)
            .collect(),
    }
}

fn endpoint_config(
    base: &EndpointConfig,
    service_time: Duration,
    network_rtt: Duration,
) -> EndpointConfig {
    let uncongested_rtt = service_time + network_rtt;
    // Recalculate the endpoint RTT without replacing the caller's absolute delay budget.
    EndpointConfig::new(uncongested_rtt, base.initial_concurrency())
        .with_queue_tolerance(base.queue_tolerance())
        .with_queue_capacity(base.queue_capacity())
        .with_max_inflight(base.max_inflight())
}

#[test]
fn endpoint_config_retains_explicit_queue_tolerance() {
    let base = config(Duration::from_millis(10)).with_queue_tolerance(Duration::from_millis(2));
    let endpoint = endpoint_config(&base, Duration::from_millis(10), Duration::from_millis(90));

    assert_eq!(endpoint.expected_rtt(), Duration::from_millis(100));
    assert_eq!(endpoint.queue_tolerance(), Duration::from_millis(2));
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
            network_rtt: Duration::ZERO,
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

#[test]
fn clients_with_heterogeneous_network_rtt_all_make_progress() {
    let mut clients = client_specs(8, Duration::ZERO);
    for client in &mut clients[4..] {
        client.network_rtt = Duration::from_millis(90);
        // Preserve the original scenario's RTT-relative default: 50ms rather than 5ms.
        client.config = config(Duration::from_millis(100));
    }
    let report = run(
        clients,
        vec![server(4)],
        None,
        Duration::from_secs(30),
        Duration::from_secs(60),
    );

    assert!(
        report
            .client_completed
            .iter()
            .all(|completed| *completed > 0),
        "{report:?}"
    );
    assert!(jain(&report.client_completed) > 0.95, "{report:?}");
}

#[test]
fn equal_queue_budgets_remain_fair_across_rtts_and_baseline_windows() {
    for seed in [0, 100, 200, 300, 400] {
        let mut clients = client_specs(8, Duration::ZERO);
        for (index, client) in clients.iter_mut().enumerate() {
            if index >= 4 {
                client.network_rtt = Duration::from_millis(90);
            }
            client.config = config(Duration::from_millis(10) + client.network_rtt)
                .with_queue_tolerance(Duration::from_millis(5));
        }
        let report = run_with_seed(
            clients,
            vec![server(4)],
            None,
            Duration::from_secs(90),
            Duration::from_secs(600),
            seed,
        );

        // Check every 30s window, rather than letting an aggregate hide late drift.
        // The initial 90s permit convergence under the common queue-delay target.
        for window in 0..report.client_windows[0].len() {
            let counts: Vec<_> = report
                .client_windows
                .iter()
                .map(|counts| counts[window])
                .collect();
            let fairness = jain(&counts);
            let throughput = counts.iter().sum::<u64>() as f64 / 30.0;
            assert!(
                counts.iter().all(|count| *count > 0),
                "seed={seed} window={window} counts={counts:?}"
            );
            assert!(
                fairness > 0.95,
                "seed={seed} window={window} jain={fairness} counts={counts:?}"
            );
            // Four 10ms workers provide 400 req/s; fairness must not come from idling.
            assert!(
                throughput > 360.0,
                "seed={seed} window={window} throughput={throughput}"
            );
        }
    }
}

#[test]
fn capacity_change_measurements_are_reproducible() {
    let scenario = || {
        run_with_capacity_changes(
            client_specs(8, Duration::ZERO),
            vec![server(4)],
            None,
            Duration::ZERO,
            Duration::from_secs(15),
            0,
            vec![
                CapacityChange {
                    at: Duration::from_secs(5),
                    server: 0,
                    workers: 1,
                },
                CapacityChange {
                    at: Duration::from_secs(10),
                    server: 0,
                    workers: 4,
                },
            ],
        )
    };
    assert_eq!(scenario(), scenario());
}

#[test]
fn capacity_loss_and_recovery_preserve_progress() {
    for shared_budget in [false, true] {
        for seed in [0, 100, 200, 300, 400] {
            let mut clients = client_specs(8, Duration::ZERO);
            for (index, client) in clients.iter_mut().enumerate() {
                if index >= 4 {
                    client.network_rtt = Duration::from_millis(90);
                }
                client.config = config(Duration::from_millis(10) + client.network_rtt);
                if shared_budget {
                    client.config = client
                        .config
                        .clone()
                        .with_queue_tolerance(Duration::from_millis(5));
                }
            }
            let report = run_with_capacity_changes(
                clients,
                vec![server(4)],
                None,
                Duration::ZERO,
                Duration::from_secs(600),
                seed,
                vec![
                    CapacityChange {
                        at: Duration::from_secs(120),
                        server: 0,
                        workers: 1,
                    },
                    CapacityChange {
                        at: Duration::from_secs(240),
                        server: 0,
                        workers: 4,
                    },
                ],
            );

            for (window, metrics) in report.windows.iter().enumerate() {
                let counts: Vec<_> = report
                    .client_windows
                    .iter()
                    .map(|counts| counts[window])
                    .collect();
                let throughput = counts.iter().sum::<u64>() as f64 / 30.0;
                let fairness = jain(&counts);
                println!(
                    "capacity shared={shared_budget} seed={seed} seconds={} throughput={throughput:.3} jain={fairness:.6} counts={counts:?} {metrics:?}",
                    window * 30,
                );
                let context = format!("shared={shared_budget} seed={seed} window={window}");
                assert!(
                    counts.iter().all(|count| *count > 0),
                    "{context} {counts:?}"
                );
                assert!(metrics.max_client_queued <= 8 * 4, "{context} {metrics:?}");
                assert!(metrics.max_inflight <= 8 * 1024, "{context} {metrics:?}");
                assert!(
                    metrics.max_server_queued <= metrics.max_inflight,
                    "{context} {metrics:?}"
                );

                // Include both transition windows in the capacity/progress checks.
                // Returning responses can straddle the worker-change boundary.
                let capacity = if (4..8).contains(&window) {
                    100.0
                } else {
                    400.0
                };
                assert!(
                    throughput <= capacity * 1.02,
                    "{context} throughput={throughput}"
                );

                // Queue tolerance is a control target, not a hard server-queue cap.
                // Low-capacity utilization sometimes falls below 90% even without
                // a worker change; those limitations are documented separately.

                if window == 3 || window >= 10 {
                    // Recover the existing >90% utilization check at full capacity,
                    // allowing 60s after restoration; check every later 30s window.
                    assert!(throughput > 360.0, "{context} throughput={throughput}");
                }
                if shared_budget && (window == 3 || (5..8).contains(&window) || window >= 10) {
                    assert!(
                        fairness > 0.95,
                        "{context} jain={fairness} counts={counts:?}"
                    );
                }
            }
        }
    }
}
