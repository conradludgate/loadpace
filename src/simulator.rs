//! A small deterministic event-driven simulator for controller changes.
//!
//! The simulator is deliberately kept in the library so algorithm changes can
//! be compared against the same scenarios in unit and integration tests. It is
//! not intended to model a particular protocol or network; service time is the
//! only endpoint-specific model in this first version.

use crate::{
    DispatchReservation, DispatchState, EndpointConfig, EndpointController, InFlightRequest,
    Outcome,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A fixed-service-time endpoint used by [`Simulator`].
#[derive(Clone, Debug)]
pub struct SimulatedEndpoint {
    pub config: EndpointConfig,
    pub service_time: Duration,
}

/// Configuration for a deterministic offered-load simulation.
#[derive(Clone, Debug)]
pub struct SimulationConfig {
    pub duration: Duration,
    pub offered_rate: f64,
    pub endpoints: Vec<SimulatedEndpoint>,
    pub seed: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            offered_rate: 1.0,
            endpoints: vec![SimulatedEndpoint {
                config: EndpointConfig::default(),
                service_time: Duration::from_millis(50),
            }],
            seed: 1,
        }
    }
}

/// Aggregate simulation results.
#[derive(Clone, Debug, PartialEq)]
pub struct SimulationReport {
    pub offered: u64,
    pub accepted: u64,
    pub backpressured: u64,
    pub dispatched: u64,
    pub completed: u64,
    pub max_queued: usize,
    pub endpoints: Vec<EndpointReport>,
}

/// Per-endpoint simulation results.
#[derive(Clone, Debug, PartialEq)]
pub struct EndpointReport {
    pub dispatched: u64,
    pub completed: u64,
    pub snapshot: crate::ControllerSnapshot,
}

struct EndpointRuntime {
    controller: EndpointController,
    pending: VecDeque<DispatchReservation>,
    service_time: Duration,
    dispatched: u64,
    completed: u64,
}

struct Completion {
    at: Instant,
    endpoint: usize,
    request: InFlightRequest,
}

/// Runs a fixed offered-rate workload through adaptive endpoints and P2C.
pub fn simulate(config: SimulationConfig) -> SimulationReport {
    assert!(config.offered_rate.is_finite() && config.offered_rate > 0.0);
    assert!(
        !config.endpoints.is_empty(),
        "a simulation needs at least one endpoint"
    );

    let start = Instant::now();
    let end = start + config.duration;
    let arrival_interval = Duration::from_secs_f64(1.0 / config.offered_rate);
    let mut next_arrival = start;
    let mut offered = 0;
    let mut accepted = 0;
    let mut backpressured = 0;
    let mut max_queued = 0;
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut endpoints: Vec<_> = config
        .endpoints
        .into_iter()
        .map(|endpoint| EndpointRuntime {
            controller: EndpointController::new(endpoint.config, start),
            pending: VecDeque::new(),
            service_time: endpoint.service_time,
            dispatched: 0,
            completed: 0,
        })
        .collect();
    let mut completions: Vec<Completion> = Vec::new();

    while next_arrival <= end || completions.iter().any(|completion| completion.at <= end) {
        let next_completion = completions.iter().map(|completion| completion.at).min();
        let next_dispatch = endpoints
            .iter_mut()
            .filter_map(|endpoint| next_dispatch_at(endpoint, start))
            .min();
        let next_event = [Some(next_arrival), next_completion, next_dispatch]
            .into_iter()
            .flatten()
            .min();
        let Some(now) = next_event else { break };
        if now > end {
            break;
        }

        complete_ready(&mut endpoints, &mut completions, now);
        drive_all(&mut endpoints, &mut completions, now);

        if now == next_arrival {
            offered += 1;
            let choice = choose_p2c(&mut endpoints, &mut rng, now);
            if let Some(index) = choice {
                let endpoint = &mut endpoints[index];
                let reservation = endpoint
                    .controller
                    .reserve(now)
                    .expect("P2C selected an endpoint that was not ready");
                endpoint.pending.push_back(reservation);
                accepted += 1;
            } else {
                backpressured += 1;
            }
            next_arrival = next_arrival
                .checked_add(arrival_interval)
                .expect("arrival schedule overflowed Instant");
        }

        drive_all(&mut endpoints, &mut completions, now);
        max_queued = max_queued.max(
            endpoints
                .iter()
                .map(|endpoint| endpoint.controller.queued())
                .max()
                .unwrap_or(0),
        );
    }

    let dispatched = endpoints.iter().map(|endpoint| endpoint.dispatched).sum();
    let completed = endpoints.iter().map(|endpoint| endpoint.completed).sum();
    let reports = endpoints
        .iter_mut()
        .map(|endpoint| EndpointReport {
            dispatched: endpoint.dispatched,
            completed: endpoint.completed,
            snapshot: endpoint.controller.snapshot(end),
        })
        .collect();

    SimulationReport {
        offered,
        accepted,
        backpressured,
        dispatched,
        completed,
        max_queued,
        endpoints: reports,
    }
}

fn choose_p2c(endpoints: &mut [EndpointRuntime], rng: &mut StdRng, now: Instant) -> Option<usize> {
    let ready: Vec<_> = endpoints
        .iter_mut()
        .enumerate()
        .filter_map(|(index, endpoint)| {
            endpoint.controller.refresh(now);
            endpoint.controller.may_schedule().then_some(index)
        })
        .collect();
    match ready.as_slice() {
        [] => None,
        [only] => Some(*only),
        _ => {
            let a = rng.gen_range(0..ready.len());
            let mut b = rng.gen_range(0..ready.len() - 1);
            if b >= a {
                b += 1;
            }
            let a_index = ready[a];
            let b_index = ready[b];
            let a_load = endpoints[a_index].controller.load(now);
            let b_load = endpoints[b_index].controller.load(now);
            (a_load <= b_load).then_some(a_index).or(Some(b_index))
        }
    }
}

fn next_dispatch_at(endpoint: &mut EndpointRuntime, now: Instant) -> Option<Instant> {
    let reservation = endpoint.pending.front().copied()?;
    match endpoint.controller.dispatch_state(reservation, now) {
        DispatchState::WaitUntil(at) => Some(at),
        DispatchState::Ready => Some(now),
        DispatchState::WaitForPrevious | DispatchState::InflightLimit => None,
        DispatchState::Cancelled => Some(now),
    }
}

fn drive_all(endpoints: &mut [EndpointRuntime], completions: &mut Vec<Completion>, now: Instant) {
    for (index, endpoint) in endpoints.iter_mut().enumerate() {
        endpoint.controller.refresh(now);
        while let Some(reservation) = endpoint.pending.front().copied() {
            match endpoint.controller.dispatch_state(reservation, now) {
                DispatchState::Ready => {
                    endpoint.pending.pop_front();
                    let active = endpoint
                        .controller
                        .on_dispatched(reservation, now)
                        .expect("dispatch state changed unexpectedly");
                    endpoint.dispatched += 1;
                    completions.push(Completion {
                        at: now + endpoint.service_time,
                        endpoint: index,
                        request: active,
                    });
                }
                DispatchState::Cancelled => {
                    endpoint.pending.pop_front();
                }
                DispatchState::WaitUntil(_)
                | DispatchState::WaitForPrevious
                | DispatchState::InflightLimit => {
                    break;
                }
            }
        }
    }
}

fn complete_ready(
    endpoints: &mut [EndpointRuntime],
    completions: &mut Vec<Completion>,
    now: Instant,
) {
    let mut remaining = Vec::with_capacity(completions.len());
    for completion in completions.drain(..) {
        if completion.at <= now {
            let endpoint = &mut endpoints[completion.endpoint];
            endpoint.controller.on_complete(
                completion.request,
                Outcome::Success,
                endpoint.service_time,
                now,
            );
            endpoint.completed += 1;
        } else {
            remaining.push(completion);
        }
    }
    *completions = remaining;
}
