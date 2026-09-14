use crate::ScheduleError;
use crate::gcra::{Gcra, saturating_add};
use crate::gradient::{Gradient2, Gradient2Config};
use crate::latency::{LatencyEstimator, LatencyEstimatorConfig};
use crate::probe::{Probe, ProbeSchedule, ProbeState};
use rand::Rng;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_CONTROLLER_ID: AtomicU64 = AtomicU64::new(0);

/// Configuration for one adaptive endpoint.
#[derive(Clone, Debug)]
pub struct EndpointConfig {
    /// Number of accepted-but-not-yet-dispatched requests allowed per endpoint.
    pub queue_capacity: usize,
    /// Emergency-only cap for requests that have actually dispatched.
    pub max_inflight: usize,
    /// RTT estimator configuration.
    pub latency: LatencyEstimatorConfig,
    /// Fractional operating-point configuration.
    pub gradient: Gradient2Config,
    /// Randomized probe policy used by framework adapters.
    pub probe_schedule: ProbeSchedule,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4,
            max_inflight: 1024,
            latency: LatencyEstimatorConfig::default(),
            gradient: Gradient2Config::default(),
            probe_schedule: ProbeSchedule::default(),
        }
    }
}

impl EndpointConfig {
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    pub fn max_inflight(mut self, max_inflight: usize) -> Self {
        self.max_inflight = max_inflight;
        self
    }
}

/// Classification supplied by the caller after a request completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The response represents healthy work for this endpoint.
    Success,
    /// The response, transport error, timeout, or cancellation is unhealthy.
    Failure,
}

/// A request that has reserved a slot in the endpoint's virtual queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchReservation {
    controller_id: u64,
    id: u64,
}

/// A request that has actually been dispatched and is now counted against the
/// emergency inflight cap.
#[derive(Debug, PartialEq, Eq)]
pub struct InFlightRequest {
    controller_id: u64,
    id: u64,
    dispatched_at: Instant,
    was_paced: bool,
}

impl InFlightRequest {
    /// Returns the actual dispatch time used for RTT measurement.
    pub fn dispatched_at(&self) -> Instant {
        self.dispatched_at
    }

    /// Returns whether the request waited for a future GCRA slot.
    pub fn was_paced(&self) -> bool {
        self.was_paced
    }
}

/// The result of asking whether a virtual queue reservation may dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchState {
    Ready,
    WaitUntil(Instant),
    WaitForPrevious,
    InflightLimit,
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
struct PendingReservation {
    id: u64,
    scheduled_at: Instant,
    was_paced: bool,
}

/// A point-in-time view useful for metrics, tests, and P2C load prediction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ControllerSnapshot {
    pub expected_rtt: Duration,
    pub baseline_rtt: Duration,
    pub short_rtt: Duration,
    pub long_rtt: Duration,
    pub target_concurrency: f64,
    pub effective_concurrency: f64,
    pub base_rate: f64,
    pub effective_rate: f64,
    pub committed_tat: Instant,
    pub virtual_tail_tat: Option<Instant>,
    pub queued: usize,
    pub inflight: usize,
    pub queue_capacity: usize,
    pub max_inflight: usize,
    pub completed: u64,
    pub failures: u64,
    pub latency_samples: u64,
    pub active_probe: Option<Probe>,
}

/// The per-endpoint adaptive controller.
///
/// This type contains no async machinery and can be driven by a deterministic
/// simulator. `reserve` creates a virtual GCRA slot, while `on_dispatched`
/// commits a slot only when the underlying service is genuinely ready.
#[derive(Debug)]
pub struct EndpointController {
    controller_id: u64,
    config: EndpointConfig,
    latency: LatencyEstimator,
    gradient: Gradient2,
    pacer: Gcra,
    probe: ProbeState,
    pending: VecDeque<PendingReservation>,
    active_requests: HashSet<u64>,
    next_id: u64,
    queued: usize,
    inflight: usize,
    completed: u64,
    failures: u64,
}

impl EndpointController {
    pub fn new(config: EndpointConfig, now: Instant) -> Self {
        assert!(
            config.queue_capacity > 0,
            "endpoint queue capacity must be positive"
        );
        assert!(config.max_inflight > 0, "max inflight must be positive");

        let latency = LatencyEstimator::new(config.latency.clone());
        let gradient = Gradient2::new(config.gradient.clone());
        let rate = gradient.concurrency() / latency.expected_rtt().as_secs_f64();
        let controller_id = NEXT_CONTROLLER_ID.fetch_add(1, Ordering::Relaxed);

        Self {
            controller_id,
            config,
            latency,
            gradient,
            pacer: Gcra::new(rate, now),
            probe: ProbeState::new(),
            pending: VecDeque::new(),
            active_requests: HashSet::new(),
            next_id: 0,
            queued: 0,
            inflight: 0,
            completed: 0,
            failures: 0,
        }
    }

    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }

    pub fn pacer(&self) -> &Gcra {
        &self.pacer
    }

    pub fn gradient(&self) -> &Gradient2 {
        &self.gradient
    }

    pub fn latency(&self) -> &LatencyEstimator {
        &self.latency
    }

    pub fn probe(&self) -> &ProbeState {
        &self.probe
    }

    pub fn queued(&self) -> usize {
        self.queued
    }

    pub fn inflight(&self) -> usize {
        self.inflight
    }

    pub fn may_schedule(&self) -> bool {
        self.queued < self.config.queue_capacity
    }

    /// Reserves one bounded scheduling slot and appends it to the virtual
    /// pacing queue.
    pub fn reserve(&mut self, now: Instant) -> Result<DispatchReservation, ScheduleError> {
        if !self.may_schedule() {
            return Err(ScheduleError::QueueFull);
        }

        let scheduled_at = self.next_virtual_slot(now);
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(crate::ScheduleError::IdExhausted)?;
        self.pending.push_back(PendingReservation {
            id,
            scheduled_at,
            was_paced: scheduled_at > now,
        });
        self.queued += 1;

        Ok(DispatchReservation {
            controller_id: self.controller_id,
            id,
        })
    }

    /// Drops an accepted but not-yet-dispatched request.
    ///
    /// Cancellation is safe even when an earlier request remains queued: the
    /// virtual tail is rebuilt from the committed TAT, so a cancelled hole
    /// cannot permanently throttle the endpoint.
    pub fn cancel(&mut self, reservation: DispatchReservation, now: Instant) -> bool {
        if reservation.controller_id != self.controller_id {
            return false;
        }

        let Some(position) = self
            .pending
            .iter()
            .position(|entry| entry.id == reservation.id)
        else {
            return false;
        };

        self.pending.remove(position);
        self.queued -= 1;
        self.rebuild_virtual_queue(now);
        true
    }

    pub fn dispatch_state(
        &mut self,
        reservation: DispatchReservation,
        now: Instant,
    ) -> DispatchState {
        if reservation.controller_id != self.controller_id {
            return DispatchState::Cancelled;
        }

        let Some(front) = self.pending.front() else {
            return DispatchState::Cancelled;
        };
        if front.id != reservation.id {
            if self.pending.iter().any(|entry| entry.id == reservation.id) {
                return DispatchState::WaitForPrevious;
            }
            return DispatchState::Cancelled;
        }
        if self.inflight >= self.config.max_inflight {
            return DispatchState::InflightLimit;
        }
        if front.scheduled_at > now {
            return DispatchState::WaitUntil(front.scheduled_at);
        }
        DispatchState::Ready
    }

    /// Commits the oldest reservation after the underlying service reports
    /// readiness. Dispatches are intentionally FIFO inside one endpoint.
    pub fn on_dispatched(
        &mut self,
        reservation: DispatchReservation,
        now: Instant,
    ) -> Option<InFlightRequest> {
        if self.dispatch_state(reservation, now) != DispatchState::Ready {
            return None;
        }

        let pending = self
            .pending
            .pop_front()
            .expect("dispatch state checked the queue");
        debug_assert_eq!(pending.id, reservation.id);
        self.queued -= 1;
        self.inflight += 1;
        debug_assert!(self.active_requests.insert(reservation.id));
        self.pacer.commit(now);
        self.rebuild_virtual_queue(now);

        Some(InFlightRequest {
            controller_id: self.controller_id,
            id: reservation.id,
            dispatched_at: now,
            was_paced: pending.was_paced,
        })
    }

    /// Records a response and updates the endpoint's operating point.
    pub fn on_complete(
        &mut self,
        request: InFlightRequest,
        outcome: Outcome,
        latency: Duration,
        now: Instant,
    ) -> bool {
        if request.controller_id != self.controller_id || !self.active_requests.remove(&request.id)
        {
            return false;
        }

        debug_assert_eq!(self.inflight, self.active_requests.len() + 1);
        self.completed += 1;

        match outcome {
            Outcome::Success => {
                self.latency.observe_at(latency, now);
                // Keep the congestion reference anchored to the endpoint's
                // minimum RTT. A per-client long EWMA can absorb shared
                // queueing and let an incumbent retain an unfair share when
                // a new client joins.
                self.gradient.on_rtt_with_baseline_at(
                    self.latency.short(),
                    self.latency.baseline(),
                    self.inflight,
                    request.was_paced(),
                    now,
                );
            }
            Outcome::Failure => {
                self.failures += 1;
                self.gradient.on_failure_at(now);
            }
        }

        self.inflight -= 1;
        self.update_rate(now);
        true
    }

    /// Records an error obtained while the transport was being made ready.
    /// No request was dispatched, so this does not alter inflight accounting,
    /// but the endpoint is still penalized for future scheduling.
    pub fn on_admission_failure(&mut self, now: Instant) {
        self.failures += 1;
        self.gradient.on_failure_at(now);
        self.update_rate(now);
    }

    /// Starts a temporary positive probe.
    pub fn start_positive_probe(&mut self, delta: f64, until: Instant, now: Instant) {
        self.probe.start_positive(delta, until);
        self.update_rate(now);
    }

    /// Starts a temporary negative probe.
    pub fn start_negative_probe(&mut self, factor: f64, until: Instant, now: Instant) {
        self.probe.start_negative(factor, until);
        self.update_rate(now);
    }

    /// Gives a seeded or production RNG a chance to start a temporary probe.
    pub fn maybe_start_probe<R: Rng + ?Sized>(
        &mut self,
        schedule: &ProbeSchedule,
        rng: &mut R,
        now: Instant,
    ) -> Option<Probe> {
        let previous = self.probe.current();
        let probe = schedule.maybe_start(&mut self.probe, rng, now);
        if previous != self.probe.current() {
            self.update_rate(now);
        }
        probe
    }

    /// Expires a probe, if necessary, and updates the pacer to the base rate.
    pub fn refresh(&mut self, now: Instant) {
        let _ = self.probe.active(now);
        self.update_rate(now);
    }

    /// Predicts when one additional request would complete if current
    /// conditions remain stable.
    pub fn predicted_completion(&self, now: Instant) -> Instant {
        let dispatch = self.next_virtual_slot(now);
        saturating_add(dispatch, self.latency.expected_rtt())
    }

    /// Returns a scalar suitable for comparing endpoints. Lower is better.
    pub fn load(&self, now: Instant) -> f64 {
        self.predicted_completion(now)
            .saturating_duration_since(now)
            .as_secs_f64()
    }

    pub fn snapshot(&mut self, now: Instant) -> ControllerSnapshot {
        self.refresh(now);
        let target = self.gradient.concurrency();
        let effective = self.probe.effective_concurrency(target, now);
        let expected = self.latency.expected_rtt().as_secs_f64();
        let base_rate = target / expected;
        let effective_rate = effective / expected;

        ControllerSnapshot {
            expected_rtt: self.latency.expected_rtt(),
            baseline_rtt: self.latency.baseline(),
            short_rtt: self.latency.short(),
            long_rtt: self.latency.long(),
            target_concurrency: target,
            effective_concurrency: effective,
            base_rate,
            effective_rate,
            committed_tat: self.pacer.tat(),
            virtual_tail_tat: self
                .pending
                .back()
                .map(|entry| saturating_add(entry.scheduled_at, self.pacer.interval())),
            queued: self.queued,
            inflight: self.inflight,
            queue_capacity: self.config.queue_capacity,
            max_inflight: self.config.max_inflight,
            completed: self.completed,
            failures: self.failures,
            latency_samples: self.latency.samples(),
            active_probe: self.probe.current(),
        }
    }

    fn next_virtual_slot(&self, now: Instant) -> Instant {
        self.pending
            .back()
            .map(|entry| saturating_add(entry.scheduled_at, self.pacer.interval()))
            .unwrap_or_else(|| self.pacer.next_at(now))
            .max(now)
    }

    fn update_rate(&mut self, now: Instant) {
        let target = self.gradient.concurrency();
        let effective = self.probe.effective_concurrency(target, now);
        let rate = effective / self.latency.expected_rtt().as_secs_f64();
        self.pacer.set_rate(rate, now);
        self.rebuild_virtual_queue(now);
    }

    fn rebuild_virtual_queue(&mut self, now: Instant) {
        let mut next = self.pacer.next_at(now);
        for entry in &mut self.pending {
            entry.scheduled_at = next;
            next = saturating_add(next, self.pacer.interval());
        }
    }
}
