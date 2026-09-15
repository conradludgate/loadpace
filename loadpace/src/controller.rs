use crate::ScheduleError;
use crate::gcra::{Gcra, saturating_add};
use crate::gradient::{Gradient2, Gradient2Config};
use crate::latency::{LatencyEstimator, LatencyEstimatorConfig};
use crate::probe::{Probe, ProbeSchedule, ProbeState};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::VecDeque;
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
    /// Randomized probe policy used by the controller during normal dispatch
    /// and load-selection refreshes.
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
    /// Sets the maximum number of accepted but not-yet-dispatched requests.
    ///
    /// This is a bounded scheduling horizon rather than an overload buffer.
    /// A controller rejects further reservations once the capacity is used.
    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the emergency cap on requests that have actually dispatched.
    ///
    /// Normal traffic should be controlled by pacing before this limit is
    /// reached. The cap protects against pathological latency and stuck work.
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
    /// The reservation is at the queue head and may dispatch immediately.
    Ready,
    /// The controller should be checked again at this instant.
    ///
    /// This may be the reservation's pacing deadline or an earlier internal
    /// controller transition, such as a probe starting or ending.
    WaitUntil(Instant),
    /// An earlier reservation must dispatch or be cancelled first.
    WaitForPrevious,
    /// The endpoint's emergency inflight cap is currently full.
    InflightLimit,
    /// The reservation is foreign, was cancelled, or no longer exists.
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
    /// Long-term RTT used to convert the concurrency target into a base rate.
    pub expected_rtt: Duration,
    /// Minimum RTT observed within the rolling baseline window.
    pub baseline_rtt: Duration,
    /// Short-term exponentially weighted RTT estimate.
    pub short_rtt: Duration,
    /// Long-term exponentially weighted RTT estimate.
    pub long_rtt: Duration,
    /// Fractional concurrency selected by the Gradient2 controller.
    pub target_concurrency: f64,
    /// Concurrency implied by the current rate, including any active probe.
    pub effective_concurrency: f64,
    /// Requests per second derived from target concurrency and expected RTT.
    pub base_rate: f64,
    /// Current requests per second after applying an active probe, if any.
    pub effective_rate: f64,
    /// The GCRA theoretical arrival time committed by actual dispatches.
    pub committed_tat: Instant,
    /// Predicted TAT after all current virtual queue reservations.
    ///
    /// This is [`None`] when the virtual queue is empty.
    pub virtual_tail_tat: Option<Instant>,
    /// Number of accepted but not-yet-dispatched reservations.
    pub queued: usize,
    /// Number of dispatched requests awaiting completion.
    pub inflight: usize,
    /// Configured maximum number of queued reservations.
    pub queue_capacity: usize,
    /// Configured emergency cap on dispatched requests.
    pub max_inflight: usize,
    /// Number of dispatched requests reported as completed.
    pub completed: u64,
    /// Number of unhealthy completions and transport admission failures.
    pub failures: u64,
    /// Number of successful RTT samples observed by the latency estimator.
    pub latency_samples: u64,
    /// Temporary probe active at the time of the snapshot, if any.
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
    next_id: u64,
    inflight: usize,
    completed: u64,
    failures: u64,
    probe_rng: StdRng,
}

impl EndpointController {
    /// Creates an endpoint controller with an initially available slot.
    ///
    /// # Panics
    ///
    /// Panics when the queue capacity or emergency inflight cap is zero, or
    /// when the nested latency or Gradient2 configuration is invalid.
    pub fn new(config: EndpointConfig, now: Instant) -> Self {
        Self::new_with_rng(config, now, rand::make_rng())
    }

    /// Creates a controller with deterministic probe entropy.
    ///
    /// This is useful for simulations and tests. The controller still owns
    /// all probe decisions; the seed only makes those decisions reproducible.
    ///
    /// # Panics
    ///
    /// Panics under the same invalid configurations as [`Self::new`].
    pub fn new_with_seed(config: EndpointConfig, now: Instant, seed: u64) -> Self {
        Self::new_with_rng(config, now, StdRng::seed_from_u64(seed))
    }

    fn new_with_rng(config: EndpointConfig, now: Instant, probe_rng: StdRng) -> Self {
        assert!(
            config.queue_capacity > 0,
            "endpoint queue capacity must be positive"
        );
        assert!(config.max_inflight > 0, "max inflight must be positive");

        config.probe_schedule.validate();
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
            next_id: 0,
            inflight: 0,
            completed: 0,
            failures: 0,
            probe_rng,
        }
    }

    /// Returns the configuration owned by this controller.
    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }

    /// Returns the controller's GCRA pacer for diagnostics.
    pub fn pacer(&self) -> &Gcra {
        &self.pacer
    }

    /// Returns the controller's Gradient2 state for diagnostics.
    pub fn gradient(&self) -> &Gradient2 {
        &self.gradient
    }

    /// Returns the controller's latency estimator for diagnostics.
    pub fn latency(&self) -> &LatencyEstimator {
        &self.latency
    }

    /// Returns the number of accepted but not-yet-dispatched reservations.
    pub fn queued(&self) -> usize {
        self.pending.len()
    }

    /// Returns the number of dispatched requests awaiting completion.
    pub fn inflight(&self) -> usize {
        self.inflight
    }

    /// Returns whether the bounded virtual queue can accept a reservation.
    ///
    /// This does not guarantee immediate dispatch: the request may still wait
    /// for its pacing deadline, earlier reservations, or inflight capacity.
    pub fn may_schedule(&self) -> bool {
        self.pending.len() < self.config.queue_capacity
    }

    /// Reserves one bounded scheduling slot and appends it to the virtual
    /// pacing queue.
    ///
    /// A successful reservation must later be passed to [`Self::on_dispatched`]
    /// or [`Self::cancel`]. Reservations belong to the controller that created
    /// them and are dispatched FIFO.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::QueueFull`] when the scheduling horizon is
    /// full, or [`ScheduleError::IdExhausted`] if the controller cannot assign
    /// another reservation identity.
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
            // A future prediction is not evidence that the request actually
            // waited. An earlier reservation may still be cancelled before
            // this one reaches the head of the queue.
            was_paced: false,
        });
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
    ///
    /// Returns `true` when the reservation was removed. Returns `false` when
    /// it belongs to another controller or is no longer queued.
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
        self.rebuild_virtual_queue(now);
        true
    }

    /// Refreshes controller policy and reports a reservation's current state.
    ///
    /// Callers should wait until the instant from [`DispatchState::WaitUntil`]
    /// before polling again. [`DispatchState::WaitForPrevious`] is woken by an
    /// earlier dispatch or cancellation, while [`DispatchState::InflightLimit`]
    /// is woken by a completion.
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
        // Keep all time-driven policy in the controller. Framework adapters
        // only need to arrange a wakeup for the deadline returned below.
        self.refresh(now);
        let front = self
            .pending
            .front()
            .expect("validated reservation must remain in the queue");
        if self.inflight >= self.config.max_inflight {
            return DispatchState::InflightLimit;
        }
        let scheduled_at = front.scheduled_at;
        if scheduled_at > now {
            self.pending
                .front_mut()
                .expect("validated reservation must remain in the queue")
                .was_paced = true;
            let wake_at = self
                .next_dispatch_refresh_at()
                .map_or(scheduled_at, |refresh_at| scheduled_at.min(refresh_at));
            return DispatchState::WaitUntil(wake_at);
        }
        DispatchState::Ready
    }

    /// Commits the oldest reservation after the underlying service reports
    /// readiness. Dispatches are intentionally FIFO inside one endpoint.
    /// Returns the reservation's current state without changing it when it is
    /// not dispatchable.
    ///
    /// # Errors
    ///
    /// Returns the current [`DispatchState`] unless the reservation is
    /// [`DispatchState::Ready`]. In particular, callers must not treat an
    /// earlier pacing decision as a permanent readiness grant because
    /// time-driven controller state may have changed.
    pub fn on_dispatched(
        &mut self,
        reservation: DispatchReservation,
        now: Instant,
    ) -> Result<InFlightRequest, DispatchState> {
        let state = self.dispatch_state(reservation, now);
        if state != DispatchState::Ready {
            return Err(state);
        }

        let pending = self
            .pending
            .pop_front()
            .expect("ready reservation must be at the front of the queue");
        debug_assert_eq!(pending.id, reservation.id);
        self.inflight += 1;
        self.pacer.commit(now);
        self.rebuild_virtual_queue(now);

        Ok(InFlightRequest {
            controller_id: self.controller_id,
            dispatched_at: now,
            was_paced: pending.was_paced,
        })
    }

    /// Records a response and updates the endpoint's operating point.
    ///
    /// `latency` must measure actual dispatch to completion and exclude time
    /// spent in local admission, pacing, or service-readiness waits. Successful
    /// outcomes update the RTT estimator; failures reduce the operating point
    /// without treating a fast error as healthy latency.
    ///
    /// Returns `false` without changing state when `request` belongs to another
    /// controller. Returns `true` after consuming a request owned by this one.
    pub fn on_complete(
        &mut self,
        request: InFlightRequest,
        outcome: Outcome,
        latency: Duration,
        now: Instant,
    ) -> bool {
        if request.controller_id != self.controller_id {
            return false;
        }

        debug_assert!(self.inflight > 0);
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

    /// Expires probes, lets the controller make a probe decision, and
    /// refreshes the derived pacing rate.
    ///
    /// Framework adapters should call this through ordinary dispatch or load
    /// operations rather than running a separate application probe task.
    pub fn refresh(&mut self, now: Instant) {
        // Refresh is called by normal dispatch and load-metric paths. Once an
        // endpoint has produced a sample, keeping the schedule here means an
        // endpoint that P2C is beginning to avoid can still receive a probe.
        // Before that first observation, require an actual queued dispatch so
        // the initial request is not treated as a probe opportunity.
        if self.latency.samples() > 0 || (self.inflight > 0 && !self.pending.is_empty()) {
            self.config
                .probe_schedule
                .maybe_start(&mut self.probe, &mut self.probe_rng, now);
        }
        let _ = self.probe.active(now);
        self.update_rate(now);
    }

    /// Returns the currently active probe, if any.
    pub fn active_probe(&self) -> Option<Probe> {
        self.probe.current()
    }

    /// Predicts when one additional request would complete if current
    /// conditions remain stable.
    ///
    /// The prediction includes the virtual queue tail and expected endpoint
    /// RTT. Reading it refreshes time-driven probe state.
    pub fn predicted_completion(&mut self, now: Instant) -> Instant {
        self.refresh(now);
        let dispatch = self.next_virtual_slot(now);
        saturating_add(dispatch, self.latency.expected_rtt())
    }

    /// Returns a scalar suitable for comparing endpoints. Lower is better.
    ///
    /// The value is the predicted completion delay in seconds from `now`.
    pub fn load(&mut self, now: Instant) -> f64 {
        self.predicted_completion(now)
            .saturating_duration_since(now)
            .as_secs_f64()
    }

    /// Refreshes time-driven state and returns a point-in-time metrics view.
    ///
    /// Calling this method may expire or start a probe, and can therefore
    /// update the effective pacing rate even though it does not reserve work.
    pub fn snapshot(&mut self, now: Instant) -> ControllerSnapshot {
        self.refresh(now);
        let target = self.gradient.concurrency();
        let expected = self.latency.expected_rtt().as_secs_f64();
        let base_rate = target / expected;
        let effective_rate = self.probe.effective_rate(base_rate, now);
        let effective = effective_rate * expected;

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
            queued: self.pending.len(),
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

    fn next_dispatch_refresh_at(&self) -> Option<Instant> {
        if let Some(probe) = self.probe.current() {
            return Some(probe.until);
        }

        let probing_has_demand =
            self.latency.samples() > 0 || (self.inflight > 0 && !self.pending.is_empty());
        if probing_has_demand {
            self.probe.next_probe_at()
        } else {
            None
        }
    }

    fn update_rate(&mut self, now: Instant) {
        let target = self.gradient.concurrency();
        let base_rate = target / self.latency.expected_rtt().as_secs_f64();
        let rate = self.probe.effective_rate(base_rate, now);
        let previous_interval = self.pacer.interval();
        self.pacer.set_rate(rate, now);
        let next = self.pacer.next_at(now);
        let virtual_head_expired = self
            .pending
            .front()
            .is_some_and(|entry| entry.scheduled_at < next);
        if self.pacer.interval() != previous_interval || virtual_head_expired {
            self.rebuild_virtual_queue(now);
        }
    }

    fn rebuild_virtual_queue(&mut self, now: Instant) {
        let mut next = self.pacer.next_at(now);
        for entry in &mut self.pending {
            entry.scheduled_at = next;
            next = saturating_add(next, self.pacer.interval());
        }
    }
}
