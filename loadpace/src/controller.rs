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

const SHORT_RTT_HALF_LIVES: f64 = 2.0;
const LONG_RTT_HALF_LIVES: f64 = 20.0;
const BASELINE_RTT_WINDOWS: u32 = 1_200;

/// Configuration for one adaptive endpoint.
#[derive(Clone, Debug)]
pub struct EndpointConfig {
    expected_rtt: Duration,
    initial_concurrency: usize,
    queue_tolerance: Duration,
    queue_capacity: usize,
    max_inflight: usize,
}

impl EndpointConfig {
    /// Creates endpoint configuration from workload-level assumptions.
    ///
    /// `expected_rtt` is the typical dispatch-to-response round-trip time
    /// before the controller has observations. `initial_concurrency` is the
    /// number of requests the endpoint is expected to sustain concurrently at
    /// startup. Together they establish the initial rate using Little's Law.
    ///
    /// Algorithm-specific estimator, congestion-control, and probe parameters
    /// are derived internally so applications are not coupled to a particular
    /// controller implementation.
    ///
    /// # Panics
    ///
    /// Panics if `expected_rtt` or `initial_concurrency` is zero.
    #[must_use]
    pub fn new(expected_rtt: Duration, initial_concurrency: usize) -> Self {
        assert!(!expected_rtt.is_zero(), "expected RTT must be positive");
        assert!(
            initial_concurrency > 0,
            "initial concurrency must be positive"
        );
        Self {
            expected_rtt,
            initial_concurrency,
            queue_tolerance: expected_rtt / 2,
            queue_capacity: 4,
            max_inflight: initial_concurrency.saturating_mul(4).max(1024),
        }
    }

    /// Returns the initial dispatch-to-response RTT estimate.
    #[must_use]
    pub fn expected_rtt(&self) -> Duration {
        self.expected_rtt
    }

    /// Returns the initial sustainable concurrency assumption.
    #[must_use]
    pub fn initial_concurrency(&self) -> usize {
        self.initial_concurrency
    }

    /// Returns the queueing delay tolerated by congestion control.
    #[must_use]
    pub fn queue_tolerance(&self) -> Duration {
        self.queue_tolerance
    }

    /// Returns the bounded scheduling horizon per endpoint.
    #[must_use]
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Returns the emergency cap on dispatched requests.
    #[must_use]
    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    /// Sets the queueing delay tolerated before reducing the operating point.
    ///
    /// The default is half of `expected_rtt`. This is an absolute delay budget,
    /// not a multiplier, so callers can align it with a service-level target.
    #[must_use]
    pub fn with_queue_tolerance(mut self, tolerance: Duration) -> Self {
        self.queue_tolerance = tolerance;
        self
    }

    /// Sets the maximum number of accepted but not-yet-dispatched requests.
    ///
    /// This is a bounded scheduling horizon rather than an overload buffer.
    /// A controller rejects further reservations once the capacity is used.
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the emergency cap on requests that have actually dispatched.
    ///
    /// Normal traffic should be controlled by pacing before this limit is
    /// reached. The cap protects against pathological latency and stuck work.
    #[must_use]
    pub fn with_max_inflight(mut self, max_inflight: usize) -> Self {
        self.max_inflight = max_inflight;
        self
    }

    fn latency_config(&self) -> LatencyEstimatorConfig {
        let expected_samples_per_rtt = self.initial_concurrency as f64;
        LatencyEstimatorConfig {
            initial_rtt: self.expected_rtt,
            short_alpha: ewma_alpha(expected_samples_per_rtt * SHORT_RTT_HALF_LIVES),
            long_alpha: ewma_alpha(expected_samples_per_rtt * LONG_RTT_HALF_LIVES),
            min_rtt: Duration::from_micros(1).min(self.expected_rtt),
            baseline_window: self
                .expected_rtt
                .saturating_mul(BASELINE_RTT_WINDOWS)
                .max(Duration::from_secs(60)),
        }
    }

    fn gradient_config(&self) -> Gradient2Config {
        Gradient2Config {
            initial_concurrency: self.initial_concurrency as f64,
            min_concurrency: 0.25,
            max_concurrency: self.max_inflight as f64,
            queue_tolerance: self.queue_tolerance,
            gain: 0.1,
            smoothing: 0.2,
            update_interval: self.expected_rtt.saturating_mul(2),
            failure_factor: 0.7,
        }
    }

    fn probe_schedule(&self) -> ProbeSchedule {
        ProbeSchedule::default()
    }
}

fn ewma_alpha(samples_per_half_life: f64) -> f64 {
    1.0 - 0.5_f64.powf(samples_per_half_life.recip())
}

/// Classification supplied by the caller after a request completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The response represents healthy work for this endpoint.
    Success,
    /// Endpoint feedback indicates an unhealthy response or transport error.
    ///
    /// Use [`EndpointController::on_abandoned`] instead when a request ends
    /// without receiving endpoint feedback, such as after cancellation or a
    /// client-side timeout.
    Failure,
}

/// How a dispatched request ended, for [`EndpointController::finish`].
///
/// Classify endpoint feedback separately from a local timeout or cancellation:
/// only real feedback resets the controller's missing-feedback clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Completion {
    /// Healthy endpoint feedback; observe the dispatch-to-completion RTT.
    Success,
    /// Unhealthy endpoint feedback; penalize the operating point without an RTT sample.
    Failure,
    /// No endpoint feedback, such as a local timeout or cancellation.
    ///
    /// Release inflight accounting and penalize the operating point, preserving
    /// the missing-feedback clock while other requests remain inflight.
    Abandoned,
}

/// Coalesced wakeup effects returned by [`EndpointController::take_changes`].
///
/// These flags request a state recheck, not a readiness grant. Multiple changes
/// can accumulate before they are taken, including changes that undo one another.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControllerChanges {
    /// Space was released from the bounded scheduling horizon.
    ///
    /// Wake callers waiting to reserve a slot. Other callers may have consumed
    /// the space since it was released, so admission must still be checked.
    pub admission: bool,
    /// Queued requests should recheck their dispatch state.
    ///
    /// FIFO order, pacing deadlines, probe transitions, or inflight capacity
    /// may have changed. This flag can be conservative.
    pub dispatch: bool,
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

#[derive(Clone, Copy, Debug)]
struct FeedbackEpoch {
    last_feedback_at: Instant,
    expected_rtt: Duration,
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
    /// Requests per second after applying an active probe, if any.
    pub probed_rate: f64,
    /// Time elapsed since the current inflight epoch last received feedback.
    ///
    /// This is [`None`] when the endpoint has no inflight requests.
    pub feedback_silence: Option<Duration>,
    /// Exponential rate multiplier caused by missing feedback.
    ///
    /// The multiplier is `1.0` for the first expected RTT of silence, then
    /// halves for every additional expected RTT without feedback.
    pub feedback_factor: f64,
    /// Current requests per second after probes and missing-feedback decay.
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
    /// Number of unhealthy completions, abandoned requests, and transport
    /// admission failures.
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
    feedback_epoch: Option<FeedbackEpoch>,
    probe_rng: StdRng,
    changes: ControllerChanges,
}

impl EndpointController {
    /// Creates an endpoint controller with an initially available slot.
    ///
    /// # Panics
    ///
    /// Panics when the queue capacity or emergency inflight cap is zero, or
    /// when initial concurrency exceeds the emergency inflight cap.
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
        assert!(
            config.initial_concurrency <= config.max_inflight,
            "initial concurrency must not exceed max inflight"
        );

        config.probe_schedule().validate();
        let latency = LatencyEstimator::new(config.latency_config());
        let gradient = Gradient2::new(config.gradient_config());
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
            feedback_epoch: None,
            probe_rng,
            changes: ControllerChanges::default(),
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

    /// Takes and clears wakeup effects accumulated by controller operations.
    ///
    /// New controllers have no pending effects. Calling this method does not
    /// refresh policy or advance the clock. Effects also accumulate when
    /// [`Self::load`] or [`Self::snapshot`] changes time-driven pacing state.
    ///
    /// One adapter should own delivery: drain effects while holding its
    /// controller lock, then release the lock before waking waiters. Register
    /// waiters before inspecting state to avoid losing a concurrent wakeup.
    /// The FIFO head already checking [`Self::dispatch_state`] can consume its
    /// own refresh effects without waking itself; later reservations remain
    /// blocked until it dispatches or is cancelled.
    #[must_use]
    pub fn take_changes(&mut self) -> ControllerChanges {
        std::mem::take(&mut self.changes)
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
        self.changes.admission = true;
        self.changes.dispatch = true;
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
        if self.inflight == 0 {
            self.feedback_epoch = Some(FeedbackEpoch {
                last_feedback_at: now,
                expected_rtt: self.latency.expected_rtt(),
            });
        }
        self.inflight += 1;
        self.changes.admission = true;
        self.changes.dispatch = true;
        self.pacer.commit(now);
        self.rebuild_virtual_queue(now);

        Ok(InFlightRequest {
            controller_id: self.controller_id,
            dispatched_at: now,
            was_paced: pending.was_paced,
        })
    }

    /// Finishes a dispatched request using elapsed time from its dispatch token.
    ///
    /// Successful feedback observes `now - request.dispatched_at()`, clamped
    /// to zero if the clock moved backward. This excludes time before actual
    /// dispatch, including reservation and readiness waits. Use
    /// [`Self::on_complete`] when latency is measured separately.
    ///
    /// [`Completion::Failure`] represents real endpoint feedback; use
    /// [`Completion::Abandoned`] for a local timeout or cancellation without
    /// feedback. Neither records a healthy RTT sample.
    ///
    /// Returns `false` without changing state for a foreign request, or `true`
    /// after consuming a request owned by this controller.
    pub fn finish(
        &mut self,
        request: InFlightRequest,
        completion: Completion,
        now: Instant,
    ) -> bool {
        let latency = now.saturating_duration_since(request.dispatched_at());
        match completion {
            Completion::Success => self.on_complete(request, Outcome::Success, latency, now),
            Completion::Failure => self.on_complete(request, Outcome::Failure, latency, now),
            Completion::Abandoned => self.on_abandoned(request, now),
        }
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
        self.changes.dispatch = true;
        self.feedback_epoch = (self.inflight > 0).then(|| FeedbackEpoch {
            last_feedback_at: now,
            expected_rtt: self.latency.expected_rtt(),
        });
        self.update_rate(now);
        true
    }

    /// Records a dispatched request that ended without endpoint feedback.
    ///
    /// Abandonment releases inflight accounting and penalizes the learned
    /// operating point, but does not reset the missing-feedback clock while
    /// other requests remain inflight. This is appropriate for cancellation
    /// and timeouts where no response was observed.
    ///
    /// Returns `false` without changing state when `request` belongs to another
    /// controller. Returns `true` after consuming a request owned by this one.
    pub fn on_abandoned(&mut self, request: InFlightRequest, now: Instant) -> bool {
        if request.controller_id != self.controller_id {
            return false;
        }

        debug_assert!(self.inflight > 0);
        self.failures += 1;
        self.gradient.on_failure_at(now);
        self.inflight -= 1;
        self.changes.dispatch = true;
        if self.inflight == 0 {
            self.feedback_epoch = None;
        }
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
        let previous_refresh = self.next_dispatch_refresh_at();
        // Refresh is called by normal dispatch and load-metric paths. Once an
        // endpoint has produced a sample, keeping the schedule here means an
        // endpoint that P2C is beginning to avoid can still receive a probe.
        // Before that first observation, require an actual queued dispatch so
        // the initial request is not treated as a probe opportunity.
        if self.latency.samples() > 0 || (self.inflight > 0 && !self.pending.is_empty()) {
            self.config
                .probe_schedule()
                .maybe_start(&mut self.probe, &mut self.probe_rng, now);
        }
        let _ = self.probe.active(now);
        self.update_rate(now);
        self.changes.dispatch |= previous_refresh != self.next_dispatch_refresh_at();
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
        let (base_rate, probed_rate, feedback_factor, effective_rate) = self.rates_at(now);
        let effective = effective_rate * expected;

        ControllerSnapshot {
            expected_rtt: self.latency.expected_rtt(),
            baseline_rtt: self.latency.baseline(),
            short_rtt: self.latency.short(),
            long_rtt: self.latency.long(),
            target_concurrency: target,
            effective_concurrency: effective,
            base_rate,
            probed_rate,
            feedback_silence: self
                .feedback_epoch
                .map(|epoch| now.saturating_duration_since(epoch.last_feedback_at)),
            feedback_factor,
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
        let (_, _, _, rate) = self.rates_at(now);
        let previous_interval = self.pacer.interval();
        self.pacer.set_rate(rate, now);
        let next = self.pacer.next_at(now);
        let virtual_head_expired = self
            .pending
            .front()
            .is_some_and(|entry| entry.scheduled_at < next);
        if self.pacer.interval() != previous_interval || virtual_head_expired {
            self.changes.dispatch = true;
            self.rebuild_virtual_queue(now);
        }
    }

    fn rates_at(&mut self, now: Instant) -> (f64, f64, f64, f64) {
        let base_rate = self.gradient.concurrency() / self.latency.expected_rtt().as_secs_f64();
        let probed_rate = self.probe.effective_rate(base_rate, now);
        let feedback_factor = self.feedback_factor(now, probed_rate);
        let effective_rate = probed_rate * feedback_factor;
        (base_rate, probed_rate, feedback_factor, effective_rate)
    }

    fn feedback_factor(&self, now: Instant, rate: f64) -> f64 {
        let Some(epoch) = self.feedback_epoch else {
            return 1.0;
        };
        let silence = now.saturating_duration_since(epoch.last_feedback_at);
        let excess = silence.saturating_sub(epoch.expected_rtt);
        if excess.is_zero() {
            return 1.0;
        }

        let half_lives = excess.as_secs_f64() / epoch.expected_rtt.as_secs_f64();
        let factor = (-half_lives).exp2();
        // Keep GCRA's interval representable even after extreme silence. This
        // is effectively zero traffic while remaining a smooth rate decay.
        let minimum_rate = Duration::MAX.as_secs_f64().recip() * 2.0;
        factor.max((minimum_rate / rate).min(1.0))
    }

    fn rebuild_virtual_queue(&mut self, now: Instant) {
        let mut next = self.pacer.next_at(now);
        for entry in &mut self.pending {
            entry.scheduled_at = next;
            next = saturating_add(next, self.pacer.interval());
        }
    }
}
