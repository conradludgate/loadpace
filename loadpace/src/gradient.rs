use crate::gcra::saturating_add;
use std::time::{Duration, Instant};

/// Parameters for the fractional Gradient2 controller.
#[derive(Clone, Debug)]
pub struct Gradient2Config {
    /// Initial virtual concurrency.
    pub initial_concurrency: f64,
    /// Lower bound for the virtual concurrency.
    pub min_concurrency: f64,
    /// Upper bound for the virtual concurrency.
    pub max_concurrency: f64,
    /// RTT inflation tolerated before reducing the operating point.
    pub tolerance: f64,
    /// Additive queue allowance used when latency is healthy.
    pub gain: f64,
    /// Smoothing applied to each new operating-point estimate.
    pub smoothing: f64,
    /// Minimum time between healthy operating-point updates.
    pub update_interval: Duration,
    /// Multiplier used after a classified failure.
    pub failure_factor: f64,
}

impl Default for Gradient2Config {
    fn default() -> Self {
        Self {
            initial_concurrency: 1.0,
            min_concurrency: 0.25,
            max_concurrency: 1024.0,
            tolerance: 1.5,
            gain: 0.1,
            smoothing: 0.2,
            update_interval: Duration::from_millis(100),
            failure_factor: 0.7,
        }
    }
}

/// A fractional Gradient2 operating-point controller.
///
/// Unlike an integer concurrency limiter, this controller deliberately keeps
/// values such as `1.3`. The endpoint controller turns that value into a rate
/// with Little's Law and lets GCRA enforce the rate.
#[derive(Clone, Debug)]
pub struct Gradient2 {
    config: Gradient2Config,
    concurrency: f64,
    last_gradient: f64,
    updates: u64,
    next_update_at: Option<Instant>,
}

impl Gradient2 {
    /// Creates a fractional Gradient2 controller.
    ///
    /// # Panics
    ///
    /// Panics when the concurrency bounds, tolerance, gain, smoothing,
    /// update interval, or failure factor is invalid.
    pub fn new(config: Gradient2Config) -> Self {
        assert!(
            config.min_concurrency.is_finite()
                && config.min_concurrency > 0.0
                && config.max_concurrency.is_finite()
                && config.max_concurrency >= config.min_concurrency,
            "Gradient2 concurrency bounds must be finite and ordered"
        );
        assert!(
            config.initial_concurrency >= config.min_concurrency
                && config.initial_concurrency <= config.max_concurrency,
            "initial concurrency must be within the Gradient2 bounds"
        );
        assert!(
            config.tolerance.is_finite()
                && config.tolerance >= 1.0
                && config.gain.is_finite()
                && config.gain > 0.0,
            "Gradient2 tolerance must be finite and >= 1 and gain must be positive"
        );
        assert!(
            config.smoothing.is_finite()
                && (0.0..=1.0).contains(&config.smoothing)
                && config.smoothing > 0.0,
            "Gradient2 smoothing must be finite and in (0, 1]"
        );
        assert!(
            !config.update_interval.is_zero(),
            "Gradient2 update interval must be positive"
        );
        assert!(
            (0.0..=1.0).contains(&config.failure_factor) && config.failure_factor > 0.0,
            "failure factor must be in (0, 1]"
        );

        Self {
            concurrency: config.initial_concurrency,
            config,
            last_gradient: 1.0,
            updates: 0,
            next_update_at: None,
        }
    }

    /// Updates the operating point from current and long-term RTT estimates.
    ///
    /// A sample from an application-limited caller is still useful for
    /// diagnostics, but it must not increase the operating point: low demand
    /// is not evidence that the endpoint has spare capacity.
    pub fn on_rtt(&mut self, current_rtt: Duration, long_rtt: Duration, inflight: usize) -> bool {
        self.on_rtt_at(current_rtt, long_rtt, inflight, Instant::now())
    }

    /// Updates the operating point at an explicit time.
    ///
    /// This is the deterministic form of [`Self::on_rtt`]. Healthy operating
    /// point changes are limited by [`Gradient2Config::update_interval`], so
    /// a high response rate cannot make the controller adapt proportionally
    /// faster than a low response rate.
    pub fn on_rtt_at(
        &mut self,
        current_rtt: Duration,
        long_rtt: Duration,
        inflight: usize,
        now: Instant,
    ) -> bool {
        self.on_rtt_with_pacing_at(current_rtt, long_rtt, inflight, true, now)
    }

    /// Updates the operating point with an explicit pacing signal.
    ///
    /// `was_paced` is true when the request had to wait for a future GCRA
    /// slot. A healthy request that arrived while the pacer was idle is
    /// application-limited and must not cause additive growth.
    pub fn on_rtt_with_pacing(
        &mut self,
        current_rtt: Duration,
        long_rtt: Duration,
        inflight: usize,
        was_paced: bool,
    ) -> bool {
        self.on_rtt_with_pacing_at(current_rtt, long_rtt, inflight, was_paced, Instant::now())
    }

    /// Updates the operating point with an explicit pacing signal and time.
    pub fn on_rtt_with_pacing_at(
        &mut self,
        current_rtt: Duration,
        long_rtt: Duration,
        inflight: usize,
        was_paced: bool,
        now: Instant,
    ) -> bool {
        self.on_rtt_with_reference_at(current_rtt, long_rtt, inflight, was_paced, now)
    }

    /// Updates the operating point using a minimum-RTT reference.
    ///
    /// Endpoint controllers use this form so an incumbent's queue-inflated
    /// long RTT cannot become permission to keep its share after a new client
    /// joins. The reference remains anchored to the endpoint's observed
    /// minimum while `current_rtt` reacts quickly to shared queueing.
    pub fn on_rtt_with_baseline(
        &mut self,
        current_rtt: Duration,
        baseline_rtt: Duration,
        inflight: usize,
        was_paced: bool,
    ) -> bool {
        self.on_rtt_with_baseline_at(
            current_rtt,
            baseline_rtt,
            inflight,
            was_paced,
            Instant::now(),
        )
    }

    /// Updates the operating point against a minimum-RTT reference at an
    /// explicit time.
    pub fn on_rtt_with_baseline_at(
        &mut self,
        current_rtt: Duration,
        baseline_rtt: Duration,
        inflight: usize,
        was_paced: bool,
        now: Instant,
    ) -> bool {
        self.on_rtt_with_reference_at(current_rtt, baseline_rtt, inflight, was_paced, now)
    }

    fn on_rtt_with_reference_at(
        &mut self,
        current_rtt: Duration,
        reference_rtt: Duration,
        inflight: usize,
        was_paced: bool,
        now: Instant,
    ) -> bool {
        let current_rtt = current_rtt.as_secs_f64().max(f64::MIN_POSITIVE);
        let reference_rtt = reference_rtt.as_secs_f64().max(f64::MIN_POSITIVE);
        let application_limited = !was_paced || (inflight as f64) < self.concurrency / 2.0;
        if application_limited {
            return false;
        }

        // Bound the gradient so a single outlier cannot halve the limit more
        // than once, while a healthy sample can recover toward the current
        // operating point.
        let gradient = (self.config.tolerance * reference_rtt / current_rtt).clamp(0.5, 1.0);
        self.last_gradient = gradient;
        if self
            .next_update_at
            .is_some_and(|next_update_at| now < next_update_at)
        {
            return false;
        }

        let estimate = self.concurrency * gradient + self.config.gain;
        self.concurrency = (self.concurrency * (1.0 - self.config.smoothing)
            + estimate * self.config.smoothing)
            .clamp(self.config.min_concurrency, self.config.max_concurrency);
        self.updates += 1;
        self.next_update_at = Some(saturating_add(now, self.config.update_interval));
        true
    }

    /// Reduces the operating point after an outcome classified as unhealthy.
    pub fn on_failure(&mut self) {
        self.on_failure_at(Instant::now());
    }

    /// Reduces the operating point at an explicit time.
    pub fn on_failure_at(&mut self, now: Instant) {
        self.concurrency = (self.concurrency * self.config.failure_factor)
            .clamp(self.config.min_concurrency, self.config.max_concurrency);
        self.last_gradient = 0.0;
        self.updates += 1;
        self.next_update_at = Some(saturating_add(now, self.config.update_interval));
    }

    pub fn concurrency(&self) -> f64 {
        self.concurrency
    }

    pub fn last_gradient(&self) -> f64 {
        self.last_gradient
    }

    pub fn updates(&self) -> u64 {
        self.updates
    }
}
