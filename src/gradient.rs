use std::time::Duration;

/// Parameters for the fractional Gradient2-style controller.
#[derive(Clone, Debug)]
pub struct Gradient2Config {
    /// Initial virtual concurrency.
    pub initial_concurrency: f64,
    /// Lower bound for the virtual concurrency.
    pub min_concurrency: f64,
    /// Upper bound for the virtual concurrency.
    pub max_concurrency: f64,
    /// Target RTT inflation tolerated before reducing the operating point.
    pub tolerance: f64,
    /// Additive step applied to the fractional operating point.
    pub gain: f64,
    /// Multiplier used after a classified failure.
    pub failure_factor: f64,
}

impl Default for Gradient2Config {
    fn default() -> Self {
        Self {
            initial_concurrency: 1.0,
            min_concurrency: 0.25,
            max_concurrency: 1024.0,
            tolerance: 1.0,
            gain: 0.1,
            failure_factor: 0.7,
        }
    }
}

/// A continuous operating-point controller inspired by Gradient2.
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
}

impl Gradient2 {
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
            (0.0..=1.0).contains(&config.failure_factor) && config.failure_factor > 0.0,
            "failure factor must be in (0, 1]"
        );

        Self {
            concurrency: config.initial_concurrency,
            config,
            last_gradient: 1.0,
            updates: 0,
        }
    }

    /// Updates the operating point from an observed RTT and no-load baseline.
    pub fn on_rtt(&mut self, rtt: Duration, baseline: Duration) {
        let rtt = rtt.as_secs_f64().max(f64::MIN_POSITIVE);
        let baseline = baseline.as_secs_f64().max(f64::MIN_POSITIVE);
        // The tolerance is a headroom multiplier: a response at or below the
        // baseline is an additive-increase opportunity, while an inflated RTT
        // produces a multiplicative decrease proportional to the inflation.
        let gradient = (baseline * self.config.tolerance / rtt).clamp(0.0, 4.0);
        if gradient >= 1.0 {
            self.concurrency += self.config.gain;
        } else {
            self.concurrency -= self.config.gain * (1.0 - gradient) * self.concurrency;
        }
        self.concurrency = self
            .concurrency
            .clamp(self.config.min_concurrency, self.config.max_concurrency);
        self.last_gradient = gradient;
        self.updates += 1;
    }

    /// Reduces the operating point after an outcome classified as unhealthy.
    pub fn on_failure(&mut self) {
        self.concurrency = (self.concurrency * self.config.failure_factor)
            .clamp(self.config.min_concurrency, self.config.max_concurrency);
        self.last_gradient = 0.0;
        self.updates += 1;
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
