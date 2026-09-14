use std::time::Duration;

/// Configuration for the endpoint latency estimator.
#[derive(Clone, Debug)]
pub struct LatencyEstimatorConfig {
    /// Initial estimate used before the first response.
    pub initial_rtt: Duration,
    /// Weight given to new samples by the short EWMA.
    pub short_alpha: f64,
    /// Weight given to new samples by the long EWMA.
    pub long_alpha: f64,
    /// Smallest RTT accepted by the estimator.
    pub min_rtt: Duration,
}

impl Default for LatencyEstimatorConfig {
    fn default() -> Self {
        Self {
            initial_rtt: Duration::from_millis(50),
            short_alpha: 0.25,
            long_alpha: 0.05,
            min_rtt: Duration::from_micros(1),
        }
    }
}

/// Smoothed endpoint RTT estimates.
///
/// The estimator intentionally receives dispatch-to-completion measurements;
/// callers should not feed local queue or pacing delay into it.
#[derive(Clone, Debug)]
pub struct LatencyEstimator {
    config: LatencyEstimatorConfig,
    short: f64,
    long: f64,
    baseline: Option<f64>,
    samples: u64,
}

impl LatencyEstimator {
    pub fn new(config: LatencyEstimatorConfig) -> Self {
        assert!(!config.min_rtt.is_zero(), "minimum RTT must be positive");
        assert!(
            config.initial_rtt >= config.min_rtt,
            "initial RTT must not be below the minimum RTT"
        );
        assert!(
            (0.0..=1.0).contains(&config.short_alpha) && config.short_alpha > 0.0,
            "short alpha must be in (0, 1]"
        );
        assert!(
            (0.0..=1.0).contains(&config.long_alpha) && config.long_alpha > 0.0,
            "long alpha must be in (0, 1]"
        );

        let initial = config.initial_rtt.as_secs_f64();
        Self {
            config,
            short: initial,
            long: initial,
            baseline: None,
            samples: 0,
        }
    }

    pub fn observe(&mut self, sample: Duration) {
        let value = sample.max(self.config.min_rtt).as_secs_f64();
        self.short = ewma(self.short, value, self.config.short_alpha);
        self.long = ewma(self.long, value, self.config.long_alpha);
        self.baseline = Some(
            self.baseline
                .map_or(value, |baseline| baseline.min(value))
                .max(self.config.min_rtt.as_secs_f64()),
        );
        self.samples += 1;
    }

    pub fn expected_rtt(&self) -> Duration {
        Duration::from_secs_f64(self.long.max(self.config.min_rtt.as_secs_f64()))
    }

    pub fn short(&self) -> Duration {
        Duration::from_secs_f64(self.short)
    }

    pub fn long(&self) -> Duration {
        Duration::from_secs_f64(self.long)
    }

    pub fn baseline(&self) -> Duration {
        Duration::from_secs_f64(
            self.baseline
                .unwrap_or(self.config.initial_rtt.as_secs_f64())
                .max(self.config.min_rtt.as_secs_f64()),
        )
    }

    pub fn samples(&self) -> u64 {
        self.samples
    }
}

fn ewma(previous: f64, sample: f64, alpha: f64) -> f64 {
    previous + alpha * (sample - previous)
}
