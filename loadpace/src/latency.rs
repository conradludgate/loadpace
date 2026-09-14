use std::time::{Duration, Instant};

const BASELINE_BUCKETS: usize = 8;

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
    /// Time covered by the rolling minimum-RTT window.
    pub baseline_window: Duration,
}

impl Default for LatencyEstimatorConfig {
    fn default() -> Self {
        Self {
            initial_rtt: Duration::from_millis(50),
            short_alpha: 0.25,
            long_alpha: 0.05,
            min_rtt: Duration::from_micros(1),
            baseline_window: Duration::from_secs(60),
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
    baseline_buckets: [f64; BASELINE_BUCKETS],
    baseline_bucket: usize,
    baseline_bucket_started: Option<Instant>,
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
        assert!(
            config.baseline_window >= Duration::from_nanos(BASELINE_BUCKETS as u64),
            "baseline window must be large enough for its buckets"
        );

        let initial = config.initial_rtt.as_secs_f64();
        Self {
            config,
            short: initial,
            long: initial,
            baseline: None,
            baseline_buckets: [f64::INFINITY; BASELINE_BUCKETS],
            baseline_bucket: 0,
            baseline_bucket_started: None,
            samples: 0,
        }
    }

    pub fn observe(&mut self, sample: Duration) {
        self.observe_at(sample, Instant::now());
    }

    pub fn observe_at(&mut self, sample: Duration, now: Instant) {
        let value = sample.max(self.config.min_rtt).as_secs_f64();
        self.short = ewma(self.short, value, self.config.short_alpha);
        self.long = ewma(self.long, value, self.config.long_alpha);
        self.advance_baseline_window(now);
        self.baseline_buckets[self.baseline_bucket] =
            self.baseline_buckets[self.baseline_bucket].min(value);
        let baseline = self
            .baseline_buckets
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        self.baseline = baseline.is_finite().then_some(
            baseline.max(self.config.min_rtt.as_secs_f64()),
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

    /// Returns the minimum observed RTT, or the initial estimate before the
    /// first successful sample arrives.
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

    fn advance_baseline_window(&mut self, now: Instant) {
        let bucket_duration = self
            .config
            .baseline_window
            .checked_div(BASELINE_BUCKETS as u32)
            .expect("baseline window bucket duration must be representable");
        let Some(started) = self.baseline_bucket_started else {
            self.baseline_bucket_started = Some(now);
            return;
        };

        let elapsed = now.saturating_duration_since(started);
        let steps = (elapsed.as_nanos() / bucket_duration.as_nanos()) as usize;
        if steps >= BASELINE_BUCKETS {
            self.baseline_buckets = [f64::INFINITY; BASELINE_BUCKETS];
            self.baseline_bucket = 0;
            self.baseline_bucket_started = Some(now);
            self.baseline = None;
            return;
        }

        if steps > 0 {
            for _ in 0..steps {
                self.baseline_bucket = (self.baseline_bucket + 1) % BASELINE_BUCKETS;
                self.baseline_buckets[self.baseline_bucket] = f64::INFINITY;
            }
            self.baseline_bucket_started = Some(
                started
                    .checked_add(bucket_duration.saturating_mul(steps as u32))
                    .expect("baseline window schedule overflowed Instant"),
            );
        }
    }
}

fn ewma(previous: f64, sample: f64, alpha: f64) -> f64 {
    previous + alpha * (sample - previous)
}
