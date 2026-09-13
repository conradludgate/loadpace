use std::time::{Duration, Instant};

/// A single-rate, spacing-oriented GCRA pacer.
///
/// `tat` is the theoretical arrival time of the next request admitted by the
/// pacer. This is the useful form of GCRA for Loadpace: it gives callers a
/// dispatch deadline and a cheap virtual queue-tail prediction without adding
/// burst capacity.
#[derive(Clone, Debug)]
pub struct Gcra {
    interval: Duration,
    tat: Instant,
}

impl Gcra {
    /// Creates a pacer with one request available immediately.
    ///
    /// # Panics
    ///
    /// Panics when `rate_per_second` is not finite and strictly positive, or
    /// when its interval is too large to represent as a [`Duration`].
    pub fn new(rate_per_second: f64, now: Instant) -> Self {
        Self {
            interval: rate_to_interval(rate_per_second),
            tat: now,
        }
    }

    /// Returns the configured average dispatch rate.
    pub fn rate(&self) -> f64 {
        self.interval.as_secs_f64().recip()
    }

    /// Returns the spacing between requests.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Returns the next time a request could be dispatched without reserving it.
    pub fn next_at(&self, now: Instant) -> Instant {
        self.tat.max(now)
    }

    /// Changes the rate while preserving the current pacing phase.
    ///
    /// # Panics
    ///
    /// Panics when `rate_per_second` is invalid or its interval is too large
    /// to represent as a [`Duration`].
    pub fn set_rate(&mut self, rate_per_second: f64, now: Instant) {
        let interval = rate_to_interval(rate_per_second);
        let phase =
            self.tat.saturating_duration_since(now).as_secs_f64() / self.interval.as_secs_f64();
        self.interval = interval;
        let remaining = Duration::from_secs_f64(phase * interval.as_secs_f64());
        self.tat = saturating_add(now, remaining);
    }

    /// Commits a request that actually dispatched at `dispatched_at`.
    ///
    /// If the transport was not ready at the predicted time, the missed time
    /// is reflected as pacing debt. This prevents a burst after a readiness
    /// stall while allowing the endpoint controller to keep its virtual queue
    /// separate from actual dispatch. Speculative queue reservations belong to
    /// the higher-level endpoint controller and must not be committed here.
    pub fn commit(&mut self, dispatched_at: Instant) {
        let base = self.next_at(dispatched_at);
        self.tat = saturating_add(base, self.interval);
    }

    /// Returns the TAT used for diagnostics and prediction.
    pub fn tat(&self) -> Instant {
        self.tat
    }
}

fn rate_to_interval(rate_per_second: f64) -> Duration {
    assert!(
        rate_per_second.is_finite() && rate_per_second > 0.0,
        "GCRA rate must be finite and positive"
    );

    // Duration::from_secs_f64 panics for values it cannot represent. Clamp the
    // interval to nanosecond precision: below that point a timer cannot make
    // a more useful distinction anyway.
    let seconds = 1.0 / rate_per_second;
    assert!(
        seconds.is_finite() && seconds <= Duration::MAX.as_secs_f64(),
        "GCRA rate is too low to represent as a Duration"
    );
    let seconds = seconds.max(1e-9);
    Duration::from_secs_f64(seconds)
}

pub(crate) fn saturating_add(instant: Instant, duration: Duration) -> Instant {
    instant
        .checked_add(duration)
        .expect("GCRA schedule overflowed Instant")
}
