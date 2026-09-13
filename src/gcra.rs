use std::time::{Duration, Instant};

/// A single-rate, spacing-oriented GCRA pacer.
///
/// `tat` is the theoretical arrival time of the next request admitted by the
/// pacer. This is the useful form of GCRA for Loadpace: it gives callers both
/// a dispatch deadline and a cheap virtual queue-tail prediction.
#[derive(Clone, Debug)]
pub struct Gcra {
    interval: Duration,
    tat: Instant,
}

/// The result of reserving one slot in a [`Gcra`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcraReservation {
    /// When the reserved request may be sent.
    pub scheduled_at: Instant,
    /// The TAT after this reservation. Kept public for diagnostics.
    pub next_tat: Instant,
}

impl Gcra {
    /// Creates a pacer with one request available immediately.
    ///
    /// # Panics
    ///
    /// Panics when `rate_per_second` is not finite and strictly positive.
    pub fn new(rate_per_second: f64, now: Instant) -> Self {
        assert!(
            rate_per_second.is_finite() && rate_per_second > 0.0,
            "GCRA rate must be finite and positive"
        );

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

    /// Reserves one pacing slot.
    pub fn reserve(&mut self, now: Instant) -> GcraReservation {
        let scheduled_at = self.next_at(now);
        let next_tat = saturating_add(scheduled_at, self.interval);
        self.tat = next_tat;

        GcraReservation {
            scheduled_at,
            next_tat,
        }
    }

    /// Cancels the most recently reserved slot.
    ///
    /// GCRA reservations are normally committed in order. An earlier
    /// reservation cannot be removed safely without rebuilding the virtual
    /// queue, which is why the higher-level endpoint controller owns that
    /// operation.
    pub fn cancel_last(&mut self, reservation: GcraReservation) -> bool {
        if self.tat == reservation.next_tat {
            self.tat = reservation.scheduled_at;
            true
        } else {
            false
        }
    }

    /// Changes the rate while preserving the current pacing debt.
    pub fn set_rate(&mut self, rate_per_second: f64, now: Instant) {
        assert!(
            rate_per_second.is_finite() && rate_per_second > 0.0,
            "GCRA rate must be finite and positive"
        );

        self.tat = self.next_at(now);
        self.interval = rate_to_interval(rate_per_second);
    }

    /// Commits a request that actually dispatched at `dispatched_at`.
    ///
    /// If the transport was not ready at the predicted time, the missed time
    /// is reflected as pacing debt. This prevents a burst after a readiness
    /// stall while allowing the endpoint controller to keep its virtual queue
    /// separate from actual dispatch.
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
    // Duration::from_secs_f64 panics for values it cannot represent. Clamp the
    // interval to nanosecond precision: below that point a timer cannot make
    // a more useful distinction anyway.
    let seconds = (1.0 / rate_per_second).max(1e-9);
    Duration::from_secs_f64(seconds)
}

pub(crate) fn saturating_add(instant: Instant, duration: Duration) -> Instant {
    instant.checked_add(duration).unwrap_or(instant)
}

