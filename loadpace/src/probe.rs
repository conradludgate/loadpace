use crate::gcra::saturating_add;
use rand::{Rng, RngExt};
use std::time::{Duration, Instant};

/// The temporary perturbations supported by Loadpace.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProbeKind {
    /// Temporarily add a fixed number of requests per second.
    Positive { delta_rate: f64 },
    /// Temporarily multiply the base request rate.
    Negative { factor: f64 },
}

/// An active temporary probe.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Probe {
    pub kind: ProbeKind,
    pub until: Instant,
}

/// Per-endpoint probe state.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProbeState {
    active: Option<Probe>,
    next_probe_at: Option<Instant>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn at_zero() -> Instant {
        Instant::now()
    }

    #[test]
    fn probes_are_additive_rate_positive_and_multiplicative_negative() {
        let now = at_zero();
        let mut state = ProbeState::new();

        state.start_positive(1.0, now + Duration::from_secs(1));
        assert_eq!(
            state.effective_rate(8.0, now),
            9.0,
            "positive probes must be additive"
        );
        assert_eq!(state.effective_rate(8.0, now + Duration::from_secs(1)), 8.0);

        state.start_negative(0.8, now + Duration::from_secs(2));
        assert_eq!(
            state.active(now).map(|probe| probe.kind),
            Some(ProbeKind::Negative { factor: 0.8 })
        );
        assert!((state.effective_rate(8.0, now) - 6.4).abs() < f64::EPSILON);
    }

    #[test]
    fn probe_schedule_is_time_gated() {
        let now = at_zero();
        let mut state = ProbeState::new();
        let schedule = ProbeSchedule {
            positive_probability: 1.0,
            negative_probability: 0.0,
            duration: Duration::from_millis(100),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(1),
            ..ProbeSchedule::default()
        };
        let mut rng = rand::rngs::StdRng::seed_from_u64(5);

        assert!(schedule.maybe_start(&mut state, &mut rng, now).is_some());
        assert!(
            schedule
                .maybe_start(&mut state, &mut rng, now + Duration::from_millis(100))
                .is_none()
        );
        assert!(
            schedule
                .maybe_start(&mut state, &mut rng, now + Duration::from_secs(1))
                .is_some()
        );
    }
}

impl ProbeState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Starts a temporary positive probe.
    ///
    /// # Panics
    ///
    /// Panics when `delta` is not finite and strictly positive.
    fn start_positive(&mut self, delta_rate: f64, until: Instant) {
        assert!(
            delta_rate.is_finite() && delta_rate > 0.0,
            "positive probe rate delta must be positive"
        );
        self.active = Some(Probe {
            kind: ProbeKind::Positive { delta_rate },
            until,
        });
    }

    /// Starts a temporary negative probe.
    ///
    /// # Panics
    ///
    /// Panics when `factor` is not finite or is outside `(0, 1)`.
    fn start_negative(&mut self, factor: f64, until: Instant) {
        assert!(
            factor.is_finite() && (0.0..1.0).contains(&factor),
            "negative probe factor must be in (0, 1)"
        );
        self.active = Some(Probe {
            kind: ProbeKind::Negative { factor },
            until,
        });
    }

    pub(crate) fn active(&mut self, now: Instant) -> Option<Probe> {
        if self.active.is_some_and(|probe| probe.until <= now) {
            self.active = None;
        }
        self.active
    }

    pub(crate) fn effective_rate(&mut self, base: f64, now: Instant) -> f64 {
        match self.active(now).map(|probe| probe.kind) {
            None => base,
            Some(ProbeKind::Positive { delta_rate }) => base + delta_rate,
            Some(ProbeKind::Negative { factor }) => base * factor,
        }
    }

    pub(crate) fn current(&self) -> Option<Probe> {
        self.active
    }

    /// Returns when the next probe decision may be made.
    pub(crate) fn next_probe_at(&self) -> Option<Instant> {
        self.next_probe_at
    }
}

/// Randomized probe policy used internally by an [`crate::EndpointController`].
///
/// The controller owns the random source and decides when to consult this
/// schedule. Applications only need to configure it when they want to tune
/// the controller's defaults.
#[derive(Clone, Debug)]
pub struct ProbeSchedule {
    pub positive_probability: f64,
    pub negative_probability: f64,
    pub positive_rate_delta: f64,
    pub negative_factor: f64,
    pub duration: Duration,
    pub min_interval: Duration,
    pub max_interval: Duration,
}

impl Default for ProbeSchedule {
    fn default() -> Self {
        Self {
            // A decision is made every one to five seconds, so these values
            // give an expected probe opportunity about every twenty seconds.
            // That is short enough to repair a stale allocation while still
            // keeping probes rare relative to ordinary request traffic.
            positive_probability: 0.10,
            negative_probability: 0.05,
            positive_rate_delta: 20.0,
            negative_factor: 0.8,
            duration: Duration::from_secs(1),
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(5),
        }
    }
}

impl ProbeSchedule {
    /// Validates the probabilities, perturbation parameters, and timing.
    ///
    /// # Panics
    ///
    /// Panics when a probability, perturbation, or duration is invalid.
    pub fn validate(&self) {
        assert!(
            self.positive_probability.is_finite()
                && self.positive_probability >= 0.0
                && self.negative_probability.is_finite()
                && self.negative_probability >= 0.0
                && self.positive_probability + self.negative_probability <= 1.0,
            "probe probabilities must be finite, non-negative, and sum to at most one"
        );
        assert!(
            self.positive_rate_delta.is_finite() && self.positive_rate_delta > 0.0,
            "positive probe rate delta must be finite and positive"
        );
        assert!(
            self.negative_factor.is_finite() && (0.0..1.0).contains(&self.negative_factor),
            "negative probe factor must be finite and in (0, 1)"
        );
        assert!(!self.duration.is_zero(), "probe duration must be positive");
        assert!(
            !self.min_interval.is_zero() && self.max_interval >= self.min_interval,
            "probe interval bounds must be positive and ordered"
        );
    }

    /// Starts at most one probe when the endpoint is not already probing.
    ///
    pub(crate) fn maybe_start<R: Rng + ?Sized>(
        &self,
        state: &mut ProbeState,
        rng: &mut R,
        now: Instant,
    ) -> Option<Probe> {
        if let Some(active) = state.active(now) {
            return Some(active);
        }

        let due_at = state.next_probe_at.get_or_insert(now);
        if *due_at > now {
            return None;
        }

        let interval = self.random_interval(rng);
        state.next_probe_at = Some(saturating_add(now, interval));

        let draw = rng.random::<f64>();
        if draw < self.positive_probability {
            let until = saturating_add(now, self.duration);
            state.start_positive(self.positive_rate_delta, until);
            state.current()
        } else if draw < self.positive_probability + self.negative_probability {
            let until = saturating_add(now, self.duration);
            state.start_negative(self.negative_factor, until);
            state.current()
        } else {
            None
        }
    }

    fn random_interval<R: Rng + ?Sized>(&self, rng: &mut R) -> Duration {
        let span = self.max_interval - self.min_interval;
        if span.is_zero() {
            return self.min_interval;
        }

        let jitter = Duration::from_secs_f64(span.as_secs_f64() * rng.random::<f64>()).min(span);
        self.min_interval + jitter
    }
}
