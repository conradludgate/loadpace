use rand::Rng;
use std::time::{Duration, Instant};

/// The temporary perturbations supported by Loadpace.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProbeKind {
    /// Temporarily add a fixed amount to the base virtual concurrency.
    Positive { delta: f64 },
    /// Temporarily multiply the base virtual concurrency.
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
pub struct ProbeState {
    active: Option<Probe>,
}

impl ProbeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start_positive(&mut self, delta: f64, until: Instant) {
        assert!(delta.is_finite() && delta > 0.0, "positive probe delta must be positive");
        self.active = Some(Probe {
            kind: ProbeKind::Positive { delta },
            until,
        });
    }

    pub fn start_negative(&mut self, factor: f64, until: Instant) {
        assert!(
            factor.is_finite() && (0.0..1.0).contains(&factor),
            "negative probe factor must be in (0, 1)"
        );
        self.active = Some(Probe {
            kind: ProbeKind::Negative { factor },
            until,
        });
    }

    pub fn active(&mut self, now: Instant) -> Option<Probe> {
        if self.active.is_some_and(|probe| probe.until <= now) {
            self.active = None;
        }
        self.active
    }

    pub fn effective_concurrency(&mut self, base: f64, now: Instant) -> f64 {
        match self.active(now).map(|probe| probe.kind) {
            None => base,
            Some(ProbeKind::Positive { delta }) => base + delta,
            Some(ProbeKind::Negative { factor }) => base * factor,
        }
    }

    pub fn current(&self) -> Option<Probe> {
        self.active
    }
}

/// Randomized probe policy. The random source is supplied by the caller so
/// simulations can use a seeded RNG and production code can use its preferred
/// entropy source.
#[derive(Clone, Debug)]
pub struct ProbeSchedule {
    pub positive_probability: f64,
    pub negative_probability: f64,
    pub positive_delta: f64,
    pub negative_factor: f64,
    pub duration: Duration,
}

impl Default for ProbeSchedule {
    fn default() -> Self {
        Self {
            positive_probability: 0.02,
            negative_probability: 0.01,
            positive_delta: 1.0,
            negative_factor: 0.8,
            duration: Duration::from_secs(1),
        }
    }
}

impl ProbeSchedule {
    /// Starts at most one probe when the endpoint is not already probing.
    pub fn maybe_start<R: Rng + ?Sized>(
        &self,
        state: &mut ProbeState,
        rng: &mut R,
        now: Instant,
    ) -> Option<Probe> {
        if state.active(now).is_some() {
            return state.current();
        }

        let draw = rng.gen::<f64>();
        let probe = if draw < self.positive_probability {
            state.start_positive(self.positive_delta, now + self.duration);
            state.current()
        } else if draw < self.positive_probability + self.negative_probability {
            state.start_negative(self.negative_factor, now + self.duration);
            state.current()
        } else {
            None
        };

        probe
    }
}

