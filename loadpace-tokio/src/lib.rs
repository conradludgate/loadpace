//! Adaptive pacing for ordinary async functions on Tokio.
//!
//! [`Endpoint::try_reserve`] accepts work into a bounded scheduling horizon.
//! Await [`Reservation::dispatch`] before starting endpoint work, then classify
//! its result with [`ActiveRequest::finish`]. [`Reservation::run`] combines these
//! steps while delaying creation of the operation until dispatch.
//!
//! Dropping a reservation cancels queued work. Dropping an active request records
//! abandonment without pretending the endpoint supplied feedback. No background
//! tasks are spawned, and endpoint clones share one controller.
//!
//! # Example
//!
//! ```
//! use loadpace::{Completion, EndpointConfig};
//! use loadpace_tokio::Endpoint;
//! use std::time::Duration;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let endpoint = Endpoint::new(EndpointConfig::new(Duration::from_millis(20), 32));
//! let reservation = endpoint.try_reserve().expect("propagate backpressure if full");
//! let result = reservation.run(
//!     || async { 42 }, // Start a connection or other async operation here.
//!     |_| Completion::Success,
//! ).await;
//! assert_eq!(result, 42);
//! # }
//! ```
//!
//! # Deployment scope
//!
//! Loadpace coordinates trusted, cooperative microservice clients. It does not
//! replace server-enforced quotas, admission control, or public API protection.
//!
//! # Runtime
//!
//! Dispatch waits require a Tokio runtime with time enabled. Controller instants
//! come from Tokio's clock, including when time is paused in tests.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use loadpace::{
    Completion, ControllerChanges, ControllerSnapshot, DispatchReservation, DispatchState,
    EndpointConfig, EndpointController, InFlightRequest, PairChoice, ScheduleError,
};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use tokio::sync::Notify;

struct Shared {
    controller: Mutex<EndpointController>,
    admission: Notify,
    dispatch: Notify,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, EndpointController> {
        self.controller
            .lock()
            .expect("loadpace-tokio internal mutex was poisoned")
    }

    fn with_controller<T>(&self, operation: impl FnOnce(&mut EndpointController) -> T) -> T {
        let (result, changes) = {
            let mut controller = self.lock();
            let result = operation(&mut controller);
            (result, controller.take_changes())
        };
        self.notify(changes);
        result
    }

    fn notify(&self, changes: ControllerChanges) {
        if changes.admission {
            self.admission.notify_waiters();
        }
        if changes.dispatch {
            self.dispatch.notify_waiters();
        }
    }
}

fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Shared pacing and bounded admission for one endpoint.
///
/// Keep one instance per destination and clone it for concurrent callers. The
/// caller owns destination identity, discovery, and candidate sampling.
#[derive(Clone)]
pub struct Endpoint {
    shared: Arc<Shared>,
}

impl Endpoint {
    /// Creates an endpoint using the current Tokio clock.
    ///
    /// # Panics
    ///
    /// Panics for invalid settings, as documented by [`EndpointController::new`].
    pub fn new(config: EndpointConfig) -> Self {
        Self::from_controller(EndpointController::new(config, now()))
    }

    /// Creates an endpoint with reproducible probe entropy for tests.
    ///
    /// # Panics
    ///
    /// Panics for invalid settings, as documented by [`EndpointController::new`].
    pub fn new_with_seed(config: EndpointConfig, seed: u64) -> Self {
        Self::from_controller(EndpointController::new_with_seed(config, now(), seed))
    }

    fn from_controller(controller: EndpointController) -> Self {
        Self {
            shared: Arc::new(Shared {
                controller: Mutex::new(controller),
                admission: Notify::new(),
                dispatch: Notify::new(),
            }),
        }
    }

    /// Immediately reserves a bounded queue slot, or returns backpressure.
    ///
    /// A reservation does not permit sending work until its dispatch wait ends.
    /// Dropping it, even before polling dispatch, releases the queue slot.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::QueueFull`] or [`ScheduleError::IdExhausted`].
    pub fn try_reserve(&self) -> Result<Reservation, ScheduleError> {
        self.shared
            .with_controller(|controller| {
                let current = now();
                controller.refresh(current);
                controller.reserve(current)
            })
            .map(|token| self.reservation(token))
    }

    /// Compares two sampled endpoints and reserves one atomically.
    ///
    /// Uses [`loadpace::reserve_pair`], preserving argument order for ties while
    /// locking in a stable order. Both controllers' wakeup effects are delivered
    /// after unlocking. If `other` is a clone of this endpoint, reserves once and
    /// returns [`PairChoice::First`]. Sampling remains the caller's responsibility.
    ///
    /// # Errors
    ///
    /// Returns the fallback candidate's scheduling error if neither accepts work.
    /// This describes only the supplied candidates, not the entire pool.
    pub fn try_reserve_pair(
        &self,
        other: &Self,
    ) -> Result<(PairChoice, Reservation), ScheduleError> {
        if Arc::ptr_eq(&self.shared, &other.shared) {
            return self
                .try_reserve()
                .map(|reservation| (PairChoice::First, reservation));
        }
        let first_is_lower = Arc::as_ptr(&self.shared) < Arc::as_ptr(&other.shared);
        let (lower, upper) = if first_is_lower {
            (self, other)
        } else {
            (other, self)
        };
        let (result, lower_changes, upper_changes) = {
            let mut lower_controller = lower.shared.lock();
            let mut upper_controller = upper.shared.lock();
            let current = now();
            let result = if first_is_lower {
                loadpace::reserve_pair(&mut lower_controller, &mut upper_controller, current)
            } else {
                loadpace::reserve_pair(&mut upper_controller, &mut lower_controller, current)
            };
            (
                result,
                lower_controller.take_changes(),
                upper_controller.take_changes(),
            )
        };
        lower.shared.notify(lower_changes);
        upper.shared.notify(upper_changes);
        result.map(|(choice, token)| {
            let endpoint = match choice {
                PairChoice::First => self,
                PairChoice::Second => other,
            };
            (choice, endpoint.reservation(token))
        })
    }

    fn reservation(&self, token: DispatchReservation) -> Reservation {
        Reservation {
            endpoint: self.clone(),
            token: Some(token),
        }
    }

    /// Waits until the scheduling horizon has room, without reserving a slot.
    ///
    /// Another caller may consume the slot before [`Self::try_reserve`]. Recheck
    /// admission after this returns. This is not FIFO admission and does not
    /// guarantee immediate dispatch. The caller must bound its own waiting tasks
    /// and deadlines; this method accepts no work into the controller's queue.
    pub async fn ready(&self) {
        loop {
            let notified = self.shared.admission.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if self.shared.lock().may_schedule() {
                return;
            }
            notified.await;
        }
    }

    /// Returns predicted completion cost in seconds; lower costs are preferred.
    ///
    /// Refreshes time-driven policy and wakes affected dispatch waiters.
    pub fn load(&self) -> f64 {
        self.shared
            .with_controller(|controller| controller.load(now()))
    }

    /// Returns controller diagnostics, refreshing policy and delivering wakeups.
    pub fn snapshot(&self) -> ControllerSnapshot {
        self.shared
            .with_controller(|controller| controller.snapshot(now()))
    }
}

/// An owned bounded queue slot, cancelled automatically on drop.
///
/// Reservations dispatch FIFO, even if their futures are polled out of order.
/// Retaining an unpolled reservation blocks later reservations on the endpoint.
#[must_use = "dropping a reservation cancels the queued operation"]
pub struct Reservation {
    endpoint: Endpoint,
    token: Option<DispatchReservation>,
}

impl Reservation {
    /// Waits for pacing, FIFO order, and emergency inflight capacity, then
    /// returns an active guard. Start endpoint work immediately after this await.
    ///
    /// Cancelling this future releases its reservation, including if the future
    /// was never polled. It never spawns the operation or a background task.
    pub async fn dispatch(mut self) -> ActiveRequest {
        let token = self.token.expect("a live reservation owns a token");
        loop {
            let notified = self.endpoint.shared.dispatch.notified();
            let mut notified = std::pin::pin!(notified);
            // Register before checking state so a concurrent completion or
            // cancellation cannot be lost between inspection and suspension.
            notified.as_mut().enable();
            let (result, changes) = {
                let mut controller = self.endpoint.shared.lock();
                let result = controller.on_dispatched(token, now());
                (result, controller.take_changes())
            };
            match result {
                Ok(active) => {
                    self.token = None;
                    self.endpoint.shared.notify(changes);
                    return ActiveRequest {
                        endpoint: self.endpoint.clone(),
                        token: Some(active),
                    };
                }
                Err(DispatchState::WaitUntil(deadline)) => {
                    // The FIFO head already observes its refreshed deadline.
                    // Broadcasting its decay effects would wake it in a loop.
                    let _ = tokio::time::timeout_at(deadline.into(), notified.as_mut()).await;
                }
                Err(DispatchState::WaitForPrevious | DispatchState::InflightLimit) => {
                    notified.await;
                }
                Err(DispatchState::Ready | DispatchState::Cancelled) => {
                    unreachable!("a live reservation either dispatches or waits");
                }
            }
        }
    }

    /// Waits for dispatch, creates and awaits the operation, then classifies its
    /// output and records completion. Returns the output unchanged.
    ///
    /// The closure runs only after dispatch. Use [`Completion::Abandoned`] for
    /// local timeouts without endpoint feedback. Dropping this future cancels
    /// queued work or abandons active work; a panic in either closure also drops
    /// the active guard. Separately spawned work must be cancelled by the caller.
    pub async fn run<F, Fut, C>(self, operation: F, classify: C) -> Fut::Output
    where
        F: FnOnce() -> Fut,
        Fut: Future,
        C: FnOnce(&Fut::Output) -> Completion,
    {
        let active = self.dispatch().await;
        let output = operation().await;
        active.finish(classify(&output));
        output
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.endpoint
                .shared
                .with_controller(|controller| controller.cancel(token, now()));
        }
    }
}

/// A dispatched operation, recorded as abandoned if dropped without finishing.
///
/// Hold this guard until endpoint work ends. Finishing consumes it so completion
/// is recorded once. The guard tracks accounting; it cannot cancel work spawned
/// independently by the caller.
#[must_use = "dropping an active request records abandonment"]
pub struct ActiveRequest {
    endpoint: Endpoint,
    token: Option<InFlightRequest>,
}

impl ActiveRequest {
    /// Returns the dispatch time in the Tokio clock's standard-library domain.
    /// Queueing time before this instant is excluded from the observed RTT.
    pub fn dispatched_at(&self) -> Instant {
        self.token
            .as_ref()
            .expect("an active request owns a token")
            .dispatched_at()
    }

    /// Records endpoint feedback or explicit abandonment and releases inflight
    /// accounting. Success measures RTT from dispatch to this call.
    pub fn finish(mut self, completion: Completion) {
        self.complete(completion);
    }

    fn complete(&mut self, completion: Completion) {
        if let Some(token) = self.token.take() {
            self.endpoint
                .shared
                .with_controller(|controller| controller.finish(token, completion, now()));
        }
    }
}

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.complete(Completion::Abandoned);
    }
}
