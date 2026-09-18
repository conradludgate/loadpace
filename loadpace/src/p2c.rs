//! Comparison and bounded reservation for two caller-sampled endpoints.

use crate::{DispatchReservation, EndpointController, ScheduleError};
use std::time::Instant;

/// Identifies the selected argument to [`reserve_pair`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PairChoice {
    /// The first controller supplied by the caller.
    First,
    /// The second controller supplied by the caller.
    Second,
}

/// Reserves the cheaper of two endpoints, falling back to the other if needed.
///
/// Both loads are refreshed at `now`; equal costs prefer `first`. A successful
/// call reserves exactly one slot. The returned token belongs to the controller
/// identified by [`PairChoice`] and must be dispatched or cancelled there.
///
/// The caller owns sampling, endpoint identity, and synchronization. Sample two
/// distinct endpoints, acquire locks in a consistent order, then pass arguments
/// in sampled order to avoid biasing ties toward lock order. For a single
/// endpoint, use [`EndpointController::reserve`] directly.
///
/// This function leaves wakeup effects on **both** controllers, including when
/// neither accepts work. Drain their [`EndpointController::take_changes`] under
/// the locks and deliver notifications after releasing both locks.
///
/// # Errors
///
/// If both reservations fail, returns the fallback endpoint's scheduling error.
/// Rejection describes these two candidates, not the rest of the endpoint pool.
pub fn reserve_pair(
    first: &mut EndpointController,
    second: &mut EndpointController,
    now: Instant,
) -> Result<(PairChoice, DispatchReservation), ScheduleError> {
    let first_load = first.load(now);
    let second_load = second.load(now);
    let candidates = if first_load <= second_load {
        [(PairChoice::First, first), (PairChoice::Second, second)]
    } else {
        [(PairChoice::Second, second), (PairChoice::First, first)]
    };
    let [(preferred_choice, preferred), (fallback_choice, fallback)] = candidates;
    preferred
        .reserve(now)
        .map(|reservation| (preferred_choice, reservation))
        .or_else(|_| {
            fallback
                .reserve(now)
                .map(|reservation| (fallback_choice, reservation))
        })
}
