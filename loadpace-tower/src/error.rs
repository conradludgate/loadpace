use std::{error::Error, fmt, sync::Arc};
use tower::BoxError;

/// A terminal readiness failure shared by an endpoint's queued requests.
///
/// The original error is available through [`Error::source`]. Clones share
/// that error without requiring the inner service's error type to be cloneable.
#[derive(Clone, Debug)]
pub struct ServiceError {
    inner: Arc<BoxError>,
}

impl ServiceError {
    pub(crate) fn new(inner: BoxError) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "adaptive endpoint failed: {}", self.inner)
    }
}

impl Error for ServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.inner.as_ref().as_ref())
    }
}
