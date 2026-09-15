//! Adaptive DNS discovery and power-of-two-choices endpoint selection.
//!
//! Unlike Rama's DNS IP pickers, this service reserves Loadpace admission as
//! part of endpoint selection and carries that reservation through the inner
//! service future.
//!
//! This module has the same trusted-deployment scope as the crate. DNS answers
//! identify cooperating private service endpoints; discovery and P2C selection
//! are not security boundaries for routing hostile public traffic.

use super::{EndpointOwner, EndpointState, RequestGuard, begin_dispatch, lock, now};
use arc_swap::ArcSwap;
use loadpace::{ControllerSnapshot, DispatchReservation, EndpointConfig, Outcome, ScheduleError};
use moka::future::Cache;
use rama::{
    Layer, Service,
    dns::client::{GlobalDnsResolver, resolver::DnsAddressResolver},
    error::{BoxError, BoxErrorExt as _},
    extensions::ExtensionsRef,
    futures::StreamExt as _,
    net::{
        ConnectorTargetInputExt,
        address::{Domain, Host, HostWithPort},
        client::ConnectorTarget,
        mode::DnsResolveIpMode,
    },
};
use rand::RngExt as _;
use std::{
    error::Error,
    fmt,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// Default age after which a cached DNS result refreshes in the background.
pub const DEFAULT_REFRESH_AFTER: Duration = Duration::from_secs(30);
/// Default idle lifetime of a host/port cache entry.
pub const DEFAULT_EVICT_AFTER_IDLE: Duration = Duration::from_secs(300);
/// Default maximum age of the last successful DNS result.
pub const DEFAULT_EVICT_AFTER_STALE: Duration = Duration::from_secs(300);
/// Default maximum number of cached host/port entries.
pub const DEFAULT_MAX_ENTRIES: u64 = 1024;

/// Configuration for [`AdaptiveDnsLoadBalancer`] and [`AdaptiveDnsLayer`].
pub struct AdaptiveDnsConfig<R = GlobalDnsResolver> {
    /// Resolver used for hostname discovery.
    pub resolver: R,
    /// Controller configuration cloned for each newly discovered endpoint.
    pub endpoint: EndpointConfig,
    /// Age after which a successful result refreshes in the background.
    pub refresh_after: Duration,
    /// Idle lifetime of a host/port cache entry.
    pub evict_after_idle: Duration,
    /// Maximum age of the last successful result before it is discarded.
    pub evict_after_stale: Duration,
    /// Address families and preference used for resolution.
    pub mode: DnsResolveIpMode,
    /// Maximum number of cached `(hostname, port)` entries.
    pub max_entries: u64,
}

impl<R: Clone> Clone for AdaptiveDnsConfig<R> {
    fn clone(&self) -> Self {
        Self {
            resolver: self.resolver.clone(),
            endpoint: self.endpoint.clone(),
            refresh_after: self.refresh_after,
            evict_after_idle: self.evict_after_idle,
            evict_after_stale: self.evict_after_stale,
            mode: self.mode,
            max_entries: self.max_entries,
        }
    }
}

impl AdaptiveDnsConfig {
    /// Creates a configuration using Rama's process-global resolver.
    #[must_use]
    pub fn new() -> Self {
        Self::with_resolver(GlobalDnsResolver::new())
    }
}

impl Default for AdaptiveDnsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> AdaptiveDnsConfig<R> {
    /// Creates a configuration using a custom resolver and default policies.
    pub fn with_resolver(resolver: R) -> Self {
        Self {
            resolver,
            endpoint: EndpointConfig::default(),
            refresh_after: DEFAULT_REFRESH_AFTER,
            evict_after_idle: DEFAULT_EVICT_AFTER_IDLE,
            evict_after_stale: DEFAULT_EVICT_AFTER_STALE,
            mode: DnsResolveIpMode::default(),
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }
}

/// A point-in-time view of one cached adaptive DNS endpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct DnsEndpointSnapshot {
    /// Hostname whose resolution produced this endpoint.
    pub hostname: Domain,
    /// Connector port, which is part of the endpoint identity.
    pub port: u16,
    /// Resolved address.
    pub ip: IpAddr,
    /// Current Loadpace controller state.
    pub controller: ControllerSnapshot,
}

/// Errors produced by [`AdaptiveDnsLoadBalancer`].
#[derive(Debug)]
pub enum DnsServiceError<E> {
    /// DNS lookup or cache initialization failed.
    Discovery(BoxError),
    /// Neither sampled endpoint could admit the request.
    Rejected(ScheduleError),
    /// The selected endpoint's inner service failed.
    Inner(E),
}

impl<E> DnsServiceError<E> {
    /// Returns the admission error, when selection was rejected.
    pub fn rejected(&self) -> Option<ScheduleError> {
        match self {
            Self::Rejected(error) => Some(*error),
            Self::Discovery(_) | Self::Inner(_) => None,
        }
    }

    /// Returns the discovery error, when DNS failed.
    pub fn discovery(&self) -> Option<&BoxError> {
        match self {
            Self::Discovery(error) => Some(error),
            Self::Rejected(_) | Self::Inner(_) => None,
        }
    }

    /// Returns the inner-service error, when dispatch failed.
    pub fn inner(&self) -> Option<&E> {
        match self {
            Self::Inner(error) => Some(error),
            Self::Discovery(_) | Self::Rejected(_) => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for DnsServiceError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovery(error) => write!(formatter, "DNS discovery failed: {error}"),
            Self::Rejected(error) => write!(formatter, "request rejected: {error:?}"),
            Self::Inner(error) => write!(formatter, "inner service error: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for DnsServiceError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Discovery(error) => Some(error.as_ref()),
            Self::Rejected(_) => None,
            Self::Inner(error) => Some(error),
        }
    }
}

/// A layer that adds adaptive DNS discovery and atomic P2C admission.
pub struct AdaptiveDnsLayer<R = GlobalDnsResolver> {
    config: AdaptiveDnsConfig<R>,
}

impl<R> AdaptiveDnsLayer<R> {
    /// Creates a layer with the supplied DNS and endpoint configuration.
    pub fn new(config: AdaptiveDnsConfig<R>) -> Self {
        Self { config }
    }

    /// Returns the configuration used by services created by this layer.
    pub fn config(&self) -> &AdaptiveDnsConfig<R> {
        &self.config
    }
}

impl Default for AdaptiveDnsLayer {
    fn default() -> Self {
        Self::new(AdaptiveDnsConfig::new())
    }
}

impl<R: Clone> Clone for AdaptiveDnsLayer<R> {
    fn clone(&self) -> Self {
        Self::new(self.config.clone())
    }
}

impl<S, R> Layer<S> for AdaptiveDnsLayer<R>
where
    R: DnsAddressResolver + Clone,
{
    type Service = AdaptiveDnsLoadBalancer<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        AdaptiveDnsLoadBalancer::new(inner, self.config.clone())
    }

    fn into_layer(self, inner: S) -> Self::Service {
        AdaptiveDnsLoadBalancer::new(inner, self.config)
    }
}

/// A Rama service that resolves hostnames and atomically reserves one of two
/// sampled adaptive endpoints before pinning the connector target to its IP.
pub struct AdaptiveDnsLoadBalancer<S, R = GlobalDnsResolver> {
    inner: S,
    cache: Arc<DnsCache<R>>,
}

impl<S, R> AdaptiveDnsLoadBalancer<S, R>
where
    R: DnsAddressResolver + Clone,
{
    /// Wraps `inner` with adaptive DNS discovery.
    pub fn new(inner: S, config: AdaptiveDnsConfig<R>) -> Self {
        Self {
            inner,
            cache: Arc::new(DnsCache::new(config)),
        }
    }

    /// Returns snapshots for a currently cached hostname and port.
    ///
    /// This method does not perform DNS lookup or refresh the entry.
    pub async fn endpoint_snapshots(
        &self,
        hostname: &Domain,
        port: u16,
    ) -> Vec<DnsEndpointSnapshot> {
        self.cache.snapshots(hostname, port).await
    }
}

impl<S: Clone, R> Clone for AdaptiveDnsLoadBalancer<S, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: Arc::clone(&self.cache),
        }
    }
}

impl<S, R, Input> Service<Input> for AdaptiveDnsLoadBalancer<S, R>
where
    S: Service<Input> + Send + Sync + 'static,
    S::Output: Send + 'static,
    S::Error: Send + 'static,
    Input: ConnectorTargetInputExt + ExtensionsRef + Send + 'static,
    R: DnsAddressResolver + Clone,
{
    type Output = S::Output;
    type Error = DnsServiceError<S::Error>;

    async fn serve(&self, input: Input) -> Result<Self::Output, Self::Error> {
        let Some(target) = input.connector_target() else {
            return self
                .inner
                .serve(input)
                .await
                .map_err(DnsServiceError::Inner);
        };

        let Host::Name(hostname) = target.host else {
            return self
                .inner
                .serve(input)
                .await
                .map_err(DnsServiceError::Inner);
        };

        let entry = self
            .cache
            .lookup(CacheKey {
                hostname,
                port: target.port,
            })
            .await
            .map_err(DnsServiceError::Discovery)?;

        let (endpoint, reservation) = loop {
            let snapshot = entry.current.load_full();
            let selected =
                reserve_p2c(&snapshot.endpoints, now()).map_err(DnsServiceError::Rejected)?;

            // A refresh can replace the answer set concurrently with
            // selection. Re-check before publishing the target; if the chosen
            // address disappeared, cancel and select from the current set.
            let current = entry.current.load_full();
            if current
                .endpoints
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, &selected.0))
            {
                break selected;
            }
            lock(&selected.0.state.controller).cancel(selected.1, now());
            selected.0.state.dispatch.notify_waiters();
        };

        input.extensions().insert(ConnectorTarget(HostWithPort::new(
            Host::Address(endpoint.ip),
            endpoint.port,
        )));

        let mut guard = RequestGuard::new(Arc::clone(&endpoint), reservation);
        let active = begin_dispatch(&endpoint.state, reservation).await;
        guard.mark_dispatched(active);

        let result = self
            .inner
            .serve(input)
            .await
            .map_err(DnsServiceError::Inner);
        let outcome = if result.is_ok() {
            Outcome::Success
        } else {
            Outcome::Failure
        };
        guard.finish(outcome, now());
        result
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    hostname: Domain,
    port: u16,
}

struct Endpoint {
    hostname: Domain,
    port: u16,
    ip: IpAddr,
    state: EndpointState,
}

impl EndpointOwner for Endpoint {
    fn endpoint_state(&self) -> &EndpointState {
        &self.state
    }
}

impl Endpoint {
    fn snapshot(&self) -> DnsEndpointSnapshot {
        DnsEndpointSnapshot {
            hostname: self.hostname.clone(),
            port: self.port,
            ip: self.ip,
            controller: lock(&self.state.controller).snapshot(now()),
        }
    }
}

struct ResolvedSnapshot {
    endpoints: Arc<[Arc<Endpoint>]>,
    fetched_at: Instant,
}

struct HostEntry {
    current: ArcSwap<ResolvedSnapshot>,
    refreshing: AtomicBool,
}

impl HostEntry {
    fn try_acquire_refresh(self: &Arc<Self>) -> Option<RefreshGuard> {
        self.refreshing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| RefreshGuard(Arc::clone(self)))
    }
}

struct RefreshGuard(Arc<HostEntry>);

impl RefreshGuard {
    fn update(&self, value: Arc<ResolvedSnapshot>) {
        self.0.current.store(value);
    }
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.0.refreshing.store(false, Ordering::Release);
    }
}

struct DnsCache<R> {
    resolver: R,
    endpoint_config: EndpointConfig,
    refresh_after: Duration,
    evict_after_stale: Duration,
    mode: DnsResolveIpMode,
    entries: Cache<CacheKey, Arc<HostEntry>>,
}

impl<R> DnsCache<R>
where
    R: DnsAddressResolver + Clone,
{
    fn new(config: AdaptiveDnsConfig<R>) -> Self {
        Self {
            resolver: config.resolver,
            endpoint_config: config.endpoint,
            refresh_after: config.refresh_after,
            evict_after_stale: config.evict_after_stale,
            mode: config.mode,
            entries: Cache::builder()
                .max_capacity(config.max_entries)
                .time_to_idle(config.evict_after_idle)
                .build(),
        }
    }

    async fn lookup(self: &Arc<Self>, key: CacheKey) -> Result<Arc<HostEntry>, BoxError> {
        if let Some(entry) = self.entries.get(&key).await {
            let elapsed = entry.current.load().fetched_at.elapsed();
            if elapsed < self.refresh_after {
                return Ok(entry);
            }
            if elapsed < self.evict_after_stale {
                Arc::clone(self).spawn_refresh(key, &entry);
                return Ok(entry);
            }
            self.entries.invalidate(&key).await;
        }

        let cache = Arc::clone(self);
        let init_key = key.clone();
        self.entries
            .try_get_with(key, async move {
                let snapshot = cache.resolve(&init_key, None).await?;
                Ok::<_, BoxError>(Arc::new(HostEntry {
                    current: ArcSwap::from_pointee(snapshot),
                    refreshing: AtomicBool::new(false),
                }))
            })
            .await
            .map_err(|error: Arc<BoxError>| {
                format!("adaptive DNS initial resolve failed: {error}").into()
            })
    }

    fn spawn_refresh(self: Arc<Self>, key: CacheKey, entry: &Arc<HostEntry>) {
        let Some(refresh) = entry.try_acquire_refresh() else {
            return;
        };

        let previous = entry.current.load_full();
        tokio::spawn(async move {
            if let Ok(snapshot) = self.resolve(&key, Some(&previous)).await {
                refresh.update(Arc::new(snapshot));
            }
        });
    }

    async fn snapshots(&self, hostname: &Domain, port: u16) -> Vec<DnsEndpointSnapshot> {
        let key = CacheKey {
            hostname: hostname.clone(),
            port,
        };
        let Some(entry) = self.entries.get(&key).await else {
            return Vec::new();
        };
        entry
            .current
            .load_full()
            .endpoints
            .iter()
            .map(|endpoint| endpoint.snapshot())
            .collect()
    }

    async fn resolve(
        &self,
        key: &CacheKey,
        previous: Option<&ResolvedSnapshot>,
    ) -> Result<ResolvedSnapshot, BoxError> {
        let mut ips = Vec::new();
        match self.mode {
            DnsResolveIpMode::SingleIpV4 => self.collect_v4(&key.hostname, &mut ips).await,
            DnsResolveIpMode::SingleIpV6 => self.collect_v6(&key.hostname, &mut ips).await,
            DnsResolveIpMode::Dual => {
                self.collect_v6(&key.hostname, &mut ips).await;
                self.collect_v4(&key.hostname, &mut ips).await;
            }
            DnsResolveIpMode::DualPreferIpV4 => {
                self.collect_v4(&key.hostname, &mut ips).await;
                self.collect_v6(&key.hostname, &mut ips).await;
            }
        }
        ips.sort_unstable();
        ips.dedup();
        if ips.is_empty() {
            return Err(BoxError::from_static_str(
                "adaptive DNS resolver returned no addresses",
            ));
        }

        let endpoints = ips
            .into_iter()
            .map(|ip| {
                previous
                    .and_then(|snapshot| {
                        snapshot.endpoints.iter().find(|endpoint| endpoint.ip == ip)
                    })
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(Endpoint {
                            hostname: key.hostname.clone(),
                            port: key.port,
                            ip,
                            state: EndpointState::new(self.endpoint_config.clone(), now()),
                        })
                    })
            })
            .collect::<Vec<_>>()
            .into();

        Ok(ResolvedSnapshot {
            endpoints,
            fetched_at: Instant::now(),
        })
    }

    async fn collect_v4(&self, hostname: &Domain, output: &mut Vec<IpAddr>) {
        let mut stream = std::pin::pin!(self.resolver.lookup_ipv4(hostname.clone()));
        while let Some(result) = stream.next().await {
            if let Ok(ip) = result {
                output.push(IpAddr::V4(ip));
            }
        }
    }

    async fn collect_v6(&self, hostname: &Domain, output: &mut Vec<IpAddr>) {
        let mut stream = std::pin::pin!(self.resolver.lookup_ipv6(hostname.clone()));
        while let Some(result) = stream.next().await {
            if let Ok(ip) = result {
                output.push(IpAddr::V6(ip));
            }
        }
    }
}

fn reserve_p2c(
    endpoints: &[Arc<Endpoint>],
    current: std::time::Instant,
) -> Result<(Arc<Endpoint>, DispatchReservation), ScheduleError> {
    if endpoints.len() == 1 {
        let endpoint = Arc::clone(&endpoints[0]);
        let mut controller = lock(&endpoint.state.controller);
        let previous_probe = controller.active_probe();
        controller.refresh(current);
        let reservation = controller.reserve(current);
        let probe_changed = previous_probe != controller.active_probe();
        drop(controller);
        if probe_changed {
            endpoint.state.dispatch.notify_waiters();
        }
        return Ok((endpoint, reservation?));
    }

    let mut rng = rand::rng();
    let first_index = rng.random_range(0..endpoints.len());
    let mut second_index = rng.random_range(0..endpoints.len() - 1);
    if second_index >= first_index {
        second_index += 1;
    }
    let first = &endpoints[first_index];
    let second = &endpoints[second_index];

    let (lower, upper, first_is_lower) = if first.ip < second.ip {
        (first, second, true)
    } else {
        (second, first, false)
    };
    let mut lower_controller = lock(&lower.state.controller);
    let mut upper_controller = lock(&upper.state.controller);

    let lower_probe = lower_controller.active_probe();
    let upper_probe = upper_controller.active_probe();
    let lower_load = lower_controller.load(current);
    let upper_load = upper_controller.load(current);

    let prefer_lower = lower_load < upper_load || (lower_load == upper_load && first_is_lower);
    let selected = if prefer_lower {
        lower_controller
            .reserve(current)
            .map(|reservation| (Arc::clone(lower), reservation))
            .or_else(|_| {
                upper_controller
                    .reserve(current)
                    .map(|reservation| (Arc::clone(upper), reservation))
            })
    } else {
        upper_controller
            .reserve(current)
            .map(|reservation| (Arc::clone(upper), reservation))
            .or_else(|_| {
                lower_controller
                    .reserve(current)
                    .map(|reservation| (Arc::clone(lower), reservation))
            })
    };

    let lower_probe_changed = lower_probe != lower_controller.active_probe();
    let upper_probe_changed = upper_probe != upper_controller.active_probe();
    drop(lower_controller);
    drop(upper_controller);
    if lower_probe_changed {
        lower.state.dispatch.notify_waiters();
    }
    if upper_probe_changed {
        upper.state.dispatch.notify_waiters();
    }
    selected
}
