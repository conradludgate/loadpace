#![cfg(feature = "dns")]

use loadpace::{EndpointConfig, ScheduleError};
use loadpace_rama::dns::{AdaptiveDnsConfig, AdaptiveDnsLoadBalancer, DnsServiceError};
use rama::{
    Service,
    dns::client::resolver::DnsAddressResolver,
    extensions::{Extensions, ExtensionsRef},
    futures::{Stream, stream},
    net::{
        AuthorityInputExt, Protocol, ProtocolInputExt, TransportProtocolInputExt,
        address::{Domain, Host, HostWithOptPort, HostWithPort},
        client::ConnectorTarget,
        mode::DnsResolveIpMode,
        transport::TransportProtocol,
    },
};
use std::{
    convert::Infallible,
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::sync::Notify;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("test mutex was poisoned")
}

#[derive(Clone)]
struct MutableResolver {
    ips: Arc<Mutex<Vec<Ipv4Addr>>>,
    calls: Arc<AtomicUsize>,
}

impl MutableResolver {
    fn new(ips: Vec<Ipv4Addr>) -> Self {
        Self {
            ips: Arc::new(Mutex::new(ips)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn set(&self, ips: Vec<Ipv4Addr>) {
        *lock(&self.ips) = ips;
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl DnsAddressResolver for MutableResolver {
    type Error = Infallible;

    fn lookup_ipv4(
        &self,
        _: Domain,
    ) -> impl Stream<Item = Result<Ipv4Addr, Self::Error>> + Send + '_ {
        self.calls.fetch_add(1, Ordering::SeqCst);
        stream::iter(lock(&self.ips).clone().into_iter().map(Ok))
    }

    fn lookup_ipv6(
        &self,
        _: Domain,
    ) -> impl Stream<Item = Result<Ipv6Addr, Self::Error>> + Send + '_ {
        stream::empty()
    }
}

#[derive(Clone)]
struct AlternatingResolver {
    first: Ipv4Addr,
    second: Ipv4Addr,
    calls: Arc<AtomicUsize>,
}

impl DnsAddressResolver for AlternatingResolver {
    type Error = Infallible;

    fn lookup_ipv4(
        &self,
        _: Domain,
    ) -> impl Stream<Item = Result<Ipv4Addr, Self::Error>> + Send + '_ {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let ips = if call.is_multiple_of(2) {
            [self.first, self.second]
        } else {
            [self.second, self.first]
        };
        stream::iter(ips.into_iter().map(Ok))
    }

    fn lookup_ipv6(
        &self,
        _: Domain,
    ) -> impl Stream<Item = Result<Ipv6Addr, Self::Error>> + Send + '_ {
        stream::empty()
    }
}

#[derive(Default)]
struct TestRequest {
    id: usize,
    extensions: Extensions,
    authority: Option<HostWithPort>,
}

impl ExtensionsRef for TestRequest {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl AuthorityInputExt for TestRequest {
    fn authority(&self) -> Option<HostWithOptPort> {
        self.authority.clone().map(Into::into)
    }
}

impl ProtocolInputExt for TestRequest {
    fn protocol(&self) -> Option<&Protocol> {
        Some(&Protocol::HTTPS)
    }
}

impl TransportProtocolInputExt for TestRequest {
    fn transport_protocol(&self) -> Option<TransportProtocol> {
        Some(TransportProtocol::Tcp)
    }
}

fn request(id: usize, port: u16) -> TestRequest {
    TestRequest {
        id,
        extensions: Extensions::new(),
        authority: Some(HostWithPort::new(
            Host::Name(Domain::from_static("example.com")),
            port,
        )),
    }
}

#[derive(Clone, Default)]
struct ImmediateInner {
    calls: Arc<AtomicUsize>,
    targets: Arc<Mutex<Vec<HostWithPort>>>,
}

impl ImmediateInner {
    fn targets(&self) -> Vec<HostWithPort> {
        lock(&self.targets).clone()
    }
}

impl Service<TestRequest> for ImmediateInner {
    type Output = usize;
    type Error = Infallible;

    async fn serve(&self, request: TestRequest) -> Result<Self::Output, Self::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(target) = request.extensions.get_ref::<ConnectorTarget>() {
            lock(&self.targets).push(target.0.clone());
        }
        Ok(request.id)
    }
}

#[derive(Clone, Default)]
struct HeldInner {
    calls: Arc<AtomicUsize>,
    targets: Arc<Mutex<Vec<HostWithPort>>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl HeldInner {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn targets(&self) -> Vec<HostWithPort> {
        lock(&self.targets).clone()
    }
}

impl Service<TestRequest> for HeldInner {
    type Output = usize;
    type Error = Infallible;

    fn serve(
        &self,
        request: TestRequest,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        let calls = Arc::clone(&self.calls);
        let targets = Arc::clone(&self.targets);
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            let target = request
                .extensions
                .get_ref::<ConnectorTarget>()
                .expect("adaptive DNS should pin a connector target")
                .0
                .clone();
            lock(&targets).push(target);
            started.notify_one();
            release.notified().await;
            Ok(request.id)
        }
    }
}

fn endpoint_config(queue_capacity: usize) -> EndpointConfig {
    EndpointConfig::new(Duration::from_secs(1), 1)
        .with_queue_capacity(queue_capacity)
        .with_max_inflight(16)
}

fn dns_config(
    resolver: MutableResolver,
    queue_capacity: usize,
) -> AdaptiveDnsConfig<MutableResolver> {
    let endpoint = endpoint_config(queue_capacity);
    AdaptiveDnsConfig {
        mode: DnsResolveIpMode::SingleIpV4,
        ..AdaptiveDnsConfig::with_resolver(resolver, endpoint)
    }
}

async fn wait_for_calls(inner: &HeldInner, expected: usize) {
    while inner.calls() < expected {
        tokio::task::yield_now().await;
    }
}

async fn wait_for_queued(
    service: &AdaptiveDnsLoadBalancer<HeldInner, MutableResolver>,
    port: u16,
    expected: usize,
) {
    let hostname = Domain::from_static("example.com");
    loop {
        let queued = service
            .endpoint_snapshots(&hostname, port)
            .await
            .iter()
            .map(|snapshot| snapshot.controller.queued)
            .sum::<usize>();
        if queued == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn p2c_reserves_distinct_idle_endpoints() {
    let resolver =
        MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]);
    let inner = HeldInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.serve(request(1, 443)).await });
    wait_for_calls(&inner, 1).await;

    let second_service = service.clone();
    let second = tokio::spawn(async move { second_service.serve(request(2, 443)).await });
    wait_for_calls(&inner, 2).await;

    let targets = inner.targets();
    assert_eq!(targets.len(), 2);
    assert_ne!(targets[0].host, targets[1].host);

    inner.release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn full_endpoint_set_rejects_without_calling_inner() {
    let resolver =
        MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]);
    let inner = HeldInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.serve(request(1, 443)).await });
    let second_service = service.clone();
    let second = tokio::spawn(async move { second_service.serve(request(2, 443)).await });
    wait_for_calls(&inner, 2).await;

    let third_service = service.clone();
    let third = tokio::spawn(async move { third_service.serve(request(3, 443)).await });
    let fourth_service = service.clone();
    let fourth = tokio::spawn(async move { fourth_service.serve(request(4, 443)).await });
    wait_for_queued(&service, 443, 2).await;

    let result = service.serve(request(5, 443)).await;
    assert!(matches!(
        result,
        Err(DnsServiceError::Rejected(ScheduleError::QueueFull))
    ));
    assert_eq!(inner.calls(), 2);

    third.abort();
    fourth.abort();
    first.abort();
    second.abort();
    let _ = third.await;
    let _ = fourth.await;
    let _ = first.await;
    let _ = second.await;
}

#[tokio::test(start_paused = true)]
async fn endpoint_state_is_scoped_by_port() {
    let resolver = MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1)]);
    let inner = HeldInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.serve(request(1, 443)).await });
    wait_for_calls(&inner, 1).await;
    let queued_service = service.clone();
    let queued = tokio::spawn(async move { queued_service.serve(request(2, 443)).await });
    wait_for_queued(&service, 443, 1).await;

    let other_port_service = service.clone();
    let other_port = tokio::spawn(async move { other_port_service.serve(request(3, 8443)).await });
    wait_for_calls(&inner, 2).await;
    assert!(inner.targets().iter().any(|target| target.port == 8443));

    assert!(matches!(
        service.serve(request(4, 443)).await,
        Err(DnsServiceError::Rejected(ScheduleError::QueueFull))
    ));

    queued.abort();
    inner.release.notify_waiters();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(other_port.await.unwrap().unwrap(), 3);
    let _ = queued.await;
}

#[tokio::test(start_paused = true)]
async fn endpoint_state_survives_dns_refresh() {
    let resolver = MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1)]);
    let inner = ImmediateInner::default();
    let mut config = dns_config(resolver.clone(), 1);
    config.refresh_after = Duration::from_secs(1);
    let service = AdaptiveDnsLoadBalancer::new(inner, config);

    assert_eq!(service.serve(request(1, 443)).await.unwrap(), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(service.serve(request(2, 443)).await.unwrap(), 2);
    while resolver.calls() < 2 {
        tokio::task::yield_now().await;
    }

    let snapshots = service
        .endpoint_snapshots(&Domain::from_static("example.com"), 443)
        .await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].controller.completed, 2);
}

#[tokio::test(start_paused = true)]
async fn removed_endpoints_are_no_longer_selected() {
    let removed = Ipv4Addr::new(10, 0, 0, 1);
    let retained = Ipv4Addr::new(10, 0, 0, 2);
    let resolver = MutableResolver::new(vec![removed, retained]);
    let inner = ImmediateInner::default();
    let mut config = dns_config(resolver.clone(), 1);
    config.refresh_after = Duration::from_secs(1);
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), config);

    service.serve(request(1, 443)).await.unwrap();
    resolver.set(vec![retained]);
    tokio::time::advance(Duration::from_secs(2)).await;
    service.serve(request(2, 443)).await.unwrap();
    while resolver.calls() < 2 {
        tokio::task::yield_now().await;
    }
    for id in 3..10 {
        service.serve(request(id, 443)).await.unwrap();
    }

    let retained = IpAddr::V4(retained);
    assert!(
        inner
            .targets()
            .iter()
            .skip(2)
            .all(|target| target.host == Host::Address(retained))
    );
}

#[tokio::test]
async fn changed_dns_ordering_cannot_deadlock() {
    let first = Ipv4Addr::new(10, 0, 0, 1);
    let second = Ipv4Addr::new(10, 0, 0, 2);
    let resolver = AlternatingResolver {
        first,
        second,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let inner = ImmediateInner::default();
    // This test exercises lock ordering rather than pacing. Give the
    // zero-latency test service a matching startup operating point so the
    // timeout measures deadlock, not the controller's intentional rate.
    let endpoint = EndpointConfig::new(Duration::from_millis(1), 128).with_queue_capacity(128);
    let mut config = AdaptiveDnsConfig {
        mode: DnsResolveIpMode::SingleIpV4,
        ..AdaptiveDnsConfig::with_resolver(resolver, endpoint)
    };
    config.refresh_after = Duration::ZERO;
    let service = AdaptiveDnsLoadBalancer::new(inner, config);

    let mut tasks = Vec::new();
    for id in 0..128 {
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            service.serve(request(id, 443)).await
        }));
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    })
    .await
    .expect("changed DNS ordering deadlocked endpoint locks");
}

#[tokio::test(start_paused = true)]
async fn cancellation_updates_only_the_reserved_endpoint() {
    let resolver =
        MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]);
    let inner = HeldInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    let request_service = service.clone();
    let task = tokio::spawn(async move { request_service.serve(request(1, 443)).await });
    wait_for_calls(&inner, 1).await;
    let selected = inner.targets()[0].host.clone();
    task.abort();
    let _ = task.await;

    let snapshots = service
        .endpoint_snapshots(&Domain::from_static("example.com"), 443)
        .await;
    assert_eq!(snapshots.len(), 2);
    for snapshot in snapshots {
        let was_selected = selected == Host::Address(snapshot.ip);
        assert_eq!(snapshot.controller.completed, 0);
        assert_eq!(snapshot.controller.failures, u64::from(was_selected));
    }
}

#[tokio::test(start_paused = true)]
async fn completion_updates_only_the_reserved_endpoint() {
    let resolver =
        MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]);
    let inner = ImmediateInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    assert_eq!(service.serve(request(1, 443)).await.unwrap(), 1);
    let selected = inner.targets()[0].host.clone();
    let snapshots = service
        .endpoint_snapshots(&Domain::from_static("example.com"), 443)
        .await;

    assert_eq!(snapshots.len(), 2);
    for snapshot in snapshots {
        let was_selected = selected == Host::Address(snapshot.ip);
        assert_eq!(snapshot.controller.completed, u64::from(was_selected));
        assert_eq!(snapshot.controller.failures, 0);
    }
}

#[tokio::test]
async fn pinned_targets_bypass_discovery() {
    let resolver = MutableResolver::new(vec![Ipv4Addr::new(10, 0, 0, 1)]);
    let inner = ImmediateInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver.clone(), 1));
    let request = request(1, 443);
    let pinned = HostWithPort::new(Host::Address(Ipv4Addr::new(192, 0, 2, 10).into()), 8443);
    request.extensions.insert(ConnectorTarget(pinned.clone()));

    assert_eq!(service.serve(request).await.unwrap(), 1);
    assert_eq!(resolver.calls(), 0);
    assert_eq!(inner.targets(), vec![pinned]);
}

#[tokio::test]
async fn empty_dns_result_is_a_discovery_error() {
    let resolver = MutableResolver::new(Vec::new());
    let inner = ImmediateInner::default();
    let service = AdaptiveDnsLoadBalancer::new(inner.clone(), dns_config(resolver, 1));

    assert!(matches!(
        service.serve(request(1, 443)).await,
        Err(DnsServiceError::Discovery(_))
    ));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
}
