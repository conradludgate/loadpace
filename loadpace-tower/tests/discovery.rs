use futures_util::StreamExt;
use futures_util::stream;
use loadpace::EndpointConfig;
use loadpace_tower::AdaptiveDiscovery;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::discover::Change;
use tower::{BoxError, Service, ServiceExt};

pin_project_lite::pin_project! {
    struct NotUnpinStream<S> {
        #[pin]
        inner: S,
        _pin: PhantomPinned,
    }
}

impl<S> NotUnpinStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            _pin: PhantomPinned,
        }
    }
}

impl<S: futures_core::Stream> futures_core::Stream for NotUnpinStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

#[derive(Clone, Default)]
struct BoxedEcho;

impl Service<u64> for BoxedEcho {
    type Response = u64;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<u64, BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: u64) -> Self::Future {
        Box::pin(async move { Ok(request) })
    }
}

#[tokio::test(start_paused = true)]
async fn discovery_wraps_inserts_and_preserves_removes() {
    let changes = stream::iter([
        Ok::<_, BoxError>(Change::Insert(7_u64, BoxedEcho)),
        Ok::<_, BoxError>(Change::Remove(7_u64)),
    ]);
    let mut discovery = AdaptiveDiscovery::<_, u64>::new(changes, EndpointConfig::default());

    let insert = discovery.next().await.unwrap().unwrap();
    let endpoint = match insert {
        Change::Insert(key, endpoint) => {
            assert_eq!(key, 7);
            endpoint
        }
        Change::Remove(_) => panic!("expected an insert"),
    };
    assert_eq!(endpoint.oneshot(11).await.unwrap(), 11);

    assert!(matches!(
        discovery.next().await.unwrap().unwrap(),
        Change::Remove(7)
    ));
}

#[tokio::test(start_paused = true)]
async fn discovery_can_feed_towers_p2c_balance() {
    let changes = stream::iter([
        Ok::<_, BoxError>(Change::Insert(1_u64, BoxedEcho)),
        Ok::<_, BoxError>(Change::Insert(2_u64, BoxedEcho)),
    ]);
    let discovery = AdaptiveDiscovery::<_, u64>::new(changes, EndpointConfig::default());
    let balance = tower::balance::p2c::Balance::new(discovery);

    assert_eq!(balance.oneshot(99).await.unwrap(), 99);
}

#[tokio::test(start_paused = true)]
async fn discovery_accepts_a_non_unpin_stream() {
    let changes = NotUnpinStream::new(stream::iter([
        Ok::<_, BoxError>(Change::Insert(7_u64, BoxedEcho)),
        Ok::<_, BoxError>(Change::Remove(7_u64)),
    ]));
    let discovery = AdaptiveDiscovery::<_, u64>::new(changes, EndpointConfig::default());
    futures_util::pin_mut!(discovery);

    let insert = discovery.next().await.unwrap().unwrap();
    assert!(matches!(insert, Change::Insert(7, _)));
    assert!(matches!(
        discovery.next().await.unwrap().unwrap(),
        Change::Remove(7)
    ));
}
