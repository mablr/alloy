use crate::{Http, HttpConnect};
use alloy_json_rpc::{RequestPacket, ResponsePacket};
use alloy_transport::{
    utils::guess_local_url, BoxTransport, TransportConnect, TransportError, TransportErrorKind,
    TransportFut, TransportResult,
};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    header, Request, Response,
};
use hyper_util::client::legacy::Error;
use std::{future::Future, marker::PhantomData, pin::Pin, task};
use tower::{Layer, Service};
use tracing::{debug, debug_span, trace, Instrument};

#[cfg(feature = "hyper-tls")]
type Hyper = hyper_util::client::legacy::Client<
    hyper_tls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<::hyper::body::Bytes>,
>;

#[cfg(not(feature = "hyper-tls"))]
type Hyper = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    http_body_util::Full<::hyper::body::Bytes>,
>;

/// A [`hyper`] based transport client.
pub type HyperTransport = Http<HyperClient>;

impl HyperTransport {
    /// Create a new [`HyperTransport`] with the given URL and default hyper client.
    pub fn new_hyper(url: url::Url) -> Self {
        let client = HyperClient::new();
        Self::with_client(client, url)
    }
}

/// A [hyper] based client that can be used with tower layers.
#[derive(Clone, Debug)]
pub struct HyperClient<B = Full<Bytes>, S = Hyper> {
    service: S,
    _pd: PhantomData<B>,
}

/// Alias for [`Response<Incoming>`]
pub type HyperResponse = Response<Incoming>;

/// Alias for pinned box future that results in [`HyperResponse`]
pub type HyperResponseFut<T = HyperResponse, E = Error> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

impl HyperClient {
    /// Create a new [HyperClient] with the given URL and default hyper client.
    pub fn new() -> Self {
        let executor = hyper_util::rt::TokioExecutor::new();

        #[cfg(feature = "hyper-tls")]
        let service = hyper_util::client::legacy::Client::builder(executor)
            .build(hyper_tls::HttpsConnector::new());

        #[cfg(not(feature = "hyper-tls"))]
        let service =
            hyper_util::client::legacy::Client::builder(executor).build_http::<Full<Bytes>>();
        Self { service, _pd: PhantomData }
    }

    /// Create a layer builder for composing tower middleware layers with the default hyper client.
    ///
    /// This allows you to easily add layers like [`AuthLayer`] without manually constructing
    /// [`tower::ServiceBuilder`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use alloy_transport_http::{HyperClient, AuthLayer};
    /// # use alloy_rpc_types_engine::JwtSecret;
    /// let secret = JwtSecret::random();
    /// let client = HyperClient::new()
    ///     .layer(AuthLayer::new(secret))
    ///     .fuse();
    /// ```
    ///
    /// [`AuthLayer`]: crate::AuthLayer
    pub const fn layer<L: Layer<Hyper>>(self, layer: L) -> HyperClientLayered<L> {
        HyperClientLayered { inner_client: self, layer }
    }
}

impl Default for HyperClient {
    fn default() -> Self {
        Self::new()
    }
}

impl<B, S> HyperClient<B, S> {
    /// Create a new [HyperClient] with the given URL and service.
    pub const fn with_service(service: S) -> Self {
        Self { service, _pd: PhantomData }
    }
}

/// A layered hyper client that allows composing tower middleware layers.
///
/// This type is created by calling [`HyperClient::layer`] and allows you to add
/// additional layers before finalizing the client.
#[derive(Clone, Debug)]
pub struct HyperClientLayered<L> {
    inner_client: HyperClient,
    layer: L,
}

impl<L> HyperClientLayered<L>
where
    L: Layer<Hyper> + Clone,
    L::Service: Service<Request<Full<Bytes>>, Response = Response<Incoming>>
        + Clone
        + Send
        + Sync
        + 'static,
    <L::Service as Service<Request<Full<Bytes>>>>::Future: Send,
    <L::Service as Service<Request<Full<Bytes>>>>::Error: std::error::Error + Send + Sync + 'static,
{
    /// Finalize the layered client configuration and return a [`HyperClient`] ready to use.
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use alloy_transport_http::{HyperClient, AuthLayer};
    /// # use alloy_rpc_types_engine::JwtSecret;
    /// let secret = JwtSecret::random();
    /// let client = HyperClient::new()
    ///     .layer(AuthLayer::new(secret))
    ///     .fuse();
    /// ```
    pub fn fuse(self) -> HyperClient<Full<Bytes>, L::Service> {
        let service = self.layer.layer(self.inner_client.service);
        HyperClient::with_service(service)
    }
}

impl<B, S, ResBody> Http<HyperClient<B, S>>
where
    S: Service<Request<B>, Response = Response<ResBody>> + Clone + Send + Sync + 'static,
    S::Future: Send,
    S::Error: std::error::Error + Send + Sync + 'static,
    B: From<Vec<u8>> + Send + 'static + Clone,
    ResBody: BodyExt + Send + 'static,
    ResBody::Error: std::error::Error + Send + Sync + 'static,
    ResBody::Data: Send,
{
    async fn do_hyper(self, req: RequestPacket) -> TransportResult<ResponsePacket> {
        debug!(count = req.len(), "sending request packet to server");

        let mut builder = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(self.url.as_str())
            .header(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));

        // Add any additional headers from the request packet.
        for (name, value) in req.headers().iter() {
            builder = builder.header(name, value);
        }

        let ser = req.serialize().map_err(TransportError::ser_err)?;
        // convert the Box<RawValue> into a hyper request<B>
        let body = ser.get().as_bytes().to_owned().into();

        let req = builder.body(body).map_err(TransportErrorKind::custom)?;

        let mut service = self.client.service;
        let resp = service.call(req).await.map_err(TransportErrorKind::custom)?;

        let status = resp.status();

        debug!(%status, "received response from server");

        // Unpack data from the response body. We do this regardless of
        // the status code, as we want to return the error in the body
        // if there is one.
        let body = resp.into_body().collect().await.map_err(TransportErrorKind::custom)?.to_bytes();

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!(body = %String::from_utf8_lossy(&body), "response body");
        } else {
            debug!(bytes = body.len(), "retrieved response body. Use `trace` for full body");
        }

        if !status.is_success() {
            return Err(TransportErrorKind::http_error(
                status.as_u16(),
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }

        // Deserialize a Box<RawValue> from the body. If deserialization fails, return
        // the body as a string in the error. The conversion to String
        // is lossy and may not cover all the bytes in the body.
        serde_json::from_slice(&body)
            .map_err(|err| TransportError::deser_err(err, String::from_utf8_lossy(body.as_ref())))
    }
}

impl TransportConnect for HttpConnect<HyperTransport> {
    fn is_local(&self) -> bool {
        guess_local_url(self.url.as_str())
    }

    async fn get_transport(&self) -> Result<BoxTransport, TransportError> {
        Ok(BoxTransport::new(Http::with_client(HyperClient::new(), self.url.clone())))
    }
}

impl<B, S> Service<RequestPacket> for Http<HyperClient<B, S>>
where
    S: Service<Request<B>, Response = HyperResponse> + Clone + Send + Sync + 'static,
    S::Future: Send,
    S::Error: std::error::Error + Send + Sync + 'static,
    B: From<Vec<u8>> + Send + 'static + Clone + Sync,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
        // `hyper` always returns `Ok(())`.
        task::Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone();
        let span = debug_span!("HyperTransport", url = %this.url);
        Box::pin(this.do_hyper(req).instrument(span))
    }
}
