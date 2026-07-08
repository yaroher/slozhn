//! Client-side retries for unary calls.
//!
//! `RetryLayer::default()` is deny-by-default: methods are retried only when
//! explicitly allowlisted or when their protobuf descriptor marks them
//! `NO_SIDE_EFFECTS`/`IDEMPOTENT`.
//!
//! When a method is allowed, the request body is buffered up to `buffer_limit`;
//! if it fits, the call is retried on connection-level `UNAVAILABLE`
//! (grpc-status 14 arriving as a trailers-only response — the slozhn
//! connection-lost path) or a transport error, with jittered exponential
//! backoff. Bodies over the limit (streaming requests) pass through with a
//! single attempt — a partially consumed stream must never be silently
//! replayed.
//!
//! NOTE: a retried RPC may execute twice on the server if the response (not
//! the request) was lost — enable this layer for idempotent methods.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use prost::Message as _;
use prost_types::method_options::IdempotencyLevel;
use tonic::body::Body as TonicBody;

/// Retry method selection.
///
/// Method names are normalized to the gRPC form `/package.Service/Method`.
/// Explicit deny entries win over every allow source.
#[derive(Clone, Debug, Default)]
pub struct RetryPolicy {
    retry_methods: HashSet<String>,
    never_retry_methods: HashSet<String>,
    retry_all_buffered: bool,
}

impl RetryPolicy {
    pub fn retry_method(mut self, method: impl Into<String>) -> Self {
        self.retry_methods.insert(normalize_method(method));
        self
    }

    pub fn retry_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        self.retry_methods
            .extend(methods.into_iter().map(normalize_method));
        self
    }

    pub fn never_retry_method(mut self, method: impl Into<String>) -> Self {
        self.never_retry_methods.insert(normalize_method(method));
        self
    }

    pub fn never_retry_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        self.never_retry_methods
            .extend(methods.into_iter().map(normalize_method));
        self
    }

    /// Retry every buffered unary request unless explicitly denied.
    ///
    /// This is intentionally loud: retried RPCs may execute twice server-side.
    pub fn unsafe_retry_all_buffered(mut self) -> Self {
        self.retry_all_buffered = true;
        self
    }

    pub fn with_file_descriptor_set(
        mut self,
        encoded: impl AsRef<[u8]>,
    ) -> Result<Self, prost::DecodeError> {
        self.extend_file_descriptor_set(encoded)?;
        Ok(self)
    }

    pub fn with_file_descriptor_sets<I, B>(
        mut self,
        descriptor_sets: I,
    ) -> Result<Self, prost::DecodeError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for encoded in descriptor_sets {
            self.extend_file_descriptor_set(encoded)?;
        }
        Ok(self)
    }

    fn extend_file_descriptor_set(
        &mut self,
        encoded: impl AsRef<[u8]>,
    ) -> Result<(), prost::DecodeError> {
        let set = prost_types::FileDescriptorSet::decode(encoded.as_ref())?;
        for file in set.file {
            let package = file.package.unwrap_or_default();
            for service in file.service {
                let Some(service_name) = service.name else {
                    continue;
                };
                let qualified_service = if package.is_empty() {
                    service_name
                } else {
                    format!("{package}.{service_name}")
                };
                for method in service.method {
                    if !is_descriptor_retryable(&method) {
                        continue;
                    }
                    let Some(method_name) = method.name else {
                        continue;
                    };
                    self.retry_methods
                        .insert(format!("/{qualified_service}/{method_name}"));
                }
            }
        }
        Ok(())
    }

    fn allows(&self, method: &str) -> bool {
        let method = normalize_method(method);
        if self.never_retry_methods.contains(&method) {
            return false;
        }
        self.retry_all_buffered || self.retry_methods.contains(&method)
    }
}

/// See module docs. `Default`: 3 attempts, 256 KiB buffer, 50 ms base backoff,
/// and no retryable methods.
#[derive(Clone, Debug)]
pub struct RetryLayer {
    pub max_attempts: u32,
    pub buffer_limit: usize,
    pub base_backoff: Duration,
    pub policy: RetryPolicy,
}

impl Default for RetryLayer {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            buffer_limit: 256 * 1024,
            base_backoff: Duration::from_millis(50),
            policy: RetryPolicy::default(),
        }
    }
}

impl RetryLayer {
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    pub fn with_buffer_limit(mut self, buffer_limit: usize) -> Self {
        self.buffer_limit = buffer_limit;
        self
    }

    pub fn with_base_backoff(mut self, base_backoff: Duration) -> Self {
        self.base_backoff = base_backoff;
        self
    }

    pub fn retry_method(mut self, method: impl Into<String>) -> Self {
        self.policy = self.policy.retry_method(method);
        self
    }

    pub fn retry_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        self.policy = self.policy.retry_methods(methods);
        self
    }

    pub fn never_retry_method(mut self, method: impl Into<String>) -> Self {
        self.policy = self.policy.never_retry_method(method);
        self
    }

    pub fn never_retry_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        self.policy = self.policy.never_retry_methods(methods);
        self
    }

    /// Retry every buffered unary request unless explicitly denied.
    ///
    /// This restores the old v1 behavior. Use it only when the wrapped client
    /// is already scoped to idempotent methods.
    pub fn unsafe_retry_all_buffered(mut self) -> Self {
        self.policy = self.policy.unsafe_retry_all_buffered();
        self
    }

    pub fn from_file_descriptor_set(encoded: impl AsRef<[u8]>) -> Result<Self, prost::DecodeError> {
        Self::default().with_file_descriptor_set(encoded)
    }

    pub fn from_file_descriptor_sets<I, B>(descriptor_sets: I) -> Result<Self, prost::DecodeError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        Self::default().with_file_descriptor_sets(descriptor_sets)
    }

    pub fn with_file_descriptor_set(
        mut self,
        encoded: impl AsRef<[u8]>,
    ) -> Result<Self, prost::DecodeError> {
        self.policy = self.policy.with_file_descriptor_set(encoded)?;
        Ok(self)
    }

    pub fn with_file_descriptor_sets<I, B>(
        mut self,
        descriptor_sets: I,
    ) -> Result<Self, prost::DecodeError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.policy = self.policy.with_file_descriptor_sets(descriptor_sets)?;
        Ok(self)
    }
}

impl<S> tower::Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService {
            inner,
            config: self.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RetryService<S> {
    inner: S,
    config: RetryLayer,
}

fn is_unavailable<RB>(resp: &http::Response<RB>) -> bool {
    resp.headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s == "14")
}

fn jittered(base: Duration) -> Duration {
    let half = base / 2;
    half + Duration::from_millis(fastrand::u64(0..=half.as_millis() as u64))
}

fn normalize_method(method: impl Into<String>) -> String {
    let method = method.into();
    let method = method.trim();
    if method.starts_with('/') {
        method.to_owned()
    } else {
        format!("/{method}")
    }
}

fn is_descriptor_retryable(method: &prost_types::MethodDescriptorProto) -> bool {
    let level = method
        .options
        .as_ref()
        .and_then(|options| options.idempotency_level)
        .and_then(|level| IdempotencyLevel::try_from(level).ok());
    matches!(
        level,
        Some(IdempotencyLevel::NoSideEffects | IdempotencyLevel::Idempotent)
    )
}

fn request_method(req: &http::Request<TonicBody>) -> String {
    if let Some(method) = req.extensions().get::<tonic::GrpcMethod<'static>>() {
        return format!("/{}/{}", method.service(), method.method());
    }
    req.uri().path().to_owned()
}

impl<S, RB> tower::Service<http::Request<TonicBody>> for RetryService<S>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<RB>>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let method = request_method(&req);
            if config.max_attempts <= 1 || !config.policy.allows(&method) {
                return inner.call(req).await;
            }

            let (parts, body) = req.into_parts();

            // Буферизуем тело до лимита. Не влезло — одна попытка,
            // префикс + остаток склеиваются обратно без копий остатка.
            let mut collected: Vec<Bytes> = Vec::new();
            let mut total = 0usize;
            let mut body = body;
            let overflow = loop {
                match body.frame().await {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(data) => {
                            total += data.len();
                            collected.push(data);
                            if total > config.buffer_limit {
                                break true;
                            }
                        }
                        Err(_trailer_frame) => {} // клиент trailers не шлёт
                    },
                    Some(Err(_)) => break true, // ошибку тела отдаём вниз как есть
                    None => break false,
                }
            };

            if overflow {
                // невоспроизводимое тело: prefix + недочитанный остаток
                let prefix = http_body_util::Full::new(Bytes::from(collected.into_iter().fold(
                    Vec::new(),
                    |mut acc, b| {
                        acc.extend_from_slice(&b);
                        acc
                    },
                )));
                let chained = TonicBody::new(
                    prefix
                        .map_err(|e: std::convert::Infallible| match e {})
                        .boxed_unsync()
                        .chain_compat(body),
                );
                let req = http::Request::from_parts(parts, chained);
                return inner.call(req).await;
            }

            let buffered = Bytes::from(collected.into_iter().fold(
                Vec::with_capacity(total),
                |mut acc, b| {
                    acc.extend_from_slice(&b);
                    acc
                },
            ));

            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let mut req = http::Request::builder()
                    .method(parts.method.clone())
                    .uri(parts.uri.clone())
                    .version(parts.version)
                    .body(TonicBody::new(
                        http_body_util::Full::new(buffered.clone())
                            .map_err(|e: std::convert::Infallible| match e {}),
                    ))
                    .expect("rebuilt request");
                *req.headers_mut() = parts.headers.clone();

                match inner.call(req).await {
                    Ok(resp) if is_unavailable(&resp) && attempt < config.max_attempts => {
                        tracing::info!(%method, attempt, "retrying after UNAVAILABLE");
                    }
                    Err(_e) if attempt < config.max_attempts => {
                        tracing::info!(%method, attempt, "retrying after transport error");
                    }
                    other => return other,
                }
                futures_timer::Delay::new(jittered(config.base_backoff * 2u32.pow(attempt - 1)))
                    .await;
            }
        })
    }
}

/// `Body::chain` does not exist in http-body-util — a minimal two-part chain.
trait ChainCompat: Sized {
    fn chain_compat(self, rest: TonicBody) -> ChainBody<Self>;
}

impl<B> ChainCompat for B
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
{
    fn chain_compat(self, rest: TonicBody) -> ChainBody<Self> {
        ChainBody {
            first: Some(self),
            rest,
        }
    }
}

pin_project_lite::pin_project! {
    pub struct ChainBody<B> {
        #[pin]
        first: Option<B>,
        #[pin]
        rest: TonicBody,
    }
}

impl<B> http_body::Body for ChainBody<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let mut this = self.project();
        if let Some(first) = this.first.as_mut().as_pin_mut() {
            match std::task::ready!(first.poll_frame(cx)) {
                Some(Ok(f)) => return Poll::Ready(Some(Ok(f))),
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(tonic::Status::internal(e.into().to_string()))));
                }
                None => this.first.set(None),
            }
        }
        this.rest.poll_frame(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tower::{Layer, Service, ServiceExt};

    fn unary_req(payload: &'static [u8]) -> http::Request<TonicBody> {
        http::Request::builder()
            .uri("/t.S/M")
            .body(TonicBody::new(
                http_body_util::Full::new(Bytes::from_static(payload))
                    .map_err(|e: std::convert::Infallible| match e {}),
            ))
            .unwrap()
    }

    fn unary_req_with_extension(payload: &'static [u8]) -> http::Request<TonicBody> {
        let mut req = http::Request::builder()
            .uri("/transport-prefix/does-not-match")
            .body(TonicBody::new(
                http_body_util::Full::new(Bytes::from_static(payload))
                    .map_err(|e: std::convert::Infallible| match e {}),
            ))
            .unwrap();
        req.extensions_mut()
            .insert(tonic::GrpcMethod::new("t.S", "M"));
        req
    }

    fn unavailable_then_ok(
        calls: Arc<AtomicU32>,
    ) -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<String>,
        Error = std::convert::Infallible,
        Future = impl Send,
    > + Clone
    + Send {
        tower::service_fn(move |_req: http::Request<TonicBody>| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let mut resp = http::Response::new(String::new());
                if n == 0 {
                    resp.headers_mut()
                        .insert("grpc-status", "14".parse().unwrap());
                }
                Ok(resp)
            }
        })
    }

    #[tokio::test]
    async fn default_does_not_retry_unknown_method() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer::default().layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn manual_allowlist_retries_unavailable_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer::default()
            .retry_method("/t.S/M")
            .layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    fn descriptor(method: &str, level: Option<IdempotencyLevel>) -> Vec<u8> {
        let mut parts = method.trim_start_matches('/').split('/');
        let service = parts.next().expect("service");
        let method = parts.next().expect("method");
        let (package, service) = service.rsplit_once('.').expect("qualified service");
        let method = prost_types::MethodDescriptorProto {
            name: Some(method.to_owned()),
            options: level.map(|level| prost_types::MethodOptions {
                idempotency_level: Some(level as i32),
                ..Default::default()
            }),
            ..Default::default()
        };
        prost_types::FileDescriptorSet {
            file: vec![prost_types::FileDescriptorProto {
                package: Some(package.to_owned()),
                service: vec![prost_types::ServiceDescriptorProto {
                    name: Some(service.to_owned()),
                    method: vec![method],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn descriptor_idempotency_allows_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let layer = RetryLayer::from_file_descriptor_set(descriptor(
            "/t.S/M",
            Some(IdempotencyLevel::Idempotent),
        ))
        .unwrap();
        let mut svc = layer.layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn grpc_method_extension_drives_policy() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer::default()
            .retry_method("/t.S/M")
            .layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req_with_extension(b"hi"))
            .await
            .unwrap();
        assert!(!resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn descriptor_unknown_does_not_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let layer = RetryLayer::from_file_descriptor_set(descriptor(
            "/t.S/M",
            Some(IdempotencyLevel::IdempotencyUnknown),
        ))
        .unwrap();
        let mut svc = layer.layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_deny_wins_over_allow() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer::default()
            .unsafe_retry_all_buffered()
            .never_retry_method("/t.S/M")
            .layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let always_unavailable = tower::service_fn({
            let calls = calls.clone();
            move |_req: http::Request<TonicBody>| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let mut resp = http::Response::new(String::new());
                    resp.headers_mut()
                        .insert("grpc-status", "14".parse().unwrap());
                    Ok::<_, std::convert::Infallible>(resp)
                }
            }
        });
        let mut svc = RetryLayer::default()
            .retry_method("/t.S/M")
            .layer(always_unavailable);
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"hi"))
            .await
            .unwrap();
        assert!(resp.headers().contains_key("grpc-status")); // последняя неудача отдана как есть
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn oversized_body_passes_through_without_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let mut svc = RetryLayer {
            buffer_limit: 4, // всё, что больше 4 байт — «стрим»
            ..Default::default()
        }
        .retry_method("/t.S/M")
        .layer(unavailable_then_ok(calls.clone()));
        let resp = svc
            .ready()
            .await
            .unwrap()
            .call(unary_req(b"way-too-big-body"))
            .await
            .unwrap();
        // первый ответ (UNAVAILABLE) отдан как есть, ретрая не было
        assert!(resp.headers().contains_key("grpc-status"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
