//! Message validation middleware, modeled on go-grpc-middleware's validator
//! interceptor, adapted to a byte-level tower stack: incoming gRPC messages
//! are decoded and checked BEFORE the handler runs, and the failure is cast
//! into a gRPC status by a user-supplied function — so the response can carry
//! anything, including a domain error proto in `grpc-status-details-bin`
//! (via `tonic::Status::with_details`).
//!
//! Three registration levels, most convenient first:
//!
//! - **Zero registration** (`validate` feature): [`ValidateLayer::from_descriptor_set`]
//!   walks the embedded `FILE_DESCRIPTOR_SET`, maps every service method to
//!   its input message descriptor, and validates each message reflectively
//!   with `prost-reflect-validate` against the PGV rules
//!   (`validate.rules`) stored in the descriptor options. One line covers
//!   every method of every service.
//! - **Typed fast path** (`validate` feature): [`ValidateLayer::message`]
//!   registers a concrete message type whose derived
//!   `prost_validate::Validator` impl is used instead of reflection
//!   (~an order of magnitude faster; use for hot methods).
//! - **Manual rules**: [`ValidateLayer::method`] takes a typed closure for
//!   rules PGV cannot express. No optional dependencies required.
//!
//! Explicit registrations override the reflective default for that method.
//!
//! The **caster** ([`ValidateLayer::caster`]) receives all collected
//! violations (`Vec<E>`) and produces the final `tonic::Status`; it fully
//! owns the code, the message, the details bytes, and any metadata. Note:
//! `prost-validate` is currently fail-fast, so the vec holds one entry per
//! failing message today — the signature already fits a future
//! validate-all upstream without an API break.
//!
//! Mechanics: the request body is wrapped and gRPC length-prefixed messages
//! are re-assembled across chunk boundaries; each complete message is
//! validated as it flows. A violation surfaces as a request-body error
//! carrying the caster's `tonic::Status`, which tonic recovers verbatim
//! (details included) and returns to the caller — one mechanism for unary
//! and every streaming shape (message N of a client stream is checked the
//! same way as a unary request). The layer works on the request direction,
//! so it validates inbound messages on a server stack and outbound messages
//! on a client stack (fail fast, before the network).
//!
//! Deliberate edges: methods with no validator get a passthrough body (one
//! map lookup, zero wrapping); compressed messages (flag byte = 1) are NOT
//! validated (we don't decompress); a message that fails to decode is passed
//! through untouched — the tonic codec downstream owns that error.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use http_body::Frame;
use pin_project_lite::pin_project;
use tonic::body::Body as TonicBody;

type MsgValidator<E> = Arc<dyn Fn(&[u8]) -> Result<(), Vec<E>> + Send + Sync>;
type Caster<E> = Arc<dyn Fn(&str, Vec<E>) -> tonic::Status + Send + Sync>;

/// See the module docs.
pub struct ValidateLayer<E> {
    validators: HashMap<String, MsgValidator<E>>,
    caster: Caster<E>,
}

impl<E> Clone for ValidateLayer<E> {
    fn clone(&self) -> Self {
        Self { validators: self.validators.clone(), caster: self.caster.clone() }
    }
}

fn normalize(method: String) -> String {
    if method.starts_with('/') { method } else { format!("/{method}") }
}

impl<E: std::fmt::Display + Send + Sync + 'static> ValidateLayer<E> {
    /// Empty layer with the default caster: `INVALID_ARGUMENT`, the
    /// violations joined into the status message, no details. Chain
    /// [`Self::caster`] to take full control of the response.
    pub fn new() -> Self {
        Self {
            validators: HashMap::new(),
            caster: Arc::new(|_method, violations: Vec<E>| {
                let msg = violations
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
                tonic::Status::invalid_argument(msg)
            }),
        }
    }
}

impl<E: std::fmt::Display + Send + Sync + 'static> Default for ValidateLayer<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> ValidateLayer<E> {
    /// Full control over how violations become the gRPC response: code,
    /// message, `grpc-status-details-bin` (via `Status::with_details`) —
    /// e.g. encode a domain error proto and ship it in the details.
    pub fn caster(
        mut self,
        f: impl Fn(&str, Vec<E>) -> tonic::Status + Send + Sync + 'static,
    ) -> Self {
        self.caster = Arc::new(f);
        self
    }

    /// Manual typed validator for one method (`"/pkg.Service/Method"`).
    /// The layer decodes `M` from the message bytes and calls `f` on every
    /// request message (all streaming shapes included). Overrides any
    /// reflective/typed registration for the same method.
    pub fn method<M: prost::Message + Default>(
        mut self,
        method: impl Into<String>,
        f: impl Fn(&M) -> Result<(), Vec<E>> + Send + Sync + 'static,
    ) -> Self {
        self.validators.insert(
            normalize(method.into()),
            Arc::new(move |bytes| {
                match M::decode(bytes) {
                    Ok(m) => f(&m),
                    // malformed protobuf → not ours to report, the tonic
                    // codec downstream will produce its own decode error
                    Err(_) => Ok(()),
                }
            }),
        );
        self
    }
}

#[cfg(feature = "validate")]
impl ValidateLayer<prost_validate::Error> {
    /// Zero-registration mode: validate EVERY method of EVERY service found
    /// in the encoded `FileDescriptorSet` against the PGV rules embedded in
    /// the descriptor options, via runtime reflection
    /// (`prost-reflect-validate`). Messages without rules validate
    /// trivially. Requires the descriptor set to be compiled with
    /// `validate/validate.proto` imported (extension options retained —
    /// prost's `file_descriptor_set` output qualifies).
    pub fn from_descriptor_set(
        bytes: &[u8],
    ) -> Result<Self, prost_reflect::DescriptorError> {
        Self::from_descriptor_sets([bytes])
    }

    /// Like [`Self::from_descriptor_set`], but merging several descriptor
    /// sets — needed when the generator emits one `FILE_DESCRIPTOR_SET` per
    /// proto package (protoc-gen-prost does): pass dependencies first, e.g.
    /// `[validate::FILE_DESCRIPTOR_SET, my_pkg::FILE_DESCRIPTOR_SET]`.
    /// Google well-known types are pre-loaded and never need passing.
    pub fn from_descriptor_sets<I, B>(
        descriptor_sets: I,
    ) -> Result<Self, prost_reflect::DescriptorError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        // global() starts with the well-known types (descriptor.proto etc.),
        // which per-package descriptor sets reference but don't contain
        let mut pool = prost_reflect::DescriptorPool::global();
        for bytes in descriptor_sets {
            pool.decode_file_descriptor_set(bytes.as_ref())?;
        }
        let mut layer = Self::new();
        for svc in pool.services() {
            for method in svc.methods() {
                let path = format!("/{}/{}", svc.full_name(), method.name());
                let input = method.input();
                layer.validators.insert(
                    path,
                    Arc::new(move |bytes| {
                        let msg = match prost_reflect::DynamicMessage::decode(
                            input.clone(),
                            bytes,
                        ) {
                            Ok(m) => m,
                            Err(_) => return Ok(()), // tonic codec's problem
                        };
                        prost_reflect_validate::validate(&msg).map_err(|e| vec![e])
                    }),
                );
            }
        }
        Ok(layer)
    }

    /// Typed fast path: use `M`'s derived `prost_validate::Validator` impl
    /// (from `prost-validate-build`) instead of reflection for this method.
    pub fn message<M>(self, method: impl Into<String>) -> Self
    where
        M: prost::Message + Default + prost_validate::Validator,
    {
        self.method(method, |m: &M| {
            prost_validate::Validator::validate(m).map_err(|e| vec![e])
        })
    }
}

impl<S, E> tower::Layer<S> for ValidateLayer<E> {
    type Service = ValidateService<S, E>;

    fn layer(&self, inner: S) -> Self::Service {
        ValidateService {
            inner,
            validators: Arc::new(self.validators.clone()),
            caster: self.caster.clone(),
        }
    }
}

pub struct ValidateService<S, E> {
    inner: S,
    validators: Arc<HashMap<String, MsgValidator<E>>>,
    caster: Caster<E>,
}

impl<S: Clone, E> Clone for ValidateService<S, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            validators: self.validators.clone(),
            caster: self.caster.clone(),
        }
    }
}

impl<S, E, RB> tower::Service<http::Request<TonicBody>> for ValidateService<S, E>
where
    S: tower::Service<http::Request<TonicBody>, Response = http::Response<RB>>,
    E: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let method = req.uri().path();
        let Some(validator) = self.validators.get(method) else {
            // no validator for this method — zero-cost passthrough
            return self.inner.call(req);
        };
        let validator = validator.clone();
        let caster = self.caster.clone();
        let method = method.to_owned();

        let req = req.map(move |body| {
            TonicBody::new(ValidatedBody {
                inner: body,
                validator,
                caster,
                method,
                buf: BytesMut::new(),
                failed: false,
            })
        });
        self.inner.call(req)
    }
}

pin_project! {
    /// Request-body wrapper: re-assembles gRPC length-prefixed messages
    /// across chunk boundaries and validates each complete one. A violation
    /// turns into a body error carrying the caster's `tonic::Status`, which
    /// tonic recovers verbatim (details included) as the RPC outcome.
    struct ValidatedBody<E> {
        #[pin]
        inner: TonicBody,
        validator: MsgValidator<E>,
        caster: Caster<E>,
        method: String,
        // Accumulates the byte stream for message re-assembly; consumed
        // message-by-message, so it holds at most one partial message.
        buf: BytesMut,
        failed: bool,
    }
}

impl<E> ValidatedBody<E> {
    /// Validate every complete length-prefixed message currently in `buf`.
    fn check_buffered(
        buf: &mut BytesMut,
        validator: &MsgValidator<E>,
    ) -> Result<(), Vec<E>> {
        loop {
            if buf.len() < 5 {
                return Ok(());
            }
            let compressed = buf[0] != 0;
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if buf.len() < 5 + len {
                return Ok(()); // partial message — wait for more chunks
            }
            buf.advance(5);
            let message = buf.split_to(len);
            if !compressed {
                // compressed messages are skipped by design: validating
                // would require decompressing here
                validator(&message)?;
            }
        }
    }
}

impl<E: Send + Sync + 'static> http_body::Body for ValidatedBody<E> {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }
        match std::task::ready!(this.inner.poll_frame(cx)) {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => {
                    this.buf.extend_from_slice(&data);
                    match Self::check_buffered(this.buf, this.validator) {
                        Ok(()) => Poll::Ready(Some(Ok(Frame::data(data)))),
                        Err(violations) => {
                            *this.failed = true;
                            tracing::debug!(
                                method = %this.method,
                                violations = violations.len(),
                                "request message failed validation",
                            );
                            metrics::counter!(
                                "slozhn_validation_failed_total",
                                "method" => this.method.clone(),
                            )
                            .increment(1);
                            let status = (this.caster)(this.method, violations);
                            Poll::Ready(Some(Err(status)))
                        }
                    }
                }
                Err(other) => Poll::Ready(Some(Ok(other))), // trailers — pass
            },
            Some(Err(e)) => Poll::Ready(Some(Err(tonic::Status::from_error(e.into())))),
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt as _;
    use prost::Message as _;
    use tower::{Layer, Service, ServiceExt};

    /// Tiny hand-rolled test message: field 1 = string name.
    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMsg {
        #[prost(string, tag = "1")]
        name: String,
    }

    fn grpc_frame(msg: &TestMsg) -> Vec<u8> {
        let payload = msg.encode_to_vec();
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(0u8);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Inner service that fully reads the request body and reports the
    /// outcome: body bytes on success, the body error otherwise.
    type DrainResult = Result<http::Response<Result<Bytes, tonic::Status>>, std::convert::Infallible>;

    fn draining_service() -> impl tower::Service<
        http::Request<TonicBody>,
        Response = http::Response<Result<Bytes, tonic::Status>>,
        Error = std::convert::Infallible,
        Future = impl std::future::Future<Output = DrainResult> + Send,
    > + Clone
    + Send {
        tower::service_fn(|req: http::Request<TonicBody>| async move {
            let out = match req.into_body().collect().await {
                Ok(collected) => Ok(collected.to_bytes()),
                Err(e) => Err(e),
            };
            Ok(http::Response::new(out))
        })
    }

    fn name_rule() -> ValidateLayer<String> {
        ValidateLayer::<String>::new().method("/t.S/M", |m: &TestMsg| {
            if m.name.len() < 3 {
                Err(vec![format!("name too short: {:?}", m.name)])
            } else {
                Ok(())
            }
        })
    }

    fn req(path: &str, body: Vec<u8>) -> http::Request<TonicBody> {
        http::Request::builder()
            .uri(path)
            .body(TonicBody::new(
                http_body_util::Full::new(Bytes::from(body))
                    .map_err(|e: std::convert::Infallible| match e {}),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn valid_message_passes_through_unchanged() {
        let mut svc = name_rule().layer(draining_service());
        let body = grpc_frame(&TestMsg { name: "alice".into() });

        let resp = svc.ready().await.unwrap().call(req("/t.S/M", body.clone())).await.unwrap();
        assert_eq!(resp.into_body().unwrap(), Bytes::from(body));
    }

    #[tokio::test]
    async fn invalid_message_yields_caster_status_with_details() {
        let layer = name_rule().caster(|method, violations: Vec<String>| {
            assert_eq!(method, "/t.S/M");
            tonic::Status::with_details(
                tonic::Code::InvalidArgument,
                violations.join(","),
                Bytes::from_static(b"domain-details-bytes"),
            )
        });
        let mut svc = layer.layer(draining_service());
        let body = grpc_frame(&TestMsg { name: "x".into() });

        let resp = svc.ready().await.unwrap().call(req("/t.S/M", body)).await.unwrap();
        let status = resp.into_body().unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("name too short"));
        assert_eq!(status.details(), b"domain-details-bytes");
    }

    #[tokio::test]
    async fn unregistered_method_is_untouched() {
        let mut svc = name_rule().layer(draining_service());
        // would be invalid under the rule, but the method has no validator
        let body = grpc_frame(&TestMsg { name: "x".into() });

        let resp = svc.ready().await.unwrap().call(req("/t.S/Other", body.clone())).await.unwrap();
        assert_eq!(resp.into_body().unwrap(), Bytes::from(body));
    }

    #[tokio::test]
    async fn second_stream_message_is_validated() {
        let mut svc = name_rule().layer(draining_service());
        let mut body = grpc_frame(&TestMsg { name: "alice".into() });
        body.extend_from_slice(&grpc_frame(&TestMsg { name: "x".into() }));

        let resp = svc.ready().await.unwrap().call(req("/t.S/M", body)).await.unwrap();
        let status = resp.into_body().unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn message_split_across_chunks_is_reassembled() {
        // stream the frame byte-by-byte through a channel-backed body
        let frame_bytes = grpc_frame(&TestMsg { name: "x".into() });
        let stream = futures::stream::iter(
            frame_bytes
                .iter()
                .map(|b| Ok::<_, tonic::Status>(Frame::data(Bytes::copy_from_slice(&[*b]))))
                .collect::<Vec<_>>(),
        );
        let body = TonicBody::new(http_body_util::StreamBody::new(stream));
        let req = http::Request::builder().uri("/t.S/M").body(body).unwrap();

        let mut svc = name_rule().layer(draining_service());
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        let status = resp.into_body().unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "reassembled and validated");
    }

    #[cfg(feature = "validate")]
    impl prost_validate::Validator for TestMsg {
        fn validate(&self) -> prost_validate::Result {
            if self.name.len() < 3 {
                Err(prost_validate::Error::new(
                    "name",
                    prost_validate::errors::string::Error::MinLen(3),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[cfg(feature = "validate")]
    #[tokio::test]
    async fn typed_message_registration_uses_validator_impl() {
        let layer = ValidateLayer::<prost_validate::Error>::new().message::<TestMsg>("/t.S/M");
        let mut svc = layer.layer(draining_service());

        let ok = grpc_frame(&TestMsg { name: "alice".into() });
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", ok.clone())).await.unwrap();
        assert_eq!(resp.into_body().unwrap(), Bytes::from(ok));

        let bad = grpc_frame(&TestMsg { name: "x".into() });
        let resp = svc.ready().await.unwrap().call(req("/t.S/M", bad)).await.unwrap();
        let status = resp.into_body().unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("name"), "{}", status.message());
    }

    #[tokio::test]
    async fn compressed_message_is_skipped() {
        let mut svc = name_rule().layer(draining_service());
        let payload = TestMsg { name: "x".into() }.encode_to_vec();
        let mut body = vec![1u8]; // compressed flag set
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(&payload);

        let resp = svc.ready().await.unwrap().call(req("/t.S/M", body.clone())).await.unwrap();
        assert_eq!(resp.into_body().unwrap(), Bytes::from(body), "compressed → not validated");
    }
}
