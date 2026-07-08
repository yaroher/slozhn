//! Descriptor-driven idempotency, keyed by the STANDARD protobuf method
//! option `idempotency_level` (google.protobuf.MethodOptions):
//!
//! ```protobuf
//! rpc SetIcon(Req) returns (Resp) { option idempotency_level = IDEMPOTENT; }
//! rpc Get(Req) returns (Resp)     { option idempotency_level = NO_SIDE_EFFECTS; }
//! ```
//!
//! [`IdempotencyIndex`] is built once from an embedded file descriptor set
//! (protoc-gen-prost option `file_descriptor_set=true` emits a
//! `FILE_DESCRIPTOR_SET` const next to the generated types). Consumers:
//!
//! - [`IdempotencyKeyLayer`] (client) — attaches an `x-idempotency-key`
//!   metadata entry (UUID v4) to IDEMPOTENT methods so a server-side dedup
//!   can recognize replays;
//! - [`super::RetryLayer::with_file_descriptor_set`] (client) — allows
//!   retries only for methods marked IDEMPOTENT or NO_SIDE_EFFECTS, making
//!   the "only retry idempotent calls" rule machine-checked.

use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use prost::Message as _;

/// Mirror of `google.protobuf.MethodOptions.IdempotencyLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IdempotencyLevel {
    #[default]
    Unknown,
    /// Read-only: safe to retry, safe to cache.
    NoSideEffects,
    /// Repeating the call yields the same outcome: safe to retry.
    Idempotent,
}

impl IdempotencyLevel {
    pub fn retry_safe(self) -> bool {
        matches!(self, Self::NoSideEffects | Self::Idempotent)
    }
}

/// `"/pkg.Service/Method"` → [`IdempotencyLevel`], built from descriptors.
#[derive(Debug, Clone, Default)]
pub struct IdempotencyIndex {
    map: HashMap<String, IdempotencyLevel>,
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

impl IdempotencyIndex {
    /// Parse an embedded `FILE_DESCRIPTOR_SET` (bytes of
    /// `google.protobuf.FileDescriptorSet`).
    pub fn from_descriptor_set(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::default().with_file_descriptor_set(bytes)
    }

    /// Builder form of [`Self::from_descriptor_set`]; chain to merge several
    /// generated crates. Mirrors `RetryPolicy::with_file_descriptor_set`.
    pub fn with_file_descriptor_set(
        mut self,
        encoded: impl AsRef<[u8]>,
    ) -> Result<Self, prost::DecodeError> {
        self.merge_descriptor_set(encoded.as_ref())?;
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
            self.merge_descriptor_set(encoded.as_ref())?;
        }
        Ok(self)
    }

    /// Manual per-method marking, same builder style as `RetryPolicy`.
    /// Method names are normalized to the gRPC form `/package.Service/Method`.
    /// Manual entries override descriptor-derived ones (last write wins).
    pub fn idempotent_method(mut self, method: impl Into<String>) -> Self {
        self.map
            .insert(normalize_method(method), IdempotencyLevel::Idempotent);
        self
    }

    pub fn idempotent_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        for m in methods {
            self.map
                .insert(normalize_method(m), IdempotencyLevel::Idempotent);
        }
        self
    }

    pub fn no_side_effects_method(mut self, method: impl Into<String>) -> Self {
        self.map
            .insert(normalize_method(method), IdempotencyLevel::NoSideEffects);
        self
    }

    pub fn no_side_effects_methods<I, M>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: Into<String>,
    {
        for m in methods {
            self.map
                .insert(normalize_method(m), IdempotencyLevel::NoSideEffects);
        }
        self
    }

    /// Explicitly reset a method to Unknown (e.g. to override a descriptor
    /// marker you distrust).
    pub fn unknown_method(mut self, method: impl Into<String>) -> Self {
        self.map
            .insert(normalize_method(method), IdempotencyLevel::Unknown);
        self
    }

    /// Merge another descriptor set (e.g. from a second generated crate).
    pub fn merge_descriptor_set(&mut self, bytes: &[u8]) -> Result<(), prost::DecodeError> {
        let fds = prost_types::FileDescriptorSet::decode(bytes)?;
        for file in &fds.file {
            let package = file.package();
            for svc in &file.service {
                for method in &svc.method {
                    let level = match method
                        .options
                        .as_ref()
                        .map(|o| o.idempotency_level())
                        .unwrap_or(
                            prost_types::method_options::IdempotencyLevel::IdempotencyUnknown,
                        ) {
                        prost_types::method_options::IdempotencyLevel::NoSideEffects => {
                            IdempotencyLevel::NoSideEffects
                        }
                        prost_types::method_options::IdempotencyLevel::Idempotent => {
                            IdempotencyLevel::Idempotent
                        }
                        prost_types::method_options::IdempotencyLevel::IdempotencyUnknown => {
                            IdempotencyLevel::Unknown
                        }
                    };
                    let path = if package.is_empty() {
                        format!("/{}/{}", svc.name(), method.name())
                    } else {
                        format!("/{}.{}/{}", package, svc.name(), method.name())
                    };
                    self.map.insert(path, level);
                }
            }
        }
        Ok(())
    }

    /// Manual override/addition by explicit level (see also the builder
    /// forms: [`Self::idempotent_method`], [`Self::no_side_effects_method`]).
    pub fn insert(&mut self, path: impl Into<String>, level: IdempotencyLevel) {
        self.map.insert(normalize_method(path), level);
    }

    pub fn level(&self, path: &str) -> IdempotencyLevel {
        self.map.get(path).copied().unwrap_or_default()
    }

    pub fn retry_safe(&self, path: &str) -> bool {
        self.level(path).retry_safe()
    }
}

/// Client layer: attaches `x-idempotency-key` (UUID v4) to methods marked
/// IDEMPOTENT, unless the caller already set one. Read-only methods
/// (NO_SIDE_EFFECTS) need no key; unknown methods are left untouched.
#[derive(Clone)]
pub struct IdempotencyKeyLayer {
    index: Arc<IdempotencyIndex>,
}

pub const IDEMPOTENCY_KEY_METADATA: &str = "x-idempotency-key";

impl IdempotencyKeyLayer {
    pub fn new(index: Arc<IdempotencyIndex>) -> Self {
        Self { index }
    }
}

impl<S> tower::Layer<S> for IdempotencyKeyLayer {
    type Service = IdempotencyKeyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IdempotencyKeyService { inner, index: self.index.clone() }
    }
}

#[derive(Clone)]
pub struct IdempotencyKeyService<S> {
    inner: S,
    index: Arc<IdempotencyIndex>,
}

impl<S, B, RB> tower::Service<http::Request<B>> for IdempotencyKeyService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<RB>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        if self.index.level(req.uri().path()) == IdempotencyLevel::Idempotent
            && !req.headers().contains_key(IDEMPOTENCY_KEY_METADATA)
            && let Ok(v) =
                http::header::HeaderValue::from_str(&uuid::Uuid::new_v4().to_string())
        {
            req.headers_mut().insert(IDEMPOTENCY_KEY_METADATA, v);
        }
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::{Layer, Service, ServiceExt};

    fn descriptor_set() -> Vec<u8> {
        use prost_types::method_options::IdempotencyLevel as PbLevel;
        let method = |name: &str, level: Option<PbLevel>| prost_types::MethodDescriptorProto {
            name: Some(name.to_string()),
            options: level.map(|l| prost_types::MethodOptions {
                idempotency_level: Some(l as i32),
                ..Default::default()
            }),
            ..Default::default()
        };
        let fds = prost_types::FileDescriptorSet {
            file: vec![prost_types::FileDescriptorProto {
                name: Some("t.proto".into()),
                package: Some("t.v1".into()),
                service: vec![prost_types::ServiceDescriptorProto {
                    name: Some("Svc".into()),
                    method: vec![
                        method("Mutate", None),
                        method("Upsert", Some(PbLevel::Idempotent)),
                        method("Get", Some(PbLevel::NoSideEffects)),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        fds.encode_to_vec()
    }

    #[test]
    fn index_reads_standard_option() {
        let idx = IdempotencyIndex::from_descriptor_set(&descriptor_set()).unwrap();
        assert_eq!(idx.level("/t.v1.Svc/Mutate"), IdempotencyLevel::Unknown);
        assert_eq!(idx.level("/t.v1.Svc/Upsert"), IdempotencyLevel::Idempotent);
        assert_eq!(idx.level("/t.v1.Svc/Get"), IdempotencyLevel::NoSideEffects);
        assert!(!idx.retry_safe("/t.v1.Svc/Mutate"));
        assert!(idx.retry_safe("/t.v1.Svc/Upsert"));
        assert!(idx.retry_safe("/t.v1.Svc/Get"));
        assert_eq!(idx.level("/unknown.Svc/M"), IdempotencyLevel::Unknown);
    }

    #[test]
    fn manual_methods_mirror_retry_policy_style() {
        let idx = IdempotencyIndex::from_descriptor_set(&descriptor_set())
            .unwrap()
            .idempotent_method("t.v1.Svc/Mutate") // без слеша — нормализуется
            .no_side_effects_methods(["/x.Svc/List", "x.Svc/Watch"])
            .unknown_method("/t.v1.Svc/Upsert"); // ручной override дескриптора

        assert_eq!(idx.level("/t.v1.Svc/Mutate"), IdempotencyLevel::Idempotent);
        assert_eq!(idx.level("/x.Svc/List"), IdempotencyLevel::NoSideEffects);
        assert_eq!(idx.level("/x.Svc/Watch"), IdempotencyLevel::NoSideEffects);
        assert_eq!(
            idx.level("/t.v1.Svc/Upsert"),
            IdempotencyLevel::Unknown,
            "manual entry overrides the descriptor marker"
        );
        // дескрипторные не задетые ручными — на месте
        assert_eq!(idx.level("/t.v1.Svc/Get"), IdempotencyLevel::NoSideEffects);
    }

    #[tokio::test]
    async fn key_layer_marks_only_idempotent_methods() {
        let idx = Arc::new(IdempotencyIndex::from_descriptor_set(&descriptor_set()).unwrap());
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
        let probe = tower::service_fn({
            let seen = seen.clone();
            move |req: http::Request<()>| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(
                        req.headers()
                            .get(IDEMPOTENCY_KEY_METADATA)
                            .map(|v| v.to_str().unwrap().to_owned()),
                    );
                    Ok::<_, std::convert::Infallible>(http::Response::new(()))
                }
            }
        });
        let mut svc = IdempotencyKeyLayer::new(idx).layer(probe);

        for path in ["/t.v1.Svc/Upsert", "/t.v1.Svc/Mutate", "/t.v1.Svc/Get"] {
            let req = http::Request::builder().uri(path).body(()).unwrap();
            svc.ready().await.unwrap().call(req).await.unwrap();
        }
        // явный ключ не перетирается
        let mut req = http::Request::builder()
            .uri("/t.v1.Svc/Upsert")
            .body(())
            .unwrap();
        req.headers_mut()
            .insert(IDEMPOTENCY_KEY_METADATA, "my-key".parse().unwrap());
        svc.ready().await.unwrap().call(req).await.unwrap();

        let seen = seen.lock().unwrap();
        assert!(seen[0].is_some(), "IDEMPOTENT gets a key");
        assert!(uuid::Uuid::parse_str(seen[0].as_deref().unwrap()).is_ok());
        assert!(seen[1].is_none(), "UNKNOWN untouched");
        assert!(seen[2].is_none(), "NO_SIDE_EFFECTS needs no key");
        assert_eq!(seen[3].as_deref(), Some("my-key"), "explicit key wins");
    }
}
