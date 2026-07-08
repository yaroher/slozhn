//! W3C traceparent propagation (feature `otel`): the client injects the
//! current span context into request headers, the server adopts it as the
//! span parent. Uses the globally installed text-map propagator
//! (`opentelemetry::global::set_text_map_propagator`).

use opentelemetry::propagation::{Extractor, Injector};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct HeaderInjector<'a>(&'a mut http::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            key.parse::<http::header::HeaderName>(),
            http::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

struct HeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

pub(crate) fn inject(span: &tracing::Span, headers: &mut http::HeaderMap) {
    let context = span.context();
    opentelemetry::global::get_text_map_propagator(|prop| {
        prop.inject_context(&context, &mut HeaderInjector(headers));
    });
}

pub(crate) fn extract_parent(span: &tracing::Span, headers: &http::HeaderMap) {
    let parent = opentelemetry::global::get_text_map_propagator(|prop| {
        prop.extract(&HeaderExtractor(headers))
    });
    span.set_parent(parent);
}
