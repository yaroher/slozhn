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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;

    fn otel_subscriber() -> (impl tracing::Subscriber, opentelemetry_sdk::trace::SdkTracerProvider)
    {
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        (tracing_subscriber::registry().with(layer), provider)
    }

    #[test]
    fn client_injects_valid_traceparent() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let (subscriber, _provider) = otel_subscriber();
        let _guard = tracing::subscriber::set_default(subscriber);

        let span = tracing::info_span!("rpc-test");
        let mut headers = http::HeaderMap::new();
        {
            let _e = span.enter();
            inject(&span, &mut headers);
        }
        let tp = headers
            .get("traceparent")
            .expect("traceparent header must be injected")
            .to_str()
            .unwrap()
            .to_owned();
        // 00-<32 hex trace-id>-<16 hex span-id>-<flags>
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4, "{tp}");
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32);
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(parts[1], "0".repeat(32), "trace id must be non-zero");
        assert_eq!(parts[2].len(), 16);
    }

    #[test]
    fn server_adopts_remote_parent() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let (subscriber, _provider) = otel_subscriber();
        let _guard = tracing::subscriber::set_default(subscriber);

        let remote_trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "traceparent",
            format!("00-{remote_trace}-00f067aa0ba902b7-01").parse().unwrap(),
        );

        let span = tracing::info_span!("rpc-server-test");
        extract_parent(&span, &headers);

        use opentelemetry::trace::TraceContextExt as _;
        let ctx = span.context();
        let adopted = ctx.span().span_context().trace_id().to_string();
        assert_eq!(adopted, remote_trace, "server span must join the remote trace");
    }
}
