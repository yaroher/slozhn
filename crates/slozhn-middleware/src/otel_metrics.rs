//! Optional bridge from the [`metrics`] facade into OpenTelemetry metrics
//! (`otel` feature): install [`otel_metrics_recorder`] as the global
//! `metrics` recorder and everything emitted through the facade — the
//! [`super::MetricsLayer`] series and any `metrics::counter!`/`gauge!`/
//! `histogram!` calls of your own — flows into an OTel `Meter`, and from
//! there through whatever OTLP/Prometheus pipeline the `SdkMeterProvider`
//! is wired to.
//!
//! ```ignore
//! let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
//!     .with_reader(/* OTLP exporter / Prometheus reader */)
//!     .build();
//! metrics::set_global_recorder(
//!     slozhn::middleware::otel_metrics_recorder(provider.meter("slozhn")),
//! )?;
//! ```
//!
//! Mapping: `metrics` labels become OTel attributes verbatim; counters →
//! `u64` monotonic Counter (`absolute` is emulated by adding the positive
//! delta from the last absolute value); gauges → synchronous `f64` Gauge
//! (increment/decrement maintain a shadow value per label set, `set`
//! records directly); histograms → `f64` Histogram. `describe_*` units and
//! descriptions are applied when an instrument is first created; a
//! `describe_*` that arrives after the first registration of that name is
//! ignored (OTel instruments are immutable once built).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, SharedString, Unit};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;

/// Build a [`metrics::Recorder`] forwarding into `meter`. Install it with
/// `metrics::set_global_recorder` (once, at process start).
pub fn otel_metrics_recorder(meter: Meter) -> OtelMetricsRecorder {
    OtelMetricsRecorder {
        meter,
        counters: Mutex::new(HashMap::new()),
        gauges: Mutex::new(HashMap::new()),
        histograms: Mutex::new(HashMap::new()),
        descriptions: Mutex::new(HashMap::new()),
    }
}

/// See [`otel_metrics_recorder`].
pub struct OtelMetricsRecorder {
    meter: Meter,
    // One OTel instrument per metric NAME (attributes vary per handle);
    // creating the same-name instrument repeatedly would be tolerated by
    // the SDK but is wasteful and floods it with duplicate registrations.
    counters: Mutex<HashMap<String, opentelemetry::metrics::Counter<u64>>>,
    gauges: Mutex<HashMap<String, opentelemetry::metrics::Gauge<f64>>>,
    histograms: Mutex<HashMap<String, opentelemetry::metrics::Histogram<f64>>>,
    descriptions: Mutex<HashMap<String, (Option<Unit>, SharedString)>>,
}

fn attrs_of(key: &Key) -> Vec<KeyValue> {
    key.labels()
        .map(|l| KeyValue::new(l.key().to_owned(), l.value().to_owned()))
        .collect()
}

impl OtelMetricsRecorder {
    fn describe(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.descriptions
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), (unit, description));
    }

    fn description_of(&self, name: &str) -> (Option<Unit>, Option<SharedString>) {
        match self.descriptions.lock().unwrap().get(name) {
            Some((unit, description)) => (*unit, Some(description.clone())),
            None => (None, None),
        }
    }
}

struct OtelCounterHandle {
    inner: opentelemetry::metrics::Counter<u64>,
    attrs: Vec<KeyValue>,
    /// Last value seen via `absolute`, to emulate it on a monotonic add.
    last_absolute: AtomicU64,
}

impl metrics::CounterFn for OtelCounterHandle {
    fn increment(&self, value: u64) {
        self.inner.add(value, &self.attrs);
    }

    fn absolute(&self, value: u64) {
        let prev = self.last_absolute.swap(value, Ordering::AcqRel);
        let delta = value.saturating_sub(prev);
        if delta > 0 {
            self.inner.add(delta, &self.attrs);
        }
    }
}

struct OtelGaugeHandle {
    inner: opentelemetry::metrics::Gauge<f64>,
    attrs: Vec<KeyValue>,
    /// Shadow value so increment/decrement can report an absolute reading
    /// (OTel synchronous gauges only take absolute values). f64 bits in an
    /// AtomicU64 — per-handle, one handle per (name, label set).
    value: AtomicU64,
}

impl OtelGaugeHandle {
    fn update(&self, f: impl Fn(f64) -> f64) {
        let mut cur = self.value.load(Ordering::Acquire);
        loop {
            let next = f(f64::from_bits(cur)).to_bits();
            match self.value.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.record(f64::from_bits(next), &self.attrs);
                    return;
                }
                Err(actual) => cur = actual,
            }
        }
    }
}

impl metrics::GaugeFn for OtelGaugeHandle {
    fn increment(&self, value: f64) {
        self.update(|v| v + value);
    }

    fn decrement(&self, value: f64) {
        self.update(|v| v - value);
    }

    fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Release);
        self.inner.record(value, &self.attrs);
    }
}

struct OtelHistogramHandle {
    inner: opentelemetry::metrics::Histogram<f64>,
    attrs: Vec<KeyValue>,
}

impl metrics::HistogramFn for OtelHistogramHandle {
    fn record(&self, value: f64) {
        self.inner.record(value, &self.attrs);
    }
}

impl metrics::Recorder for OtelMetricsRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe(key, unit, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let name = key.name().to_owned();
        let inner = self
            .counters
            .lock()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| {
                let (unit, description) = self.description_of(&name);
                let mut b = self.meter.u64_counter(name.clone());
                if let Some(unit) = unit {
                    b = b.with_unit(unit.as_canonical_label());
                }
                if let Some(description) = description {
                    b = b.with_description(description.to_string());
                }
                b.build()
            })
            .clone();
        Counter::from_arc(Arc::new(OtelCounterHandle {
            inner,
            attrs: attrs_of(key),
            last_absolute: AtomicU64::new(0),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let name = key.name().to_owned();
        let inner = self
            .gauges
            .lock()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| {
                let (unit, description) = self.description_of(&name);
                let mut b = self.meter.f64_gauge(name.clone());
                if let Some(unit) = unit {
                    b = b.with_unit(unit.as_canonical_label());
                }
                if let Some(description) = description {
                    b = b.with_description(description.to_string());
                }
                b.build()
            })
            .clone();
        Gauge::from_arc(Arc::new(OtelGaugeHandle {
            inner,
            attrs: attrs_of(key),
            value: AtomicU64::new(0f64.to_bits()),
        }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let name = key.name().to_owned();
        let inner = self
            .histograms
            .lock()
            .unwrap()
            .entry(name.clone())
            .or_insert_with(|| {
                let (unit, description) = self.description_of(&name);
                let mut b = self.meter.f64_histogram(name.clone());
                if let Some(unit) = unit {
                    b = b.with_unit(unit.as_canonical_label());
                }
                if let Some(description) = description {
                    b = b.with_description(description.to_string());
                }
                b.build()
            })
            .clone();
        Histogram::from_arc(Arc::new(OtelHistogramHandle { inner, attrs: attrs_of(key) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::AggregatedMetrics;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    };

    fn setup() -> (SdkMeterProvider, InMemoryMetricExporter, OtelMetricsRecorder) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let recorder = otel_metrics_recorder(provider.meter("test"));
        (provider, exporter, recorder)
    }

    fn collect(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<opentelemetry_sdk::metrics::data::ResourceMetrics> {
        provider.force_flush().unwrap();
        exporter.get_finished_metrics().unwrap()
    }

    #[test]
    fn counter_gauge_histogram_flow_to_otel() {
        let (provider, exporter, recorder) = setup();

        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_total", "method" => "/t.S/M").increment(3);
            let g = metrics::gauge!("test_inflight");
            g.increment(2.0);
            g.decrement(1.0);
            metrics::histogram!("test_seconds", "code" => "0").record(0.25);
        });

        let rms = collect(&provider, &exporter);
        let metrics_by_name: std::collections::HashMap<String, &AggregatedMetrics> = rms
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|s| s.metrics())
            .map(|m| (m.name().to_owned(), m.data()))
            .collect();

        match metrics_by_name.get("test_total").expect("counter exported") {
            AggregatedMetrics::U64(opentelemetry_sdk::metrics::data::MetricData::Sum(sum)) => {
                let point = sum.data_points().next().expect("one point");
                assert_eq!(point.value(), 3);
                assert!(
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == "method"
                            && kv.value.as_str() == "/t.S/M"),
                    "label became an attribute"
                );
            }
            other => panic!("unexpected counter aggregation: {other:?}"),
        }

        match metrics_by_name.get("test_inflight").expect("gauge exported") {
            AggregatedMetrics::F64(opentelemetry_sdk::metrics::data::MetricData::Gauge(g)) => {
                let point = g.data_points().next().expect("one point");
                assert_eq!(point.value(), 1.0, "2 - 1 = 1");
            }
            other => panic!("unexpected gauge aggregation: {other:?}"),
        }

        match metrics_by_name.get("test_seconds").expect("histogram exported") {
            AggregatedMetrics::F64(opentelemetry_sdk::metrics::data::MetricData::Histogram(
                h,
            )) => {
                let point = h.data_points().next().expect("one point");
                assert_eq!(point.count(), 1);
                assert_eq!(point.sum(), 0.25);
            }
            other => panic!("unexpected histogram aggregation: {other:?}"),
        }
    }

    #[test]
    fn metrics_layer_series_reach_otel() {
        use tower::{Layer, Service, ServiceExt};

        let (provider, exporter, recorder) = setup();

        metrics::with_local_recorder(&recorder, || {
            let svc = tower::service_fn(|_req: http::Request<()>| async {
                let mut resp = http::Response::new(());
                resp.headers_mut().insert("grpc-status", "0".parse().unwrap());
                Ok::<_, std::convert::Infallible>(resp)
            });
            let mut traced = crate::MetricsLayer::client().layer(svc);

            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async {
                let req = http::Request::builder().uri("/t.S/M").body(()).unwrap();
                let resp = traced.ready().await.unwrap().call(req).await.unwrap();
                drop(resp);
            });
        });

        let rms = collect(&provider, &exporter);
        let names: Vec<String> = rms
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|s| s.metrics())
            .map(|m| m.name().to_owned())
            .collect();
        assert!(names.iter().any(|n| n == "slozhn_rpc_started_total"), "{names:?}");
        assert!(names.iter().any(|n| n == "slozhn_rpc_inflight"), "{names:?}");
        assert!(names.iter().any(|n| n == "slozhn_rpc_duration_seconds"), "{names:?}");
    }
}
