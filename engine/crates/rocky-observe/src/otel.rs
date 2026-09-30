//! Feature-gated OTLP exporters for Rocky.
//!
//! Bridges the in-process [`RunMetrics`](crate::metrics::RunMetrics) counters and
//! histograms to an OpenTelemetry Collector (or any OTLP-compatible backend such
//! as Datadog Agent, Grafana Alloy, or Honeycomb).
//!
//! This module also builds the OTLP span and metric exporters
//! (`build_span_exporter` and `build_metric_exporter`, both crate-private).
//! Every OTLP pipeline in the crate goes through them, so the transport and the
//! retry policy are chosen in one place.
//!
//! Gated behind the `otel` Cargo feature — when disabled, this module is not
//! compiled and Rocky has zero OpenTelemetry dependencies.
//!
//! # Environment variables
//!
//! | Variable | Default | Description |
//! |---|---|---|
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | gRPC endpoint of the OTLP collector |
//! | `OTEL_SERVICE_NAME` | `rocky` | Value of the `service.name` resource attribute |

use std::sync::Mutex;
use std::sync::atomic::Ordering;

use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{
    ExporterBuildError, MetricExporter, RetryPolicy, SpanExporter, WithExportConfig,
    WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tracing::{debug, error, info};

use crate::metrics::METRICS;

const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "rocky";

/// The retry policy of every OTLP exporter Rocky builds: none, so an export is
/// a single attempt.
///
/// `opentelemetry-otlp` 0.33 turned exponential-backoff retries on by default.
/// Rocky keeps its earlier behaviour (decision on #2167): a dependency bump
/// should not change what a run does at exit, and with retries on, a collector
/// that is down could hold `rocky run` open while the exporter backs off — the
/// final flush of spans and metrics waits on the export. Turning retries on is
/// a separate change and needs a bounded total wait.
fn retry_policy() -> RetryPolicy {
    RetryPolicy::disabled()
}

/// Builds the OTLP gRPC span exporter that sends to `endpoint`.
///
/// This is the only place Rocky constructs a span exporter. Must be called from
/// within a Tokio runtime (the tonic channel requires one).
///
/// # Errors
///
/// Returns the exporter's build error, for example when `endpoint` is not a
/// valid URL.
pub(crate) fn build_span_exporter(endpoint: &str) -> Result<SpanExporter, ExporterBuildError> {
    SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_retry_policy(retry_policy())
        .build()
}

/// Builds the OTLP gRPC metric exporter that sends to `endpoint`.
///
/// This is the only place Rocky constructs a metric exporter: the per-run
/// exporter ([`OtelExporter`]) and the resident scheduler's meter both use it.
/// Must be called from within a Tokio runtime (the tonic channel requires one).
///
/// # Errors
///
/// Returns the exporter's build error, for example when `endpoint` is not a
/// valid URL.
pub(crate) fn build_metric_exporter(endpoint: &str) -> Result<MetricExporter, ExporterBuildError> {
    MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_retry_policy(retry_policy())
        .build()
}

/// Wraps an OTLP-backed `SdkMeterProvider` and exposes helpers for
/// pushing Rocky's in-process metrics to an external collector.
pub struct OtelExporter {
    provider: SdkMeterProvider,
    /// Cursor over `METRICS.table_durations_ms` — index of the next
    /// unrecorded observation. Each call to [`Self::export_metrics`] reads
    /// the slice from this cursor onwards and feeds the new observations
    /// into the OTLP histogram instrument, so the periodic flush stays
    /// idempotent and the run-end JSON snapshot (which reads the full Vec)
    /// is not disturbed.
    table_cursor: Mutex<usize>,
    query_cursor: Mutex<usize>,
}

impl OtelExporter {
    /// Initialise the OTLP exporter.
    ///
    /// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4317`) and
    /// `OTEL_SERVICE_NAME` (default `rocky`) from the environment.
    ///
    /// Must be called from within a Tokio runtime (the tonic gRPC transport
    /// requires one).
    ///
    /// # Errors
    ///
    /// Returns an error if the OTLP exporter or meter provider cannot be built
    /// (e.g. invalid endpoint URL).
    pub fn init() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());

        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());

        Self::init_with_endpoint(&endpoint, &service_name)
    }

    /// [`Self::init`] with the endpoint and service name given, not read from
    /// the environment.
    fn init_with_endpoint(
        endpoint: &str,
        service_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!(endpoint = %endpoint, service = %service_name, "initialising OTLP metrics exporter");

        let exporter = build_metric_exporter(endpoint)?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(std::time::Duration::from_secs(30))
            .build();

        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .build();

        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        debug!("OTLP meter provider initialised");

        Ok(OtelExporter {
            provider,
            table_cursor: Mutex::new(0),
            query_cursor: Mutex::new(0),
        })
    }

    /// Record the current in-process metrics snapshot as OTLP observations.
    ///
    /// Counter values are emitted as gauges (last-value-wins is the right
    /// shape for monotonic process-local counters); duration distributions
    /// are emitted as histograms. The `PeriodicReader` flushes the
    /// instruments to the collector on its next interval (or on
    /// [`shutdown`](Self::shutdown)).
    ///
    /// Histogram observations are streamed from the cursor stored on
    /// `self` so successive flushes never double-count. The source `Vec`
    /// in [`crate::metrics::METRICS`] is never drained — callers reading
    /// the run-end JSON snapshot still see every observation.
    pub fn export_metrics(&self) {
        let meter = self.provider.meter("rocky");

        // --- Counters (monotonic totals) ---
        let tables_processed = meter
            .u64_gauge("rocky.tables_processed")
            .with_description("Total tables processed in this run")
            .build();
        tables_processed.record(METRICS.tables_processed.load(Ordering::Relaxed), &[]);

        let tables_failed = meter
            .u64_gauge("rocky.tables_failed")
            .with_description("Total tables that failed in this run")
            .build();
        tables_failed.record(METRICS.tables_failed.load(Ordering::Relaxed), &[]);

        let statements_executed = meter
            .u64_gauge("rocky.statements_executed")
            .with_description("Total SQL statements executed")
            .build();
        statements_executed.record(METRICS.statements_executed.load(Ordering::Relaxed), &[]);

        let retries_attempted = meter
            .u64_gauge("rocky.retries_attempted")
            .with_description("Total retries attempted")
            .build();
        retries_attempted.record(METRICS.retries_attempted.load(Ordering::Relaxed), &[]);

        let retries_succeeded = meter
            .u64_gauge("rocky.retries_succeeded")
            .with_description("Total retries that succeeded")
            .build();
        retries_succeeded.record(METRICS.retries_succeeded.load(Ordering::Relaxed), &[]);

        let anomalies_detected = meter
            .u64_gauge("rocky.anomalies_detected")
            .with_description("Total anomalies detected")
            .build();
        anomalies_detected.record(METRICS.anomalies_detected.load(Ordering::Relaxed), &[]);

        // --- Derived metrics from the snapshot ---
        let snap = METRICS.snapshot();

        let error_rate = meter
            .f64_gauge("rocky.error_rate_pct")
            .with_description("Error rate as a percentage")
            .build();
        error_rate.record(snap.error_rate_pct, &[]);

        // --- Duration histograms ---
        //
        // OTel semantic conventions specify duration metrics as histogram
        // instruments (the backend computes percentiles across hosts/runs).
        // The previous shape — emitting in-process p50/p95/max as gauges —
        // both lost information at the OTLP boundary and conflicted with
        // semantic conventions. Switch to histograms recording each raw
        // observation; cursor state on `self` keeps the periodic reader
        // idempotent (no double-counting between flushes) without draining
        // the source Vec, so the run-end JSON snapshot still sees every
        // observation.
        let table_durations = meter
            .u64_histogram("rocky.table_duration_ms")
            .with_unit("ms")
            .with_description("Table materialisation duration distribution")
            .build();
        let table_obs = METRICS.read_table_durations();
        for ms in advance_cursor(&self.table_cursor, &table_obs) {
            table_durations.record(ms, &[]);
        }

        let query_durations = meter
            .u64_histogram("rocky.query_duration_ms")
            .with_unit("ms")
            .with_description("SQL statement duration distribution")
            .build();
        let query_obs = METRICS.read_query_durations();
        for ms in advance_cursor(&self.query_cursor, &query_obs) {
            query_durations.record(ms, &[]);
        }

        debug!("OTLP metrics recorded — awaiting next periodic flush");
    }

    /// Flush pending metrics and shut down the meter provider.
    ///
    /// Call this before process exit to ensure the final batch is sent.
    pub fn shutdown(self) {
        info!("shutting down OTLP metrics exporter");
        if let Err(e) = self.provider.shutdown() {
            error!(error = %e, "OTLP meter provider shutdown failed");
        }
    }
}

/// Returns the slice of observations the caller has not yet consumed and
/// advances the cursor past them. The `min()` guards against an
/// out-of-bounds cursor in the unlikely event the source `Vec` is
/// truncated externally (it isn't today, but defending against future
/// drift is cheap).
fn advance_cursor(cursor: &Mutex<usize>, observations: &[u64]) -> Vec<u64> {
    let mut guard = cursor.lock().expect("OTLP histogram cursor poisoned");
    let start = (*guard).min(observations.len());
    let new = observations[start..].to_vec();
    *guard = observations.len();
    new
}

/// A real gRPC collector for tests that need to count export attempts.
///
/// It answers every request with `UNAVAILABLE` and logs the path of each request
/// it receives, so the log's length is the number of attempts an exporter made.
/// `UNAVAILABLE` with no retry hint is a status `opentelemetry-otlp` classes as
/// retryable; `INTERNAL` or `UNKNOWN` would prove nothing, because the exporter
/// never retries those.
#[cfg(test)]
pub(crate) mod test_collector {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use tonic::body::Body;
    use tonic::codegen::{Service, http};
    use tonic::transport::Server;
    use tonic::transport::server::TcpIncoming;

    /// The path of a trace export request.
    pub(crate) const TRACE_EXPORT: &str =
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
    /// The path of a metrics export request.
    pub(crate) const METRICS_EXPORT: &str =
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";

    /// A local collector that is always unavailable. Serving stops when it drops.
    pub(crate) struct UnavailableCollector {
        endpoint: String,
        requests: Arc<Mutex<Vec<String>>>,
        server: tokio::task::JoinHandle<()>,
    }

    impl UnavailableCollector {
        /// Starts the collector on a free local port. Needs a Tokio runtime.
        pub(crate) async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a local port for the test collector");
            let endpoint = format!(
                "http://{}",
                listener.local_addr().expect("test collector address")
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let service = Unavailable {
                requests: Arc::clone(&requests),
            };
            let server = tokio::spawn(async move {
                let served = Server::builder()
                    .serve_with_incoming(service, TcpIncoming::from(listener))
                    .await;
                if let Err(e) = served {
                    panic!("test collector stopped serving: {e}");
                }
            });
            Self {
                endpoint,
                requests,
                server,
            }
        }

        /// The `http://host:port` endpoint to point an exporter at.
        pub(crate) fn endpoint(&self) -> &str {
            &self.endpoint
        }

        /// The path of every request received so far, oldest first.
        pub(crate) fn requests(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("test collector request log")
                .clone()
        }
    }

    impl Drop for UnavailableCollector {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    /// Answers every gRPC call with `UNAVAILABLE`, after logging its path.
    #[derive(Clone)]
    struct Unavailable {
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl Service<http::Request<Body>> for Unavailable {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<Body>) -> Self::Future {
            self.requests
                .lock()
                .expect("test collector request log")
                .push(request.uri().path().to_owned());
            std::future::ready(Ok(tonic::Status::unavailable(
                "test collector is unavailable",
            )
            .into_http()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_collector::{METRICS_EXPORT, UnavailableCollector};
    use super::*;

    /// The per-run metrics exporter makes exactly one export attempt against a
    /// collector that answers `UNAVAILABLE`. `opentelemetry-otlp` 0.33 retries by
    /// default (four requests here), so this fails if the exporter built by
    /// [`OtelExporter::init`] loses its `RetryPolicy::disabled()`.
    #[tokio::test(flavor = "multi_thread")]
    async fn run_metrics_export_is_a_single_attempt_when_the_collector_is_unavailable() {
        let collector = UnavailableCollector::start().await;
        let exporter = OtelExporter::init_with_endpoint(collector.endpoint(), "rocky-test")
            .expect("build the run metrics exporter");
        exporter.export_metrics();
        // `shutdown` blocks on the final export, so keep it off the runtime
        // threads that drive the exporter's channel and the collector.
        tokio::task::spawn_blocking(move || exporter.shutdown())
            .await
            .expect("run metrics shutdown thread");

        assert_eq!(collector.requests(), [METRICS_EXPORT]);
    }

    #[test]
    fn advance_cursor_returns_only_new_observations() {
        let cursor = Mutex::new(0);
        let obs = vec![10, 20, 30];
        assert_eq!(advance_cursor(&cursor, &obs), vec![10, 20, 30]);
        assert_eq!(*cursor.lock().unwrap(), 3);

        // No new observations since the last flush.
        assert!(advance_cursor(&cursor, &obs).is_empty());
        assert_eq!(*cursor.lock().unwrap(), 3);

        // Two more observations appear; the next flush should record only those.
        let obs = vec![10, 20, 30, 40, 50];
        assert_eq!(advance_cursor(&cursor, &obs), vec![40, 50]);
        assert_eq!(*cursor.lock().unwrap(), 5);
    }

    #[test]
    fn advance_cursor_handles_empty_initial_state() {
        let cursor = Mutex::new(0);
        let obs: Vec<u64> = vec![];
        assert!(advance_cursor(&cursor, &obs).is_empty());
        assert_eq!(*cursor.lock().unwrap(), 0);
    }

    #[test]
    fn advance_cursor_clamps_when_cursor_exceeds_length() {
        // Defensive: if the source Vec ever shrinks below the cursor (it
        // won't today — `read_table_durations` clones an append-only
        // Mutex<Vec>) the helper must not panic on slice-out-of-bounds.
        let cursor = Mutex::new(10);
        let obs = vec![1, 2, 3];
        assert!(advance_cursor(&cursor, &obs).is_empty());
        assert_eq!(*cursor.lock().unwrap(), 3);
    }
}
