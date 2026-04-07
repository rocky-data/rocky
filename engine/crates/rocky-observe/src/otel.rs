//! Feature-gated OTLP metrics exporter for Rocky.
//!
//! Bridges the in-process [`RunMetrics`](crate::metrics::RunMetrics) counters and
//! histograms to an OpenTelemetry Collector (or any OTLP-compatible backend such
//! as Datadog Agent, Grafana Alloy, or Honeycomb).
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

use std::sync::atomic::Ordering;

use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tracing::{debug, error, info};

use crate::metrics::METRICS;

const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "rocky";

/// Wraps an OTLP-backed `SdkMeterProvider` and exposes helpers for
/// pushing Rocky's in-process metrics to an external collector.
pub struct OtelExporter {
    provider: SdkMeterProvider,
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

        info!(endpoint = %endpoint, service = %service_name, "initialising OTLP metrics exporter");

        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(std::time::Duration::from_secs(30))
            .build();

        let resource = Resource::builder().with_service_name(service_name).build();

        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        debug!("OTLP meter provider initialised");

        Ok(OtelExporter { provider })
    }

    /// Record the current in-process metrics snapshot as OTLP gauge/counter
    /// observations.
    ///
    /// This reads the global [`METRICS`] singleton, creates OTel instruments on
    /// the meter, and records the current values. The `PeriodicReader` will
    /// flush them to the collector on its next interval (or on [`shutdown`](Self::shutdown)).
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

        // --- Duration histograms (p50, p95, max) ---
        let table_p50 = meter
            .u64_gauge("rocky.table_duration_p50_ms")
            .with_description("Table materialisation duration p50 (ms)")
            .build();
        table_p50.record(snap.table_duration_p50_ms, &[]);

        let table_p95 = meter
            .u64_gauge("rocky.table_duration_p95_ms")
            .with_description("Table materialisation duration p95 (ms)")
            .build();
        table_p95.record(snap.table_duration_p95_ms, &[]);

        let table_max = meter
            .u64_gauge("rocky.table_duration_max_ms")
            .with_description("Table materialisation duration max (ms)")
            .build();
        table_max.record(snap.table_duration_max_ms, &[]);

        let query_p50 = meter
            .u64_gauge("rocky.query_duration_p50_ms")
            .with_description("Query duration p50 (ms)")
            .build();
        query_p50.record(snap.query_duration_p50_ms, &[]);

        let query_p95 = meter
            .u64_gauge("rocky.query_duration_p95_ms")
            .with_description("Query duration p95 (ms)")
            .build();
        query_p95.record(snap.query_duration_p95_ms, &[]);

        let query_max = meter
            .u64_gauge("rocky.query_duration_max_ms")
            .with_description("Query duration max (ms)")
            .build();
        query_max.record(snap.query_duration_max_ms, &[]);

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
