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

use std::sync::Mutex;
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

#[cfg(test)]
mod tests {
    use super::*;

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
