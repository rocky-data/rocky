//! Lifecycle wrapper around the OTLP **metrics** exporter
//! ([`rocky_observe::otel::OtelExporter`]).
//!
//! Trace exporting now lives in `rocky_observe::tracing_setup` —
//! `init_tracing` returns a `TracingGuard` that owns the tracer
//! provider so spans are emitted from process-startup forward. This
//! guard remains responsible for the metrics path only:
//!
//! - reads the in-process `RunMetrics` snapshot and pushes it through
//!   the OTLP meter provider on a 30s periodic interval,
//! - flushes a final snapshot on `Drop` so the exit-time counters
//!   reach the collector before `rocky run` returns.
//!
//! When the `otel` feature is disabled, the module compiles to a
//! zero-size struct so callers in `commands/run.rs` don't have to
//! sprinkle `cfg` attributes at every call site.

#[cfg(feature = "otel")]
pub struct OtelGuard {
    exporter: Option<rocky_observe::otel::OtelExporter>,
}

#[cfg(feature = "otel")]
impl OtelGuard {
    /// Initialise the OTLP exporter when `OTEL_EXPORTER_OTLP_ENDPOINT`
    /// is set in the environment. Returns a guard that is a no-op when
    /// OTel wasn't configured (so callers can hold one unconditionally).
    #[must_use]
    pub fn init_if_enabled() -> Self {
        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
            return Self { exporter: None };
        }
        match rocky_observe::otel::OtelExporter::init() {
            Ok(e) => {
                tracing::info!("OTLP metrics exporter initialised");
                Self { exporter: Some(e) }
            }
            Err(err) => {
                tracing::warn!(error = %err, "OTLP exporter init failed, continuing without OTel");
                Self { exporter: None }
            }
        }
    }

    /// Flush the current metrics snapshot to the OTLP collector. Called
    /// from [`Drop`] as well, but exposing it lets `rocky run` force a
    /// flush before the periodic reader's next interval when the
    /// process is about to exit.
    pub fn flush(&self) {
        if let Some(e) = &self.exporter {
            e.export_metrics();
        }
    }
}

#[cfg(feature = "otel")]
impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(e) = self.exporter.take() {
            e.export_metrics();
            e.shutdown();
        }
    }
}

#[cfg(not(feature = "otel"))]
pub struct OtelGuard;

#[cfg(not(feature = "otel"))]
impl OtelGuard {
    /// No-op when the `otel` feature is disabled. Constructing and
    /// dropping the guard costs nothing at runtime.
    #[must_use]
    pub fn init_if_enabled() -> Self {
        Self
    }

    /// No-op when the `otel` feature is disabled.
    pub fn flush(&self) {}
}
