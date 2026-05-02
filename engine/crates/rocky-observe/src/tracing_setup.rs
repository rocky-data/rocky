//! Structured-logging initialisation for the Rocky CLI.
//!
//! [`init_tracing`] sets up the [`tracing`] subscriber registry with the
//! `fmt` layer (JSON or compact, written to stderr). When the `otel`
//! feature is enabled and `OTEL_EXPORTER_OTLP_ENDPOINT` is set, it also:
//!
//! - registers the `tracing-opentelemetry` layer so every existing
//!   `tracing::info_span!` is exported via OTLP gRPC,
//! - installs the W3C `TraceContextPropagator` so a `TRACEPARENT` env
//!   var (the convention Dagster, OpenTelemetry SDKs, and most
//!   orchestrators use to forward trace context to subprocesses) is
//!   honoured,
//! - returns a [`TracingGuard`] that owns the tracer provider; the OTel
//!   batch span processor only flushes its final buffer on
//!   `provider.shutdown()`, so the caller in `main.rs` must hold this
//!   guard for the lifetime of the process or risk losing the last
//!   spans on exit.
//!
//! Metrics initialisation is still owned by `OtelGuard` in `rocky-cli`
//! (it lives there because the metrics flush is wired into the `rocky
//! run` lifecycle and reads the run-end snapshot). This split keeps
//! the *tracer* provider — which must be installed before the first
//! `info_span!` call we want captured — at the top of `main`, while
//! the *metrics* flush stays scoped to the run.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

/// Returned from [`init_tracing`]. Holds optional OTel state that must
/// outlive the program so the batch span processor flushes on drop.
///
/// The non-`otel` build is a zero-size type so callers don't need
/// `cfg` attributes around the guard binding.
pub struct TracingGuard {
    #[cfg(feature = "otel")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self.tracer_provider.take() {
            // Flush + shutdown the batch span processor so the final
            // batch reaches the collector before the process exits.
            // Errors here are non-fatal — log them and move on; we're
            // already on the exit path.
            if let Err(e) = provider.shutdown() {
                tracing::warn!(error = %e, "OTLP tracer provider shutdown failed");
            }
        }
    }
}

impl TracingGuard {
    fn new_disabled() -> Self {
        Self {
            #[cfg(feature = "otel")]
            tracer_provider: None,
        }
    }
}

/// Initialises structured logging and (when the `otel` feature is
/// enabled and `OTEL_EXPORTER_OTLP_ENDPOINT` is set) the OTel tracing
/// layer.
///
/// - `json=true`: JSON output to stderr (for machine consumption,
///   dagster-rocky).
/// - `json=false`: human-readable compact output (for local dev).
///
/// Log level controlled by `RUST_LOG` env var (default `info`).
///
/// **Hold the returned [`TracingGuard`] for the lifetime of the
/// process** — dropping it earlier flushes and shuts down the batch
/// span processor, after which subsequent spans go to the no-op
/// tracer.
#[must_use]
pub fn init_tracing(json: bool) -> TracingGuard {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // `Layer::boxed()` erases the layer type so the if/else branch
    // returns a uniform `BoxLayer` that still implements
    // `Layer<Layered<...>>` for downstream composition. Always write
    // logs to stderr so stdout stays reserved for the structured
    // JSON output of `discover` / `plan` / `run`.
    let fmt_layer = if json {
        fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(false)
            .with_writer(std::io::stderr)
            .boxed()
    } else {
        fmt::layer()
            .with_target(false)
            .with_thread_ids(false)
            .compact()
            .with_writer(std::io::stderr)
            .boxed()
    };

    #[cfg(feature = "otel")]
    {
        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() {
            match build_otel_tracer() {
                Ok(provider) => {
                    use opentelemetry::trace::TracerProvider as _;
                    let tracer = provider.tracer("rocky");
                    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer).boxed();
                    tracing_subscriber::registry()
                        .with(filter)
                        .with(fmt_layer)
                        .with(otel_layer)
                        .init();
                    install_propagator();
                    return TracingGuard {
                        tracer_provider: Some(provider),
                    };
                }
                Err(e) => {
                    // Fall through to the fmt-only registry so the CLI
                    // keeps working when the OTLP endpoint is unreachable
                    // or misconfigured. The error is logged via the fmt
                    // layer once it's installed.
                    tracing_subscriber::registry()
                        .with(filter)
                        .with(fmt_layer)
                        .init();
                    tracing::warn!(
                        error = %e,
                        "OTLP tracer init failed; continuing without trace export"
                    );
                    return TracingGuard::new_disabled();
                }
            }
        }
    }

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
    TracingGuard::new_disabled()
}

#[cfg(feature = "otel")]
fn build_otel_tracer()
-> Result<opentelemetry_sdk::trace::SdkTracerProvider, Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

    const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
    const DEFAULT_SERVICE_NAME: &str = "rocky";

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .build();

    // ParentBased(AlwaysOn) honours the upstream sampler decision when
    // a remote `traceparent` is propagated (Dagster orchestrator), and
    // falls back to "always sample" when running stand-alone. A finer
    // sampling decision (head-based at a configurable rate, tail-based
    // for failures) is left for a follow-up — 1000-table runs do
    // produce 1000+ spans per run.
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .build();

    tracing::info!(
        endpoint = %endpoint,
        service = %service_name,
        "OTLP tracer provider initialised"
    );

    Ok(provider)
}

#[cfg(feature = "otel")]
fn install_propagator() {
    // Install the W3C TraceContext propagator globally so that
    // `extract_remote_context` (used at the top of subcommand entries)
    // can pull the parent span out of carrier headers / env vars.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
}

/// Reads the `TRACEPARENT` env var (W3C trace context) and returns the
/// extracted [`opentelemetry::Context`], if any. The convention is set
/// by orchestrators that launch Rocky as a subprocess (Dagster,
/// Airflow's OpenLineage, the OTel Operator) so the spans we emit
/// land as children of the orchestrator's run.
///
/// Returns `None` when the env var is unset, malformed, or when the
/// `otel` feature is disabled.
///
/// Use the returned context with
/// `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent` on a
/// `tracing::Span`, e.g.:
///
/// ```ignore
/// use tracing_opentelemetry::OpenTelemetrySpanExt;
/// let span = tracing::info_span!("rocky.run");
/// if let Some(cx) = rocky_observe::tracing_setup::extract_remote_context() {
///     let _ = span.set_parent(cx);
/// }
/// let _enter = span.enter();
/// ```
#[cfg(feature = "otel")]
#[must_use]
pub fn extract_remote_context() -> Option<opentelemetry::Context> {
    use opentelemetry::propagation::TextMapPropagator;
    use std::collections::HashMap;

    let traceparent = std::env::var("TRACEPARENT").ok()?;
    if traceparent.is_empty() {
        return None;
    }

    let mut carrier = HashMap::new();
    carrier.insert("traceparent".to_string(), traceparent);
    if let Ok(tracestate) = std::env::var("TRACESTATE") {
        if !tracestate.is_empty() {
            carrier.insert("tracestate".to_string(), tracestate);
        }
    }

    let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
    let cx = propagator.extract(&carrier);

    // The propagator returns a non-empty Context even on bad input —
    // the only way to detect "no valid parent" is to inspect the
    // SpanContext on the extracted Context.
    use opentelemetry::trace::TraceContextExt;
    if cx.span().span_context().is_valid() {
        Some(cx)
    } else {
        None
    }
}

/// Compatibility no-op for builds without the `otel` feature.
#[cfg(not(feature = "otel"))]
#[must_use]
pub fn extract_remote_context() -> Option<()> {
    None
}

#[cfg(all(test, feature = "otel"))]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
    use tracing::info_span;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;

    /// Cargo runs tests in this module in parallel within the same
    /// process, so any test that reads / writes `TRACEPARENT` must hold
    /// this mutex for the duration of its env mutation. Without it the
    /// "set / extract / restore" sequence in one test races against the
    /// "remove / extract / restore" in another, producing flaky failures
    /// where `extract_remote_context_parses_valid_traceparent` sees an
    /// empty env var because a sibling test happened to be in the
    /// remove-restore window.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// In-memory `SpanExporter` used to assert that spans land in the
    /// pipeline. Mirrors the pattern in `tracing-opentelemetry`'s own
    /// test suite.
    #[derive(Clone, Default, Debug)]
    struct CapturingExporter(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for CapturingExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            if let Ok(mut g) = self.0.lock() {
                g.extend(batch);
            }
            Ok(())
        }
    }

    fn capturing_subscriber() -> (
        CapturingExporter,
        SdkTracerProvider,
        impl tracing::Subscriber,
    ) {
        let exporter = CapturingExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("rocky_test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(layer);
        (exporter, provider, subscriber)
    }

    /// PR 1 smoke test: an existing `info_span!` produces a span that
    /// reaches the OTLP-side exporter once the layer is registered.
    /// Asserts the layer routing is wired correctly — without it,
    /// `dag_node` / `discover_sources` / `pipeline.run` etc. silently
    /// drop on the floor.
    #[test]
    fn info_span_lands_at_otel_exporter() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (exporter, provider, subscriber) = capturing_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("pipeline.run", rocky.test = "smoke");
            let _enter = span.enter();
            // Some work happens here in production; the smoke test just
            // creates and exits the span so the batch exporter can pick
            // it up on shutdown.
        });

        provider.shutdown().expect("provider shutdown");
        let captured = exporter.0.lock().unwrap();
        assert!(
            captured.iter().any(|s| s.name == "pipeline.run"),
            "expected to see pipeline.run span in the exporter, got: {:?}",
            captured.iter().map(|s| s.name.as_ref()).collect::<Vec<_>>()
        );
    }

    /// `extract_remote_context` returns `None` when the env var is
    /// unset (the no-Dagster path). Set + restore so the test is
    /// hermetic.
    #[test]
    fn extract_remote_context_unset_returns_none() {
        let _g = env_lock();
        let prior = std::env::var_os("TRACEPARENT");
        // SAFETY: held under `env_lock` so env mutation is serialised
        // against the other env-var tests in this module.
        unsafe {
            std::env::remove_var("TRACEPARENT");
        }
        assert!(extract_remote_context().is_none());
        if let Some(v) = prior {
            unsafe {
                std::env::set_var("TRACEPARENT", v);
            }
        }
    }

    /// Round-trip a valid W3C `traceparent` value through the
    /// extractor and confirm the resulting context carries a valid
    /// `SpanContext`.
    #[test]
    fn extract_remote_context_parses_valid_traceparent() {
        use opentelemetry::trace::TraceContextExt as _;
        let _g = env_lock();
        // Format: 00-<32 hex traceid>-<16 hex spanid>-<2 hex flags>
        let valid = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let prior_tp = std::env::var_os("TRACEPARENT");
        let prior_ts = std::env::var_os("TRACESTATE");
        unsafe {
            std::env::set_var("TRACEPARENT", valid);
            std::env::remove_var("TRACESTATE");
        }
        let cx = extract_remote_context().expect("valid traceparent yields context");
        // Bind the SpanRef to a local — `cx.span()` returns a borrow
        // that must outlive `span_context()`'s usage.
        let span_ref = cx.span();
        let trace_id_str = {
            let span_cx = span_ref.span_context();
            assert!(span_cx.is_valid(), "extracted span context must be valid");
            span_cx.trace_id().to_string()
        };
        assert_eq!(trace_id_str, "0af7651916cd43dd8448eb211c80319c");
        unsafe {
            match prior_tp {
                Some(v) => std::env::set_var("TRACEPARENT", v),
                None => std::env::remove_var("TRACEPARENT"),
            }
            if let Some(v) = prior_ts {
                std::env::set_var("TRACESTATE", v);
            }
        }
    }

    /// Malformed `traceparent` should yield `None` rather than a
    /// half-built context that would silently mis-parent every span.
    #[test]
    fn extract_remote_context_rejects_malformed_traceparent() {
        let _g = env_lock();
        let prior = std::env::var_os("TRACEPARENT");
        unsafe {
            std::env::set_var("TRACEPARENT", "definitely-not-a-traceparent");
        }
        assert!(extract_remote_context().is_none());
        unsafe {
            match prior {
                Some(v) => std::env::set_var("TRACEPARENT", v),
                None => std::env::remove_var("TRACEPARENT"),
            }
        }
    }

    /// Round-trip: a span whose parent is set from a synthetic
    /// `TRACEPARENT` should land at the exporter with the same trace
    /// id as the env-var-supplied parent. Proves the propagator wiring
    /// + `OpenTelemetrySpanExt::set_parent` integration.
    #[test]
    fn span_inherits_remote_parent_trace_id() {
        let _g = env_lock();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let expected_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
        let prior = std::env::var_os("TRACEPARENT");
        unsafe {
            std::env::set_var("TRACEPARENT", valid);
        }

        let (exporter, provider, subscriber) = capturing_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("pipeline.run");
            if let Some(cx) = extract_remote_context() {
                let _ = span.set_parent(cx);
            }
            let _enter = span.enter();
        });
        provider.shutdown().expect("provider shutdown");

        let captured = exporter.0.lock().unwrap();
        let pipeline_run = captured
            .iter()
            .find(|s| s.name == "pipeline.run")
            .expect("pipeline.run span captured");
        assert_eq!(
            pipeline_run.span_context.trace_id().to_string(),
            expected_trace_id,
            "span should inherit the trace id from the env-supplied parent"
        );

        unsafe {
            match prior {
                Some(v) => std::env::set_var("TRACEPARENT", v),
                None => std::env::remove_var("TRACEPARENT"),
            }
        }
    }
}
