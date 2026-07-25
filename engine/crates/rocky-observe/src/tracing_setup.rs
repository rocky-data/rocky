//! Structured-logging initialisation for the Rocky CLI.
//!
//! [`init_tracing`] sets up the [`tracing`] subscriber registry with the
//! `fmt` layer (JSON or compact, written to stderr). When the `otel`
//! feature is enabled and `OTEL_EXPORTER_OTLP_ENDPOINT` is set, it also:
//!
//! - registers the `tracing-opentelemetry` layer so every existing
//!   `tracing::info_span!` is exported via OTLP gRPC,
//! - installs the W3C `TraceContextPropagator` so a `TRACEPARENT` env
//!   var (the convention OpenTelemetry SDKs and most orchestrators use
//!   to forward trace context to subprocesses) is honoured,
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
/// Log level controlled by `RUST_LOG` env var. The default
/// (`info,salsa=warn`) keeps Rocky's own spans at `info` while silencing the
/// chatty salsa query-cache trace.
///
/// **Hold the returned [`TracingGuard`] for the lifetime of the
/// process** — dropping it earlier flushes and shuts down the batch
/// span processor, after which subsequent spans go to the no-op
/// tracer.
#[must_use]
pub fn init_tracing(json: bool) -> TracingGuard {
    // Default to `info` for Rocky's own spans (so `rocky trace` / replay keep
    // capturing `pipeline.run`, `materialize.table`, ...) but silence the
    // chatty `salsa::function::execute` query-cache trace that would otherwise
    // dump on every bare `rocky compile`. `RUST_LOG` overrides this entirely.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,salsa=warn"));

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

    // JSONL trace layer (Arc 4 span retention) — file-per-process under
    // `.rocky/traces/{ts}-{pid}.jsonl`. Skipped for `rocky lsp` (long-
    // lived daemon — would leak files into every LSP session) and when
    // the operator opts out via `ROCKY_TRACE_DISABLE=1`.
    let trace_layer = if jsonl_layer_disabled() {
        None
    } else {
        let mw = crate::traces::JsonlMakeWriter::new(chrono::Utc::now());
        Some(
            fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(false)
                .with_span_list(true)
                .with_current_span(false)
                .with_writer(mw)
                .boxed(),
        )
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
                        .with(trace_layer)
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
                        .with(trace_layer)
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
        .with(trace_layer)
        .init();
    TracingGuard::new_disabled()
}

/// Returns `true` when the JSONL trace layer should be skipped.
///
/// Two gates: (D2) `rocky lsp` runs as a daemon and never resets between
/// editor sessions, so persistent JSONL would grow unbounded; we detect
/// it by peeking the first positional CLI argument. (D6) the
/// `ROCKY_TRACE_DISABLE` env var is the explicit escape hatch for
/// shared-filesystem users or anyone who's having a bad day with disk
/// quota.
fn jsonl_layer_disabled() -> bool {
    if std::env::var_os(crate::traces::TRACE_DISABLE_ENV).is_some() {
        return true;
    }
    matches!(std::env::args().nth(1).as_deref(), Some("lsp"))
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
    // Install the W3C TraceContext propagator globally so any code reaching
    // for `opentelemetry::global::get_text_map_propagator` (an HTTP client
    // injecting headers, for instance) finds a real one rather than the
    // no-op default. Rocky's own env-var carriers — `extract_remote_context`
    // and `current_trace_context` — deliberately build their own propagator
    // instead, so they behave identically whether or not OTLP export is
    // configured for this process.
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
    if let Ok(tracestate) = std::env::var("TRACESTATE")
        && !tracestate.is_empty()
    {
        carrier.insert("tracestate".to_string(), tracestate);
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

/// A W3C trace context describing a span, ready to hand to a child process.
///
/// Both fields travel together on purpose: a `tracestate` is scoped to the
/// trace its `traceparent` names, so propagating one without the other — or
/// pairing one with a different trace — is a spec violation that can carry a
/// foreign vendor sampling key into the wrong trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// The `traceparent` value. Always present: it is the reason this exists.
    pub traceparent: String,
    /// The `tracestate` value, when the described span carries vendor state.
    pub tracestate: Option<String>,
}

/// The W3C trace context for the span that is currently active, if there is
/// one worth describing.
///
/// This is the producer half of [`extract_remote_context`]: a parent process
/// calls it to describe its own span, hands the value to a child (Rocky's
/// scheduler stamps it on the child's `TRACEPARENT`/`TRACESTATE`), and the
/// child adopts it via [`adopt_remote_parent`].
///
/// The sampled flag is whatever the active span's trace flags say — it is
/// injected by the propagator, never hard-coded — so a child honours the
/// upstream sampling decision through its `ParentBased` sampler.
///
/// Returns `None` when there is nothing valid to propagate: no span is active,
/// the OTel layer is not registered (no `OTEL_EXPORTER_OTLP_ENDPOINT`), or the
/// `otel` feature is off. `None` is the honest answer — an all-zero trace id
/// is invalid per the W3C spec, and handing one to a child would either be
/// rejected or, worse, silently accepted as a parent that does not exist.
#[cfg(feature = "otel")]
#[must_use]
pub fn current_trace_context() -> Option<TraceContext> {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry::trace::TraceContextExt;
    use std::collections::HashMap;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let cx = tracing::Span::current().context();
    // Without the OTel layer registered, `context()` yields a context with no
    // recording span, whose `SpanContext` is the all-zero invalid one.
    if !cx.span().span_context().is_valid() {
        return None;
    }

    // Build the propagator locally rather than reading the global one: the
    // global is only installed when OTLP export is configured, and its no-op
    // default would inject nothing at all — a silent hole exactly where the
    // trace is supposed to connect.
    let mut carrier = HashMap::new();
    opentelemetry_sdk::propagation::TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    let traceparent = carrier.remove("traceparent").filter(|tp| !tp.is_empty())?;
    Some(TraceContext {
        // The propagator only writes this key when the span carries vendor
        // state, so `None` here means "no tracestate", never "dropped one".
        tracestate: carrier.remove("tracestate").filter(|ts| !ts.is_empty()),
        traceparent,
    })
}

/// Compatibility no-op for builds without the `otel` feature.
#[cfg(not(feature = "otel"))]
#[must_use]
pub fn current_trace_context() -> Option<TraceContext> {
    None
}

/// Keeps an adopted upstream trace context installed. Dropping it restores
/// whatever context was current before.
///
/// The non-`otel` build is a zero-size type, so callers bind the guard without
/// any `cfg` attribute of their own.
pub struct RemoteParentGuard {
    #[cfg(feature = "otel")]
    _attached: Option<opentelemetry::ContextGuard>,
}

impl RemoteParentGuard {
    /// A guard that adopted nothing.
    fn inert() -> Self {
        Self {
            #[cfg(feature = "otel")]
            _attached: None,
        }
    }
}

/// Adopts the upstream `TRACEPARENT` (via [`extract_remote_context`]) as the
/// parent of the root spans this process opens, for as long as the returned
/// guard is held.
///
/// Call it once, early in `main`, and hold the guard for the process. With it
/// held, `rocky run`'s root `run` span (and every other root span opened on
/// this thread) becomes a child of the upstream span, so a scheduler-spawned
/// run shows up inside the tick's trace instead of standing alone. Spans that
/// already have an in-process parent are untouched — nesting is preserved, not
/// flattened.
///
/// Returns an inert guard when `TRACEPARENT` is unset, malformed, or the
/// `otel` feature is off. A bad value is ignored: the process self-roots its
/// traces exactly as before, and never fails because of an env var.
///
/// # Why a context guard rather than `set_parent`
///
/// `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent` only works before
/// a span is entered — once entered, its `SpanBuilder` is consumed and
/// `set_parent` returns `SetParentError::AlreadyStarted` and changes nothing.
/// Every root span worth re-parenting here is opened by
/// `#[tracing::instrument]`, which enters the span before the function body
/// runs, so a `set_parent` call from inside that body would compile, look
/// wired, and do nothing. Installing the context instead is applied by the
/// layer when each root span is *created*, which is the only point at which
/// its parent is still open to change.
///
/// # Scope
///
/// The context is installed thread-locally, so it reaches spans created on the
/// thread holding the guard — for the CLI, everything the top-level future
/// drives. Spans opened on a separately spawned task (the resident scheduler's
/// own loop, the replication fan-out's per-table work) root themselves as
/// before. Note that this is about *in-process* spans: a subprocess Rocky
/// launches inherits this process's environment, so it adopts the same
/// upstream context through `TRACEPARENT` — that is how `rocky serve`'s
/// HTTP-submitted jobs, which do their work in a child `rocky run`, end up in
/// the server's launch-time trace.
///
/// The adoption is logged at `debug`, with the trace id and the sampled flag,
/// so "my spans vanished" (an upstream that sampled the trace out) is
/// answerable from the logs rather than by inspection.
#[cfg(feature = "otel")]
#[must_use]
pub fn adopt_remote_parent() -> RemoteParentGuard {
    use opentelemetry::trace::TraceContextExt;

    let Some(cx) = extract_remote_context() else {
        return RemoteParentGuard::inert();
    };
    {
        // Bind the `SpanRef` so the borrow outlives the field reads.
        let span = cx.span();
        let span_cx = span.span_context();
        tracing::debug!(
            trace_id = %span_cx.trace_id(),
            parent_span_id = %span_cx.span_id(),
            sampled = span_cx.is_sampled(),
            "adopted an upstream trace context from TRACEPARENT; \
             this process's root spans join that trace"
        );
    }
    RemoteParentGuard {
        _attached: Some(cx.attach()),
    }
}

/// Compatibility no-op for builds without the `otel` feature.
#[cfg(not(feature = "otel"))]
#[must_use]
pub fn adopt_remote_parent() -> RemoteParentGuard {
    RemoteParentGuard::inert()
}

/// In-memory span capture for tests that assert on the trace itself.
///
/// Test-only: gated on `test-support`, so no release build ever links it.
#[cfg(all(feature = "otel", feature = "test-support"))]
pub mod testing {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    /// One exported span, reduced to the identity fields a trace-shape
    /// assertion needs.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CapturedSpan {
        /// The span's name (`run`, `scheduler.tick`, ...).
        pub name: String,
        /// Lower-hex trace id — the value that must match across a connected
        /// trace.
        pub trace_id: String,
        /// Lower-hex span id.
        pub span_id: String,
        /// Lower-hex parent span id; all-zero for a root span.
        pub parent_span_id: String,
    }

    /// An in-memory tracer pipeline plus the subscriber bound to it.
    ///
    /// Spans are exported as they close (a simple processor, not a batch one),
    /// so [`TraceHarness::spans`] can be read straight after the work under
    /// test finishes — no shutdown dance, and no dependence on a background
    /// batch thread having caught up.
    pub struct TraceHarness {
        provider: SdkTracerProvider,
        exporter: InMemorySpanExporter,
    }

    impl Default for TraceHarness {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TraceHarness {
        /// Stand up the in-memory pipeline.
        #[must_use]
        pub fn new() -> Self {
            let exporter = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exporter.clone())
                .build();
            Self { provider, exporter }
        }

        /// A subscriber that routes every span into this harness. Install it
        /// with `tracing::subscriber::set_default` (thread-local) so parallel
        /// tests don't fight over the global default.
        #[must_use]
        pub fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + 'static + use<> {
            use tracing_subscriber::layer::SubscriberExt as _;
            let tracer = self.provider.tracer("rocky_test");
            let layer = tracing_opentelemetry::layer().with_tracer(tracer);
            tracing_subscriber::registry().with(layer)
        }

        /// Every span exported so far, oldest first.
        #[must_use]
        pub fn spans(&self) -> Vec<CapturedSpan> {
            let _ = self.provider.force_flush();
            self.exporter
                .get_finished_spans()
                .unwrap_or_default()
                .into_iter()
                .map(|s| CapturedSpan {
                    name: s.name.to_string(),
                    trace_id: s.span_context.trace_id().to_string(),
                    span_id: s.span_context.span_id().to_string(),
                    parent_span_id: s.parent_span_id.to_string(),
                })
                .collect()
        }

        /// The single exported span with `name`, or `None` when there is not
        /// exactly one.
        #[must_use]
        pub fn only_span(&self, name: &str) -> Option<CapturedSpan> {
            let mut matching = self.spans().into_iter().filter(|s| s.name == name);
            let first = matching.next()?;
            matching.next().is_none().then_some(first)
        }
    }
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
    /// `materialize.table` / `discover_sources` / `pipeline.run` etc.
    /// silently drop on the floor.
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

    // -----------------------------------------------------------------
    // Connected `scheduler.tick → run` trace: producer + consumer
    // -----------------------------------------------------------------

    const ROOT_PARENT: &str = "0000000000000000";

    /// Sets (or clears) the `TRACEPARENT`/`TRACESTATE` pair for as long as it
    /// is held, restoring the prior values on drop. Holds [`env_lock`]
    /// throughout, so the mutation is serialised against every other
    /// env-touching test in this module.
    struct TraceparentEnv {
        _lock: MutexGuard<'static, ()>,
        prior_traceparent: Option<std::ffi::OsString>,
        prior_tracestate: Option<std::ffi::OsString>,
    }

    impl TraceparentEnv {
        fn set(traceparent: Option<&str>) -> Self {
            Self::set_pair(traceparent, None)
        }

        fn set_pair(traceparent: Option<&str>, tracestate: Option<&str>) -> Self {
            let _lock = env_lock();
            let prior_traceparent = std::env::var_os("TRACEPARENT");
            let prior_tracestate = std::env::var_os("TRACESTATE");
            // SAFETY: held under `env_lock`, so this process mutates the trace
            // context variables from one thread at a time.
            unsafe {
                set_or_clear("TRACEPARENT", traceparent);
                set_or_clear("TRACESTATE", tracestate);
            }
            Self {
                _lock,
                prior_traceparent,
                prior_tracestate,
            }
        }
    }

    /// # Safety
    ///
    /// Callers must hold [`env_lock`].
    unsafe fn set_or_clear(key: &str, value: Option<&str>) {
        unsafe {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    impl Drop for TraceparentEnv {
        fn drop(&mut self) {
            // SAFETY: `env_lock` is still held by `_lock` (dropped after this).
            unsafe {
                set_or_clear(
                    "TRACEPARENT",
                    self.prior_traceparent.take().as_deref().map(|v| {
                        v.to_str()
                            .expect("a prior TRACEPARENT this process set is UTF-8")
                    }),
                );
                set_or_clear(
                    "TRACESTATE",
                    self.prior_tracestate.take().as_deref().map(|v| {
                        v.to_str()
                            .expect("a prior TRACESTATE this process set is UTF-8")
                    }),
                );
            }
        }
    }

    /// A tracer whose sampler matches production (`build_otel_tracer`), so the
    /// tests see the real consequence of an upstream sampling decision.
    fn parent_based_subscriber() -> (
        CapturingExporter,
        SdkTracerProvider,
        impl tracing::Subscriber,
    ) {
        use opentelemetry_sdk::trace::Sampler;

        let exporter = CapturingExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter.clone())
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
            .build();
        let tracer = provider.tracer("rocky_test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(layer);
        (exporter, provider, subscriber)
    }

    /// Stands in for `commands::run::run`: `#[tracing::instrument]` opens AND
    /// enters the span before the body runs, which is exactly why the parent
    /// has to be installed from outside.
    #[tracing::instrument(skip_all, name = "run")]
    async fn instrumented_run() {
        tracing::info!("run body");
    }

    #[tracing::instrument(skip_all, name = "outer")]
    async fn instrumented_outer() {
        instrumented_inner().await;
    }

    #[tracing::instrument(skip_all, name = "inner")]
    async fn instrumented_inner() {
        tracing::info!("inner body");
    }

    /// Open the tick span and describe it from inside, exactly as the scheduler
    /// does from within its instrumented tick.
    fn tick_trace_context() -> TraceContext {
        let span = info_span!("scheduler.tick");
        let _enter = span.enter();
        current_trace_context().expect("an active span yields a trace context")
    }

    /// Run `f` under a capturing subscriber, with whatever `TRACEPARENT` is
    /// currently set adopted for its duration, and return the exported spans.
    fn run_under_adopted_parent(f: impl FnOnce()) -> Vec<SpanData> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (exporter, provider, subscriber) = parent_based_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let _remote_parent = adopt_remote_parent();
            f();
        });
        provider.shutdown().expect("provider shutdown");
        let captured = exporter.0.lock().unwrap();
        captured.clone()
    }

    fn find<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
        spans
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no `{name}` span exported"))
    }

    /// The producer half: inside an active span, `current_trace_context` yields
    /// a syntactically valid W3C value carrying that span's ids. Without it the
    /// scheduler has nothing to stamp on a child's `TRACEPARENT`.
    #[test]
    fn current_trace_context_describes_the_active_span() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (exporter, provider, subscriber) = parent_based_subscriber();
        let cx = tracing::subscriber::with_default(subscriber, tick_trace_context);
        provider.shutdown().expect("provider shutdown");
        assert_eq!(
            cx.tracestate, None,
            "a span with no vendor state must report none, not an empty string"
        );

        let traceparent = cx.traceparent.as_str();
        let parts: Vec<&str> = traceparent.split('-').collect();
        assert_eq!(
            parts.len(),
            4,
            "W3C traceparent has 4 fields: {traceparent}"
        );
        assert_eq!(parts[0], "00", "version must be 00: {traceparent}");
        assert_eq!(parts[1].len(), 32, "trace id is 32 hex: {traceparent}");
        assert_eq!(parts[2].len(), 16, "span id is 16 hex: {traceparent}");
        assert!(
            parts
                .iter()
                .all(|p| p.chars().all(|c| c.is_ascii_hexdigit())),
            "every field is lower-hex: {traceparent}"
        );
        assert_ne!(
            parts[1], "00000000000000000000000000000000",
            "an all-zero trace id is invalid and must never be emitted"
        );
        assert_eq!(
            parts[3], "01",
            "the tick is sampled, so the flags must say so: {traceparent}"
        );

        let captured = exporter.0.lock().unwrap();
        let tick = find(&captured, "scheduler.tick");
        assert_eq!(
            parts[1],
            tick.span_context.trace_id().to_string(),
            "the traceparent must carry the tick's own trace id"
        );
        assert_eq!(
            parts[2],
            tick.span_context.span_id().to_string(),
            "the traceparent must name the tick span as the parent"
        );
    }

    /// With no OTel layer registered (the overwhelmingly common case — no
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`), there is no real span context, and the
    /// producer must say so rather than emit an all-zero traceparent.
    #[test]
    fn current_trace_context_is_none_without_an_otel_layer() {
        let subscriber = tracing_subscriber::registry();
        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!("scheduler.tick");
            let _enter = span.enter();
            assert!(current_trace_context().is_none());
        });
    }

    /// Outside any span there is nothing to describe.
    #[test]
    fn current_trace_context_is_none_outside_any_span() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();
        let (_exporter, provider, subscriber) = parent_based_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            assert!(current_trace_context().is_none());
        });
        provider.shutdown().expect("provider shutdown");
    }

    /// The consumer half: with `TRACEPARENT` set, an `#[instrument]`ed root
    /// span joins the upstream trace as a child of the upstream span.
    #[test]
    fn adopt_remote_parent_reparents_an_instrumented_root_span() {
        let _env = TraceparentEnv::set(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));

        let spans = run_under_adopted_parent(|| {
            futures_block_on(instrumented_run());
        });

        let run = find(&spans, "run");
        assert_eq!(
            run.span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "the run must join the upstream trace"
        );
        assert_eq!(
            run.parent_span_id.to_string(),
            "00f067aa0ba902b7",
            "the run must hang off the upstream span, not float in its trace"
        );
    }

    /// Adopting an upstream parent must not flatten the in-process tree: a
    /// nested span keeps its real parent, and only the root re-parents.
    #[test]
    fn adopt_remote_parent_preserves_in_process_nesting() {
        let _env = TraceparentEnv::set(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));

        let spans = run_under_adopted_parent(|| {
            futures_block_on(instrumented_outer());
        });

        let outer = find(&spans, "outer");
        let inner = find(&spans, "inner");
        assert_eq!(
            outer.parent_span_id.to_string(),
            "00f067aa0ba902b7",
            "the root adopts the upstream parent"
        );
        assert_eq!(
            inner.parent_span_id.to_string(),
            outer.span_context.span_id().to_string(),
            "a nested span keeps its in-process parent"
        );
        assert_eq!(
            inner.span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "the whole subtree lands in the upstream trace"
        );
    }

    /// A hostile or corrupted `TRACEPARENT` must never fail a run: it is
    /// ignored and the process self-roots exactly as it did before.
    #[test]
    fn adopt_remote_parent_ignores_a_malformed_traceparent() {
        for garbage in [
            "definitely-not-a-traceparent",
            "",
            "00-00000000000000000000000000000000-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "zz-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n injected",
        ] {
            let _env = TraceparentEnv::set(Some(garbage));
            let spans = run_under_adopted_parent(|| {
                futures_block_on(instrumented_run());
            });
            let run = find(&spans, "run");
            assert_eq!(
                run.parent_span_id.to_string(),
                ROOT_PARENT,
                "a malformed traceparent ({garbage:?}) must leave the run self-rooted"
            );
        }
    }

    /// No `TRACEPARENT` at all: unchanged behaviour, a self-rooted run.
    #[test]
    fn adopt_remote_parent_without_the_env_var_self_roots() {
        let _env = TraceparentEnv::set(None);
        let spans = run_under_adopted_parent(|| {
            futures_block_on(instrumented_run());
        });
        let run = find(&spans, "run");
        assert_eq!(run.parent_span_id.to_string(), ROOT_PARENT);
    }

    /// End-to-end at the library seam: what the producer emits is exactly what
    /// re-parents the consumer. This is the `scheduler.tick → run` join with
    /// the process boundary (an env var) standing in for the child.
    #[test]
    fn a_producer_traceparent_reparents_the_consumer_root_span() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        // Parent process: open the tick span and describe it.
        let (tick_exporter, tick_provider, tick_subscriber) = parent_based_subscriber();
        let traceparent =
            tracing::subscriber::with_default(tick_subscriber, || tick_trace_context().traceparent);
        tick_provider.shutdown().expect("provider shutdown");
        let (tick_trace_id, tick_span_id) = {
            let captured = tick_exporter.0.lock().unwrap();
            let tick = find(&captured, "scheduler.tick");
            (
                tick.span_context.trace_id().to_string(),
                tick.span_context.span_id().to_string(),
            )
        };

        // Child process: the value arrives as `TRACEPARENT` and is adopted.
        let _env = TraceparentEnv::set(Some(&traceparent));
        let spans = run_under_adopted_parent(|| {
            futures_block_on(instrumented_run());
        });

        let run = find(&spans, "run");
        assert_eq!(
            run.span_context.trace_id().to_string(),
            tick_trace_id,
            "the run must land in the tick's trace"
        );
        assert_eq!(
            run.parent_span_id.to_string(),
            tick_span_id,
            "the run must be a child of the tick span"
        );
    }

    /// An upstream that sampled the trace OUT suppresses the child's spans —
    /// `ParentBased(AlwaysOn)` honours the caller's decision. That is correct
    /// OTel behaviour and worth pinning: the work still runs, only the export
    /// stops, so it must never look like a failure.
    #[test]
    fn an_unsampled_remote_parent_suppresses_export_without_failing() {
        let _env = TraceparentEnv::set(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
        ));
        let spans = run_under_adopted_parent(|| {
            futures_block_on(instrumented_run());
        });
        assert!(
            spans.iter().all(|s| s.name != "run"),
            "an unsampled upstream parent drops the child's spans, by design"
        );
    }

    /// Vendor state must survive the round trip, not be silently dropped: a
    /// span that inherited a `tracestate` from upstream describes itself WITH
    /// it, so the spawner has a real pair to stamp. Without this the
    /// `Some(tracestate)` arm at the env boundary would be unreachable and the
    /// "we propagate the pair" claim would be decoration.
    #[test]
    fn a_described_span_carries_inherited_vendor_state() {
        let _env = TraceparentEnv::set_pair(
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            Some("vendor=abc,other=1"),
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (_exporter, provider, subscriber) = parent_based_subscriber();
        let cx = tracing::subscriber::with_default(subscriber, || {
            let _remote_parent = adopt_remote_parent();
            tick_trace_context()
        });
        provider.shutdown().expect("provider shutdown");

        assert!(
            cx.traceparent.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
            "the described span is in the upstream trace: {}",
            cx.traceparent
        );
        assert_eq!(
            cx.tracestate.as_deref(),
            Some("vendor=abc,other=1"),
            "vendor state inherited from upstream must be propagated onward, \
             not dropped — otherwise the pair we stamp is incomplete"
        );
    }

    /// The adopted context is thread-local, so a span opened on another thread
    /// self-roots even while the guard is held. Both the docs' "in-process
    /// background work opens its own trace" claim and the "a not-sampled
    /// upstream only quietens a trace" caveat rest on this, and every other
    /// test here runs single-threaded — so it is pinned explicitly.
    #[test]
    fn a_span_opened_on_another_thread_self_roots() {
        let _env = TraceparentEnv::set(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (exporter, provider, subscriber) = parent_based_subscriber();
        // A second tracer on the SAME provider, so the other thread's spans
        // reach the same exporter.
        let other_tracer = provider.tracer("rocky_test");

        tracing::subscriber::with_default(subscriber, || {
            let _remote_parent = adopt_remote_parent();
            futures_block_on(instrumented_run());

            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let layer = tracing_opentelemetry::layer().with_tracer(other_tracer);
                    let subscriber = tracing_subscriber::registry().with(layer);
                    // The guard above was attached on the parent thread only.
                    tracing::subscriber::with_default(subscriber, || {
                        let span = info_span!("off_thread");
                        let _enter = span.enter();
                    });
                });
            });
        });
        provider.shutdown().expect("provider shutdown");

        let spans = exporter.0.lock().unwrap();
        assert_eq!(
            find(&spans, "run").parent_span_id.to_string(),
            "00f067aa0ba902b7",
            "the guarded thread's root adopts the upstream parent"
        );
        assert_eq!(
            find(&spans, "off_thread").parent_span_id.to_string(),
            ROOT_PARENT,
            "another thread does not see the adopted context, so it self-roots"
        );
    }

    /// The reason [`adopt_remote_parent`] installs a context instead of calling
    /// `set_parent` on the root span: once a span has been entered, its builder
    /// is gone and `set_parent` is rejected. If a dependency bump ever makes
    /// this succeed, this test fails and the simpler wiring becomes available —
    /// until then, a `set_parent` inside an instrumented body would compile,
    /// look wired, and silently do nothing.
    #[test]
    fn set_parent_after_span_entry_is_rejected() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let (_exporter, provider, subscriber) = parent_based_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let mut carrier = std::collections::HashMap::new();
            carrier.insert(
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            );
            use opentelemetry::propagation::TextMapPropagator;
            let cx =
                opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);

            let span = info_span!("run");
            let before_entry = span.set_parent(cx.clone());
            assert!(
                before_entry.is_ok(),
                "before entry, set_parent is the supported path"
            );

            let _enter = span.enter();
            let after_entry = tracing::Span::current().set_parent(cx);
            assert!(
                after_entry.is_err(),
                "after entry the span builder is consumed, so set_parent cannot re-parent"
            );
        });
        provider.shutdown().expect("provider shutdown");
    }

    /// Drive a future to completion on the current thread, so the span under
    /// test is created on the thread holding the adopted context — the same
    /// relationship `main`'s `block_on` has with the CLI's root spans.
    fn futures_block_on<F: std::future::Future>(fut: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(fut)
    }
}
