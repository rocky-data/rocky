//! In-process event bus for pipeline lifecycle events.
//!
//! Provides fan-out pub/sub using [`tokio::sync::broadcast`]. Subscribers that
//! fall behind will miss events — this is observability, not a message queue.
//!
//! The [`PipelineEvent`] struct is self-contained (string-based event types) so
//! that `rocky-observe` does not depend on `rocky-core`, avoiding circular
//! dependencies.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, trace};

// ---------------------------------------------------------------------------
// ErrorClass
// ---------------------------------------------------------------------------

/// Classification of an error carried on a [`PipelineEvent`]. Lets
/// subscribers (logging, metrics, Dagster integration) distinguish retryable
/// flakiness from terminal failure without string-matching the free-form
/// `error` field.
///
/// Adapters classify their own errors (each adapter has its own error enum)
/// and stamp the event before emitting so consumers see a stable taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Retryable network-level error (connection reset, 5xx, DNS flake).
    Transient,
    /// Non-retryable — invalid SQL, schema mismatch, bad arguments.
    Permanent,
    /// Deadline expiry (per-request or global cap).
    Timeout,
    /// Auth failure — 401, expired / rotated token, bad credentials.
    Auth,
    /// Config / input validation rejected by the server before execution.
    Config,
    /// 429 or adapter-specific rate-limit signal.
    RateLimit,
}

// ---------------------------------------------------------------------------
// PipelineEvent
// ---------------------------------------------------------------------------

/// A pipeline lifecycle event.
///
/// Designed to carry enough context for any subscriber (logging, webhooks,
/// metrics collection) without coupling to specific hook or core types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEvent {
    /// Event type name (e.g., `"pipeline_start"`, `"after_materialize"`).
    pub event_type: String,
    /// Timestamp when the event was emitted.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Associated run ID (if within a run).
    pub run_id: Option<String>,
    /// Table or model name (if table/model-scoped).
    pub target: Option<String>,
    /// Duration in milliseconds (for completion events).
    pub duration_ms: Option<u64>,
    /// Error message (for error events).
    pub error: Option<String>,
    /// Current retry attempt number (1-based). Paired with `max_attempts`
    /// on retry events so subscribers can distinguish "retry 2/5" from
    /// "final failure". None on non-retry events.
    #[serde(default)]
    pub attempt: Option<u32>,
    /// Maximum attempts allowed by the adapter's retry policy. See `attempt`.
    #[serde(default)]
    pub max_attempts: Option<u32>,
    /// Structured classification of the error on this event — see
    /// [`ErrorClass`]. None when no error, or when the emitter hasn't
    /// classified the failure.
    #[serde(default)]
    pub error_class: Option<ErrorClass>,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, serde_json::Value>,
}

impl PipelineEvent {
    /// Create a new event with just the type and current timestamp.
    pub fn new(event_type: impl Into<String>) -> Self {
        PipelineEvent {
            event_type: event_type.into(),
            timestamp: chrono::Utc::now(),
            run_id: None,
            target: None,
            duration_ms: None,
            error: None,
            attempt: None,
            max_attempts: None,
            error_class: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the run ID.
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Set the target (table or model name).
    #[must_use]
    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    /// Set the duration in milliseconds.
    #[must_use]
    pub fn with_duration_ms(mut self, ms: u64) -> Self {
        self.duration_ms = Some(ms);
        self
    }

    /// Set the error message.
    #[must_use]
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }

    /// Set the current attempt / max-attempts pair. Emitted by adapter retry
    /// loops so subscribers can tell "retry N of M" from "final failure".
    #[must_use]
    pub fn with_attempt(mut self, attempt: u32, max_attempts: u32) -> Self {
        self.attempt = Some(attempt);
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Set the [`ErrorClass`] classification for this event.
    #[must_use]
    pub fn with_error_class(mut self, class: ErrorClass) -> Self {
        self.error_class = Some(class);
        self
    }

    /// Insert a key-value pair into the metadata map.
    #[must_use]
    pub fn with_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// In-process event bus for pipeline lifecycle events.
///
/// Uses [`tokio::sync::broadcast`] for fan-out to multiple subscribers.
/// Subscribers that fall behind will miss events (lossy by design —
/// this is observability, not a message queue).
pub struct EventBus {
    sender: broadcast::Sender<PipelineEvent>,
}

impl EventBus {
    /// Creates a new `EventBus` with the given channel capacity.
    ///
    /// A capacity of 256 is suitable for most pipeline runs.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        EventBus { sender }
    }

    /// Emit an event to all subscribers.
    ///
    /// If no subscribers exist, the event is dropped — `tokio::sync::broadcast::Sender::send`
    /// only errors when there are zero active receivers, which is expected
    /// for an event bus with no current listeners. The `debug!` above gives
    /// operators visibility when that happens.
    pub fn emit(&self, event: PipelineEvent) {
        debug!(event_type = %event.event_type, target = ?event.target, "event emitted");
        if self.sender.send(event).is_err() {
            trace!("no subscribers; event dropped");
        }
    }

    /// Subscribe to events. Returns a receiver that yields events.
    pub fn subscribe(&self) -> broadcast::Receiver<PipelineEvent> {
        self.sender.subscribe()
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(256)
    }
}

// ---------------------------------------------------------------------------
// Global instance
// ---------------------------------------------------------------------------

static GLOBAL_EVENT_BUS: OnceLock<EventBus> = OnceLock::new();

/// Returns the global event bus, initializing it on first access.
pub fn global_event_bus() -> &'static EventBus {
    GLOBAL_EVENT_BUS.get_or_init(EventBus::default)
}

// ---------------------------------------------------------------------------
// OTel span-event bridge
// ---------------------------------------------------------------------------

/// Annotate the current `tracing::Span` with a span event derived from the
/// given [`PipelineEvent`].
///
/// When the `otel` feature is enabled and a `tracing-opentelemetry` layer
/// is registered (see `rocky_observe::tracing_setup::init_tracing`), this
/// translates the event's structured fields (`event_type` becomes the
/// span event name; `error`, `target`, `attempt`/`max_attempts`, and
/// `error_class` become attributes) into an OTLP span event on
/// `Span::current()`. Without the layer or feature, this is a no-op so
/// callers don't need to gate the call.
///
/// Use this at every `EventBus::emit` site where the event represents a
/// point-in-time annotation (retry fired, breaker tripped, budget
/// breached). Span boundaries (`pipeline_start` / `pipeline_complete`)
/// stay as bus events and are *not* converted — the per-run lifecycle
/// span is the OTel-native shape for those.
pub fn record_span_event(event: &PipelineEvent) {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::KeyValue;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let mut attrs: Vec<KeyValue> = Vec::with_capacity(4 + event.metadata.len());
        if let Some(target) = &event.target {
            attrs.push(KeyValue::new("rocky.event.target", target.clone()));
        }
        if let Some(err) = &event.error {
            attrs.push(KeyValue::new("rocky.event.error", err.clone()));
        }
        if let (Some(a), Some(m)) = (event.attempt, event.max_attempts) {
            attrs.push(KeyValue::new("rocky.event.attempt", i64::from(a)));
            attrs.push(KeyValue::new("rocky.event.max_attempts", i64::from(m)));
        }
        if let Some(class) = event.error_class {
            attrs.push(KeyValue::new(
                "rocky.event.error_class",
                error_class_to_str(class).to_string(),
            ));
        }
        // Numeric / string metadata is forwarded too; complex values get
        // their JSON serialisation as a string so dashboards can pivot
        // without the producer learning the OTel attribute taxonomy.
        for (k, v) in &event.metadata {
            let key = format!("rocky.event.metadata.{k}");
            match v {
                serde_json::Value::String(s) => attrs.push(KeyValue::new(key, s.clone())),
                serde_json::Value::Bool(b) => attrs.push(KeyValue::new(key, *b)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        attrs.push(KeyValue::new(key, i));
                    } else if let Some(f) = n.as_f64() {
                        attrs.push(KeyValue::new(key, f));
                    } else {
                        attrs.push(KeyValue::new(key, n.to_string()));
                    }
                }
                _ => attrs.push(KeyValue::new(key, v.to_string())),
            }
        }

        // `Cow<'static, str>` requires the name to be 'static, but
        // `PipelineEvent::event_type` is a `String`. The underlying OTel
        // API takes `Into<Cow<...>>`; `String` works directly because
        // `Cow<'static, str>` accepts owned `String` via `From`.
        tracing::Span::current().add_event(event.event_type.clone(), attrs);
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = event;
    }
}

/// Mark the current `tracing::Span` as `Error` with the supplied
/// description. Used at retry-loop terminal-failure sites so the active
/// adapter span (and, by inheritance, the parent `materialize.table`
/// span) carries the right OTel status.
///
/// No-op when the `otel` feature is disabled.
pub fn set_current_span_error(message: impl Into<String>) {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::Status;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        tracing::Span::current().set_status(Status::error(message.into()));
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = message.into();
    }
}

#[cfg(feature = "otel")]
fn error_class_to_str(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Transient => "transient",
        ErrorClass::Permanent => "permanent",
        ErrorClass::Timeout => "timeout",
        ErrorClass::Auth => "auth",
        ErrorClass::Config => "config",
        ErrorClass::RateLimit => "rate_limit",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_emit_and_receive() {
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();

        let event = PipelineEvent::new("pipeline_start").with_run_id("run-1");
        bus.emit(event);

        let received = rx.recv().await.expect("should receive event");
        assert_eq!(received.event_type, "pipeline_start");
        assert_eq!(received.run_id.as_deref(), Some("run-1"));
    }

    #[tokio::test]
    async fn test_no_subscribers_doesnt_panic() {
        let bus = EventBus::new(16);
        // No subscribers — should not panic
        bus.emit(PipelineEvent::new("pipeline_start"));
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new(16);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);

        bus.emit(PipelineEvent::new("after_materialize").with_target("catalog.schema.table"));

        let e1 = rx1.recv().await.expect("subscriber 1 should receive");
        let e2 = rx2.recv().await.expect("subscriber 2 should receive");

        assert_eq!(e1.event_type, "after_materialize");
        assert_eq!(e1.target.as_deref(), Some("catalog.schema.table"));
        assert_eq!(e2.event_type, "after_materialize");
        assert_eq!(e2.target.as_deref(), Some("catalog.schema.table"));
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let event = PipelineEvent::new("materialize_error")
            .with_run_id("run-42")
            .with_target("warehouse.staging.orders")
            .with_duration_ms(1500)
            .with_error("timeout after 30s")
            .with_metadata("retries", serde_json::json!(3))
            .with_metadata("strategy", serde_json::json!("incremental"));

        assert_eq!(event.event_type, "materialize_error");
        assert_eq!(event.run_id.as_deref(), Some("run-42"));
        assert_eq!(event.target.as_deref(), Some("warehouse.staging.orders"));
        assert_eq!(event.duration_ms, Some(1500));
        assert_eq!(event.error.as_deref(), Some("timeout after 30s"));
        assert_eq!(event.metadata.get("retries"), Some(&serde_json::json!(3)));
        assert_eq!(
            event.metadata.get("strategy"),
            Some(&serde_json::json!("incremental"))
        );
    }

    #[tokio::test]
    async fn test_event_serialization() {
        let event = PipelineEvent::new("pipeline_complete")
            .with_run_id("run-99")
            .with_duration_ms(45000)
            .with_metadata("tables_processed", serde_json::json!(42));

        let json = serde_json::to_string(&event).expect("serialize");
        let deserialized: PipelineEvent = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.event_type, "pipeline_complete");
        assert_eq!(deserialized.run_id.as_deref(), Some("run-99"));
        assert_eq!(deserialized.duration_ms, Some(45000));
        assert_eq!(
            deserialized.metadata.get("tables_processed"),
            Some(&serde_json::json!(42))
        );
        // Timestamp should survive round-trip
        assert_eq!(deserialized.timestamp, event.timestamp);
    }

    #[tokio::test]
    async fn test_default_bus() {
        let bus = EventBus::default();
        let mut rx = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 1);

        bus.emit(PipelineEvent::new("state_synced"));

        let received = rx.recv().await.expect("should receive on default bus");
        assert_eq!(received.event_type, "state_synced");
    }

    // -- P2.8: retry attempt + error class --

    #[test]
    fn test_with_attempt_sets_both_attempt_and_max() {
        let event = PipelineEvent::new("materialize_retry").with_attempt(2, 5);
        assert_eq!(event.attempt, Some(2));
        assert_eq!(event.max_attempts, Some(5));
    }

    #[test]
    fn test_with_error_class_sets_classification() {
        let event = PipelineEvent::new("materialize_error")
            .with_error("connection reset")
            .with_error_class(ErrorClass::Transient);
        assert_eq!(event.error_class, Some(ErrorClass::Transient));
        assert_eq!(event.error.as_deref(), Some("connection reset"));
    }

    #[test]
    fn test_error_class_serializes_snake_case() {
        for (class, expected) in [
            (ErrorClass::Transient, "\"transient\""),
            (ErrorClass::Permanent, "\"permanent\""),
            (ErrorClass::Timeout, "\"timeout\""),
            (ErrorClass::Auth, "\"auth\""),
            (ErrorClass::Config, "\"config\""),
            (ErrorClass::RateLimit, "\"rate_limit\""),
        ] {
            let json = serde_json::to_string(&class).expect("serialize");
            assert_eq!(json, expected, "classification {class:?}");
            let round: ErrorClass = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(round, class);
        }
    }

    #[test]
    fn test_new_fields_roundtrip_through_serde() {
        let event = PipelineEvent::new("materialize_error")
            .with_run_id("run-1")
            .with_target("cat.sch.tbl")
            .with_error("HTTP 429")
            .with_attempt(3, 5)
            .with_error_class(ErrorClass::RateLimit);
        let json = serde_json::to_string(&event).expect("serialize");
        let back: PipelineEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.attempt, Some(3));
        assert_eq!(back.max_attempts, Some(5));
        assert_eq!(back.error_class, Some(ErrorClass::RateLimit));
    }

    #[test]
    fn test_missing_new_fields_deserialize_as_none() {
        // Backward-compatibility: older event payloads that lack the P2.8
        // fields should deserialize cleanly with None for each new slot.
        let legacy_json = r#"{
            "event_type": "pipeline_complete",
            "timestamp": "2026-04-19T12:00:00Z",
            "run_id": "run-legacy",
            "target": null,
            "duration_ms": 1234,
            "error": null,
            "metadata": {}
        }"#;
        let event: PipelineEvent =
            serde_json::from_str(legacy_json).expect("legacy payload deserializes");
        assert_eq!(event.attempt, None);
        assert_eq!(event.max_attempts, None);
        assert_eq!(event.error_class, None);
    }

    /// `record_span_event` must not panic when called outside any
    /// `tracing` span (e.g., a unit test that didn't enter a span).
    /// `Span::current()` returns the no-op span; the OTel layer's
    /// `add_event` impl is a silent drop on a no-op span.
    #[test]
    fn record_span_event_no_span_is_noop() {
        let evt = PipelineEvent::new("statement_retry")
            .with_target("databricks")
            .with_attempt(2, 5)
            .with_error_class(ErrorClass::Transient);
        // Must not panic.
        record_span_event(&evt);
    }

    /// `set_current_span_error` likewise must not panic on a no-op span.
    #[test]
    fn set_current_span_error_no_span_is_noop() {
        // Must not panic.
        set_current_span_error("retry budget exhausted (limit 25)");
    }

    /// With the `otel` feature on and a `tracing-opentelemetry` layer
    /// registered, `record_span_event` annotates the active span. The
    /// in-memory exporter pattern from `tracing_setup::tests` confirms
    /// the event lands with the right name and attribute values.
    #[cfg(feature = "otel")]
    #[test]
    fn record_span_event_annotates_active_span() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::error::OTelSdkResult;
        use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default, Debug)]
        struct Cap(Arc<Mutex<Vec<SpanData>>>);
        impl SpanExporter for Cap {
            async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
                if let Ok(mut g) = self.0.lock() {
                    g.extend(batch);
                }
                Ok(())
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let exp = Cap::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exp.clone())
            .build();
        let tracer = provider.tracer("rocky_test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("materialize.table");
            let _enter = span.enter();
            let evt = PipelineEvent::new("statement_retry")
                .with_target("databricks")
                .with_error("HTTP 503")
                .with_attempt(2, 5)
                .with_error_class(ErrorClass::Transient);
            record_span_event(&evt);
        });
        provider.shutdown().expect("shutdown");

        let captured = exp.0.lock().unwrap();
        let span = captured
            .iter()
            .find(|s| s.name == "materialize.table")
            .expect("span captured");
        let event = span
            .events
            .iter()
            .find(|e| e.name == "statement_retry")
            .expect("statement_retry event captured");

        // Iterate attributes and confirm the structured fields landed.
        let attrs: Vec<(String, String)> = event
            .attributes
            .iter()
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect();
        assert!(
            attrs
                .iter()
                .any(|(k, v)| k == "rocky.event.target" && v == "databricks"),
            "expected target attr, got {attrs:?}"
        );
        assert!(
            attrs
                .iter()
                .any(|(k, v)| k == "rocky.event.error_class" && v == "transient"),
            "expected error_class attr, got {attrs:?}"
        );
        assert!(
            attrs
                .iter()
                .any(|(k, v)| k == "rocky.event.attempt" && v == "2"),
            "expected attempt attr, got {attrs:?}"
        );
        assert!(
            attrs
                .iter()
                .any(|(k, v)| k == "rocky.event.max_attempts" && v == "5"),
            "expected max_attempts attr, got {attrs:?}"
        );
        assert!(
            attrs
                .iter()
                .any(|(k, v)| k == "rocky.event.error" && v == "HTTP 503"),
            "expected error attr, got {attrs:?}"
        );
    }

    /// `set_current_span_error` should set the OTel span status to
    /// `Error` with the supplied description when the layer is on.
    #[cfg(feature = "otel")]
    #[test]
    fn set_current_span_error_sets_otel_status() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::error::OTelSdkResult;
        use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default, Debug)]
        struct Cap(Arc<Mutex<Vec<SpanData>>>);
        impl SpanExporter for Cap {
            async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
                if let Ok(mut g) = self.0.lock() {
                    g.extend(batch);
                }
                Ok(())
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _enter_rt = rt.enter();

        let exp = Cap::default();
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exp.clone())
            .build();
        let tracer = provider.tracer("rocky_test");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("materialize.table");
            let _enter = span.enter();
            set_current_span_error("retry budget exhausted (limit 25)");
        });
        provider.shutdown().expect("shutdown");

        let captured = exp.0.lock().unwrap();
        let span = captured
            .iter()
            .find(|s| s.name == "materialize.table")
            .expect("span captured");
        match &span.status {
            opentelemetry::trace::Status::Error { description } => {
                assert_eq!(description.as_ref(), "retry budget exhausted (limit 25)");
            }
            other => panic!("expected Error status, got {other:?}"),
        }
    }
}
