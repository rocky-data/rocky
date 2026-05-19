//! OTLP span events for the state-cache layer.
//!
//! Mirrors the FR-B `fivetran.rate_limit_observed` emission pattern in
//! [`crate::client`] — every cache decision becomes a structured span
//! event so operators can prove cold-start volume reduction in their
//! tracing backend without parsing log strings.
//!
//! Each event is also published to the global pipeline event bus so
//! sensor / orchestrator code that subscribes to `PipelineEvent` sees
//! the same signal as the OTLP exporter.

use chrono::Utc;
use rocky_observe::events::{PipelineEvent, global_event_bus, record_span_event};
use tracing::warn;

use super::CacheError;
use crate::envelope::FivetranStateEnvelope;

/// Emit `fivetran.cache_hit` — cache served the read; HTTP path was
/// skipped. `age_seconds` is the wall-clock distance between the
/// envelope's `fetched_at` and now; lets dashboards distinguish
/// stale-but-warm hits from genuinely-fresh hits.
pub fn emit_cache_hit(backend: &str, key: &str, envelope: &FivetranStateEnvelope) {
    let age_seconds = (Utc::now() - envelope.fetched_at).num_seconds().max(0);
    let evt = PipelineEvent::new("fivetran.cache_hit")
        .with_metadata("backend", backend.to_string())
        .with_metadata("key", key.to_string())
        .with_metadata("age_seconds", age_seconds);
    record_span_event(&evt);
    global_event_bus().emit(evt);
}

/// Emit `fivetran.cache_miss` — cache had no entry (or refresh was
/// forced) so the HTTP path is about to run. `reason` is one of
/// `"no-entry"` or `"refresh-forced"`; the second variant lets
/// operators see how often `--no-cache` is being exercised in the wild.
pub fn emit_cache_miss(backend: &str, key: &str, reason: &str) {
    let evt = PipelineEvent::new("fivetran.cache_miss")
        .with_metadata("backend", backend.to_string())
        .with_metadata("key", key.to_string())
        .with_metadata("reason", reason.to_string());
    record_span_event(&evt);
    global_event_bus().emit(evt);
}

/// Emit `fivetran.cache_write` — backend persisted a new envelope.
/// `bytes` is the serialized payload size; gives operators a feel for
/// the steady-state size of a destination envelope.
pub fn emit_cache_write(backend: &str, key: &str, envelope: &FivetranStateEnvelope) {
    let bytes = serde_json::to_vec(envelope)
        .map(|v| v.len() as u64)
        .unwrap_or(0);
    let evt = PipelineEvent::new("fivetran.cache_write")
        .with_metadata("backend", backend.to_string())
        .with_metadata("key", key.to_string())
        .with_metadata("bytes", bytes)
        .with_metadata("outcome", "written".to_string());
    record_span_event(&evt);
    global_event_bus().emit(evt);
}

/// Emit `fivetran.cache_write_skipped` — backend short-circuited the
/// write because the new envelope hash-matched the prior. The
/// hash-dedupe signal is the easiest way to verify on the dashboard
/// that the cache layer is actually paying for itself.
pub fn emit_cache_write_skipped(backend: &str, key: &str, reason: &str) {
    let evt = PipelineEvent::new("fivetran.cache_write_skipped")
        .with_metadata("backend", backend.to_string())
        .with_metadata("key", key.to_string())
        .with_metadata("reason", reason.to_string());
    record_span_event(&evt);
    global_event_bus().emit(evt);
}

/// Emit `fivetran.cache_write_failed` — write propagated up. The cache
/// layer falls open here (the HTTP path already succeeded), but
/// surfacing the failure as a structured event lets ops alert on a
/// pathologically failing backend.
pub fn emit_cache_write_failed(backend: &str, key: &str, error: &CacheError) {
    let evt = PipelineEvent::new("fivetran.cache_write_failed")
        .with_metadata("backend", backend.to_string())
        .with_metadata("key", key.to_string())
        .with_metadata("error", error.to_string());
    record_span_event(&evt);
    global_event_bus().emit(evt);

    // Pair the span event with a `warn!` so the failure shows up in
    // log searches too — operators often hit logs before tracing.
    warn!(
        backend,
        key,
        error = %error,
        "fivetran cache write failed (fail-open; HTTP path already succeeded)"
    );
}
