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
}
