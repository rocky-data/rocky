//! Live DAG execution status — observable from `rocky serve`'s HTTP API.
//!
//! Wraps a [`DagExecutionResult`][crate::dag_executor::DagExecutionResult] in
//! a thread-safe container. The `DagExecutor` writes the latest run's result
//! here when invoked through the server; HTTP clients (e.g. dashboards or
//! orchestrators) can poll `GET /api/v1/dag/status` to inspect progress.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::dag_executor::DagExecutionResult;

/// Snapshot of the most recent DAG execution.
#[derive(Debug, Clone, Serialize)]
pub struct DagStatus {
    /// When the execution finished (UTC).
    pub completed_at: DateTime<Utc>,
    /// The aggregate execution result.
    pub result: DagExecutionResult,
}

/// Thread-safe holder of the latest [`DagStatus`].
///
/// Stored in `ServerState` so the HTTP handler can read it concurrently with
/// the executor writing it.
#[derive(Debug, Clone, Default)]
pub struct DagStatusStore {
    inner: Arc<RwLock<Option<DagStatus>>>,
}

impl DagStatusStore {
    /// Create an empty store (no execution recorded yet).
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fresh execution result; replaces any prior value.
    pub async fn set(&self, result: DagExecutionResult) {
        let status = DagStatus {
            completed_at: Utc::now(),
            result,
        };
        *self.inner.write().await = Some(status);
    }

    /// Read the latest recorded status, if any.
    pub async fn get(&self) -> Option<DagStatus> {
        self.inner.read().await.clone()
    }

    /// Returns `true` if at least one execution has been recorded.
    pub async fn has_status(&self) -> bool {
        self.inner.read().await.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag_executor::{DagExecutionResult, NodeResult, NodeStatus};

    fn sample_result() -> DagExecutionResult {
        DagExecutionResult {
            nodes: vec![NodeResult {
                id: "transformation:stg_orders".into(),
                kind: "transformation".into(),
                label: "stg_orders".into(),
                status: NodeStatus::Completed,
                layer: 0,
                duration_ms: 42,
                error: None,
            }],
            total_layers: 1,
            total_nodes: 1,
            completed: 1,
            failed: 0,
            skipped: 0,
            duration_ms: 100,
        }
    }

    #[tokio::test]
    async fn test_empty_store() {
        let store = DagStatusStore::new();
        assert!(!store.has_status().await);
        assert!(store.get().await.is_none());
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let store = DagStatusStore::new();
        store.set(sample_result()).await;
        assert!(store.has_status().await);
        let got = store.get().await.unwrap();
        assert_eq!(got.result.completed, 1);
        assert_eq!(got.result.nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_set_replaces() {
        let store = DagStatusStore::new();
        store.set(sample_result()).await;
        let mut second = sample_result();
        second.completed = 0;
        second.failed = 1;
        store.set(second).await;
        let got = store.get().await.unwrap();
        assert_eq!(got.result.failed, 1);
        assert_eq!(got.result.completed, 0);
    }
}
