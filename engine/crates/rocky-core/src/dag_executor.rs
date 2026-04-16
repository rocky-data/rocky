//! DAG-driven execution: run all pipelines in dependency order.
//!
//! `DagExecutor` walks a [`UnifiedDag`] using
//! [`execution_phases`][crate::unified_dag::execution_phases] (Kahn's
//! topological layering), executing all nodes in a layer concurrently and
//! moving to the next layer once they complete.
//!
//! The executor is **adapter-agnostic at the dispatch level**: each node's
//! `NodeKind` maps to a per-pipeline-type executor function supplied by the
//! caller (a closure over the existing per-pipeline run functions in
//! `rocky-cli`). The DAG executor itself owns no warehouse adapters.
//!
//! # Failure semantics
//!
//! Skip-downstream-on-failure: if node N fails, every node whose execution-DAG
//! ancestor set contains N is marked `Skipped` rather than executed. The
//! overall run is reported as failed but every layer still runs to completion
//! so unrelated branches finish.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::unified_dag::{NodeId, NodeKind, UnifiedDag, UnifiedDagError, execution_phases};

/// A boxed future returning the outcome of a single node execution.
///
/// Not required to be `Send` — within a layer, nodes are awaited sequentially
/// on the current task. This lets node implementations use `!Send` types (such
/// as `tracing`'s `EnteredSpan`) without having to refactor for cross-thread
/// safety. Layer-level dependency ordering is preserved.
pub type NodeFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// Dispatcher that maps a [`NodeKind`] + node identity to an executor future.
///
/// Implementors return `None` if the node kind is not handled by this
/// dispatcher (e.g. `Test` nodes that run implicitly inside their parent
/// transformation). The DAG executor treats `None` as "no-op success" — the
/// node is marked completed without running.
pub trait NodeDispatcher: Send + Sync {
    /// Build the future for a node, or return `None` to skip it.
    fn dispatch(&self, node_id: &NodeId, kind: NodeKind, label: &str) -> Option<NodeFuture>;
}

/// Status of a single node's execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node has not started yet.
    Pending,
    /// Node is currently executing.
    Running,
    /// Node completed successfully.
    Completed,
    /// Node failed; error message is attached.
    Failed,
    /// Node was skipped because an ancestor failed (or it was a Test node
    /// not handled by the dispatcher).
    Skipped,
}

/// Per-node execution record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResult {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub status: NodeStatus,
    pub layer: usize,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Aggregate results from a DAG execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagExecutionResult {
    pub nodes: Vec<NodeResult>,
    pub total_layers: usize,
    pub total_nodes: usize,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub duration_ms: u64,
}

impl DagExecutionResult {
    /// `true` if any node failed (or had a downstream skip).
    pub fn had_failures(&self) -> bool {
        self.failed > 0
    }
}

/// Errors specific to DAG execution (vs. errors *within* a node).
#[derive(Debug, thiserror::Error)]
pub enum DagExecutorError {
    #[error("DAG validation failed: {0}")]
    Dag(#[from] UnifiedDagError),
}

/// Executes a unified DAG using the supplied dispatcher.
pub struct DagExecutor<D: NodeDispatcher> {
    dispatcher: Arc<D>,
    /// Maximum number of concurrent node executions per layer.
    /// Defaults to the number of nodes in the layer; override for resource caps.
    pub max_concurrency: Option<usize>,
}

impl<D: NodeDispatcher + 'static> DagExecutor<D> {
    pub fn new(dispatcher: D) -> Self {
        Self {
            dispatcher: Arc::new(dispatcher),
            max_concurrency: None,
        }
    }

    pub fn with_max_concurrency(mut self, n: usize) -> Self {
        self.max_concurrency = Some(n);
        self
    }

    /// Execute the entire DAG, layer by layer.
    ///
    /// Layers respect topological dependencies (Kahn's algorithm). Nodes
    /// within a layer run sequentially on the current task — within-layer
    /// parallelism is a future enhancement (blocked on making per-node
    /// executors `Send`-safe; tracing spans currently aren't).
    pub async fn execute(&self, dag: &UnifiedDag) -> Result<DagExecutionResult, DagExecutorError> {
        let start = Instant::now();
        let layers = execution_phases(dag)?;
        let total_layers = layers.len();

        let mut failed_nodes: HashSet<NodeId> = HashSet::new();
        let mut results: Vec<NodeResult> = Vec::new();
        let ancestors = build_ancestor_map(dag);

        for (layer_idx, layer_nodes) in layers.iter().enumerate() {
            info!(
                layer = layer_idx,
                nodes = layer_nodes.len(),
                "executing DAG layer"
            );

            for node in layer_nodes {
                let id = node.id.clone();
                let kind = node.kind;
                let label = node.label.clone();

                // Skip if any ancestor has failed.
                let ancestors_for_node = ancestors.get(&id).cloned().unwrap_or_default();
                let should_skip = ancestors_for_node.iter().any(|a| failed_nodes.contains(a));
                if should_skip {
                    warn!(node = %id, "skipping node — upstream failure");
                    results.push(NodeResult {
                        id: id.0.clone(),
                        kind: kind.to_string(),
                        label,
                        status: NodeStatus::Skipped,
                        layer: layer_idx,
                        duration_ms: 0,
                        error: Some("upstream failure".into()),
                    });
                    continue;
                }

                let node_start = Instant::now();
                let dispatched = self.dispatcher.dispatch(&id, kind, &label);
                let (status, error) = match dispatched {
                    None => (NodeStatus::Skipped, None),
                    Some(fut) => match fut.await {
                        Ok(()) => (NodeStatus::Completed, None),
                        Err(e) => {
                            warn!(node = %id, error = %e, "node failed");
                            failed_nodes.insert(id.clone());
                            (NodeStatus::Failed, Some(e))
                        }
                    },
                };

                results.push(NodeResult {
                    id: id.0.clone(),
                    kind: kind.to_string(),
                    label,
                    status,
                    layer: layer_idx,
                    duration_ms: node_start.elapsed().as_millis() as u64,
                    error,
                });
            }
        }

        // Stable order: by layer then id.
        results.sort_by(|a, b| a.layer.cmp(&b.layer).then_with(|| a.id.cmp(&b.id)));

        let completed = results
            .iter()
            .filter(|r| r.status == NodeStatus::Completed)
            .count();
        let failed = results
            .iter()
            .filter(|r| r.status == NodeStatus::Failed)
            .count();
        let skipped = results
            .iter()
            .filter(|r| r.status == NodeStatus::Skipped)
            .count();
        let total_nodes = results.len();

        Ok(DagExecutionResult {
            nodes: results,
            total_layers,
            total_nodes,
            completed,
            failed,
            skipped,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

/// Compute the set of ancestor node IDs for every node in the DAG.
///
/// `ancestors[X]` = transitive closure of nodes that must complete before X.
fn build_ancestor_map(dag: &UnifiedDag) -> HashMap<NodeId, HashSet<NodeId>> {
    let mut parents: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for edge in &dag.edges {
        parents
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }

    let mut ancestors: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
    for node in &dag.nodes {
        let mut acc = HashSet::new();
        collect_ancestors(&node.id, &parents, &mut acc);
        ancestors.insert(node.id.clone(), acc);
    }
    ancestors
}

fn collect_ancestors(
    id: &NodeId,
    parents: &HashMap<NodeId, Vec<NodeId>>,
    acc: &mut HashSet<NodeId>,
) {
    if let Some(direct) = parents.get(id) {
        for p in direct {
            if acc.insert(p.clone()) {
                collect_ancestors(p, parents, acc);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unified_dag::{EdgeType, UnifiedEdge, UnifiedNode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test dispatcher: counts dispatches, fails when label is "boom".
    struct CountingDispatcher {
        dispatched: Arc<AtomicUsize>,
    }

    impl NodeDispatcher for CountingDispatcher {
        fn dispatch(&self, _id: &NodeId, _kind: NodeKind, label: &str) -> Option<NodeFuture> {
            let counter = Arc::clone(&self.dispatched);
            let label = label.to_string();
            Some(Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                if label == "boom" {
                    Err("boom failed".into())
                } else {
                    Ok(())
                }
            }))
        }
    }

    fn n(id: &str, kind: NodeKind) -> UnifiedNode {
        UnifiedNode {
            id: NodeId(id.into()),
            kind,
            label: id.into(),
            pipeline: None,
        }
    }

    fn e(from: &str, to: &str) -> UnifiedEdge {
        UnifiedEdge {
            from: NodeId(from.into()),
            to: NodeId(to.into()),
            edge_type: EdgeType::DataDependency,
        }
    }

    #[tokio::test]
    async fn test_executes_simple_chain() {
        let dag = UnifiedDag {
            nodes: vec![
                n("a", NodeKind::Source),
                n("b", NodeKind::Transformation),
            ],
            edges: vec![e("a", "b")],
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatcher = CountingDispatcher {
            dispatched: Arc::clone(&counter),
        };
        let executor = DagExecutor::new(dispatcher);
        let result = executor.execute(&dag).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        assert_eq!(result.completed, 2);
        assert_eq!(result.failed, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.total_layers, 2);
    }

    #[tokio::test]
    async fn test_failure_skips_downstream() {
        let dag = UnifiedDag {
            nodes: vec![
                UnifiedNode {
                    id: NodeId("a".into()),
                    kind: NodeKind::Source,
                    label: "boom".into(),
                    pipeline: None,
                },
                n("b", NodeKind::Transformation),
                n("c", NodeKind::Quality),
            ],
            edges: vec![e("a", "b"), e("b", "c")],
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatcher = CountingDispatcher {
            dispatched: Arc::clone(&counter),
        };
        let executor = DagExecutor::new(dispatcher);
        let result = executor.execute(&dag).await.unwrap();

        assert!(result.had_failures());
        assert_eq!(result.failed, 1);
        // b and c should both be skipped (b's ancestor failed; c's ancestor b
        // was skipped, but b is also marked failed propagation-wise once its
        // upstream fails — actually skipped nodes don't add to failed_nodes,
        // so c should still be skipped because a is in failed_nodes).
        assert_eq!(result.skipped, 2);
        // Only `a` was actually dispatched.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_parallel_branches_both_run() {
        // Diamond: a → b, a → c, b → d, c → d
        let dag = UnifiedDag {
            nodes: vec![
                n("a", NodeKind::Source),
                n("b", NodeKind::Transformation),
                n("c", NodeKind::Transformation),
                n("d", NodeKind::Quality),
            ],
            edges: vec![e("a", "b"), e("a", "c"), e("b", "d"), e("c", "d")],
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatcher = CountingDispatcher {
            dispatched: Arc::clone(&counter),
        };
        let executor = DagExecutor::new(dispatcher);
        let result = executor.execute(&dag).await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 4);
        assert_eq!(result.completed, 4);
    }

    #[tokio::test]
    async fn test_unrelated_branch_runs_after_failure() {
        // a (boom) → b ; c → d (independent branch)
        let dag = UnifiedDag {
            nodes: vec![
                UnifiedNode {
                    id: NodeId("a".into()),
                    kind: NodeKind::Source,
                    label: "boom".into(),
                    pipeline: None,
                },
                n("b", NodeKind::Transformation),
                n("c", NodeKind::Source),
                n("d", NodeKind::Transformation),
            ],
            edges: vec![e("a", "b"), e("c", "d")],
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let dispatcher = CountingDispatcher {
            dispatched: Arc::clone(&counter),
        };
        let executor = DagExecutor::new(dispatcher);
        let result = executor.execute(&dag).await.unwrap();

        // a, c, d all dispatched (b skipped). 3 dispatches.
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert_eq!(result.failed, 1); // a
        assert_eq!(result.completed, 2); // c, d
        assert_eq!(result.skipped, 1); // b
    }

    #[test]
    fn test_build_ancestor_map_diamond() {
        let dag = UnifiedDag {
            nodes: vec![
                n("a", NodeKind::Source),
                n("b", NodeKind::Transformation),
                n("c", NodeKind::Transformation),
                n("d", NodeKind::Quality),
            ],
            edges: vec![e("a", "b"), e("a", "c"), e("b", "d"), e("c", "d")],
        };
        let map = build_ancestor_map(&dag);
        let d_ancestors = &map[&NodeId("d".into())];
        assert!(d_ancestors.contains(&NodeId("a".into())));
        assert!(d_ancestors.contains(&NodeId("b".into())));
        assert!(d_ancestors.contains(&NodeId("c".into())));
        assert_eq!(d_ancestors.len(), 3);
    }
}
