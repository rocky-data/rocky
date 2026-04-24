//! `rocky run --dag` — execute every pipeline as a unified DAG.
//!
//! Builds the unified DAG from `rocky.toml` + the loaded models/seeds, then
//! invokes the [`DagExecutor`] with a dispatcher that delegates each node to
//! its existing per-pipeline-type entrypoint (replication / transformation /
//! quality / snapshot / load / seed).
//!
//! Results are emitted as a [`DagRunOutput`] in JSON mode so orchestrators
//! can correlate per-node status, timing, and errors.

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::dag_executor::{DagExecutor, NodeDispatcher, NodeFuture, NodeStatus};
use rocky_core::unified_dag::{self, NodeId, NodeKind};

use crate::output::{DagRunNodeOutput, DagRunOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky run --dag`: run every pipeline in dependency order.
pub async fn run_with_dag(config_path: &Path, json: bool) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    // Load models from the conventional `models/` directory next to the config.
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    let models_dir = config_dir.join("models");
    let models = if models_dir.is_dir() {
        super::dag::load_all_models(&models_dir).unwrap_or_default()
    } else {
        Vec::new()
    };

    let seeds_dir = config_dir.join("seeds");
    let seeds = if seeds_dir.is_dir() {
        rocky_core::seeds::discover_seeds(&seeds_dir).unwrap_or_default()
    } else {
        Vec::new()
    };

    let dag = unified_dag::build_unified_dag(&cfg, &models, &seeds)
        .context("failed to build unified DAG")?;
    info!(
        nodes = dag.node_count(),
        edges = dag.edge_count(),
        "executing unified DAG"
    );

    let dispatcher = CliDispatcher {
        config_path: config_path.to_path_buf(),
    };
    let executor = DagExecutor::new(dispatcher);
    let result = executor
        .execute(&dag)
        .await
        .context("DAG execution failed")?;

    if json {
        let output = DagRunOutput {
            version: VERSION.into(),
            command: "run --dag".into(),
            total_nodes: result.total_nodes,
            total_layers: result.total_layers,
            completed: result.completed,
            failed: result.failed,
            skipped: result.skipped,
            duration_ms: result.duration_ms,
            nodes: result
                .nodes
                .iter()
                .map(|n| DagRunNodeOutput {
                    id: n.id.clone(),
                    kind: n.kind.clone(),
                    label: n.label.clone(),
                    layer: n.layer,
                    status: status_str(&n.status).to_string(),
                    duration_ms: n.duration_ms,
                    error: n.error.clone(),
                })
                .collect(),
        };
        print_json(&output)?;
    } else {
        println!(
            "DAG run: {} nodes across {} layers ({} completed, {} failed, {} skipped) in {}ms",
            result.total_nodes,
            result.total_layers,
            result.completed,
            result.failed,
            result.skipped,
            result.duration_ms
        );
    }

    if result.had_failures() {
        anyhow::bail!("DAG execution had {} failed node(s)", result.failed);
    }
    Ok(())
}

fn status_str(s: &NodeStatus) -> &'static str {
    match s {
        NodeStatus::Pending => "pending",
        NodeStatus::Running => "running",
        NodeStatus::Completed => "completed",
        NodeStatus::Failed => "failed",
        NodeStatus::Skipped => "skipped",
    }
}

/// Dispatcher that turns each `NodeKind` into a future calling the matching
/// CLI command function.
///
/// The current implementation is intentionally simple: each non-trivial node
/// type is dispatched via `super::run::run()` with the pipeline name picked
/// up from the node's `pipeline` label. Test nodes are no-ops (tests run
/// implicitly inside their parent transformation).
struct CliDispatcher {
    config_path: std::path::PathBuf,
}

impl NodeDispatcher for CliDispatcher {
    fn dispatch(&self, _id: &NodeId, kind: NodeKind, label: &str) -> Option<NodeFuture> {
        let config_path = self.config_path.clone();
        let label = label.to_string();
        match kind {
            NodeKind::Test => {
                // Tests run as part of `transformation` execution; the DAG
                // entry is informational. Return None → marked Skipped.
                None
            }
            NodeKind::Source => {
                // Source nodes represent the extract side of a replication
                // pipeline — handled by the corresponding load node, so the
                // source itself is a marker.
                Some(Box::pin(async move {
                    info!(label = %label, "DAG: source marker (no-op)");
                    Ok(())
                }))
            }
            _ => Some(Box::pin(async move {
                let pipeline_name = label.clone();
                let partition_opts = super::PartitionRunOptions {
                    partition: None,
                    from: None,
                    to: None,
                    latest: false,
                    missing: false,
                    lookback: None,
                    parallel: 1,
                };
                // Use a pipeline-local state path next to the config.
                let state_path = config_path
                    .parent()
                    .unwrap_or(Path::new("."))
                    .join(".rocky_state");
                super::run::run(
                    &config_path,
                    None,
                    Some(&pipeline_name),
                    &state_path,
                    None,
                    false, // json — sub-runs print to stdout if not silenced
                    None,
                    false,
                    None,
                    false,
                    None,
                    &partition_opts,
                    None,
                    // DAG sub-runs inherit config-derived TTL. Threading
                    // the outer `--cache-ttl` through a unified-DAG
                    // orchestration layer would complicate the sub-run
                    // contract without clear signal; defer unless a
                    // concrete use case shows up.
                    None,
                    // DAG sub-runs do not accept `--idempotency-key`; the
                    // caller stamps dedup on the outer DAG driver, and
                    // cascading the key into every pipeline sub-run would
                    // cause every sibling to short-circuit on a single
                    // stamp.
                    None,
                    // DAG sub-runs inherit no `--env`; the outer DAG
                    // driver is pipeline-agnostic and the per-pipeline
                    // governance reconcile picks up the resolved mask on
                    // its own call path. Passing `None` preserves the
                    // pre-1.16 workspace-default resolution for DAG runs.
                    None,
                )
                .await
                .map_err(|e| e.to_string())
            })),
        }
    }
}
