//! `rocky run --dag` — execute every pipeline as a unified DAG.
//!
//! Builds the unified DAG from `rocky.toml` + the loaded models/seeds, then
//! invokes the [`DagExecutor`] with a dispatcher that delegates each node to
//! its existing per-pipeline-type entrypoint (replication / transformation /
//! quality / snapshot / load / seed).
//!
//! Results are emitted as a [`DagRunOutput`] in JSON mode so orchestrators
//! can correlate per-node status, timing, and errors.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::dag_executor::{DagExecutor, NodeDispatcher, NodeFuture, NodeStatus};
use rocky_core::unified_dag::{self, NodeId, NodeKind};

use super::run::SkipRunOptions;
use crate::output::{DagRunNodeOutput, DagRunOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Runs one pipeline sub-run: `(config_path, state_path, pipeline_name,
/// model_name, skip_opts) -> Result<(), String>`.
///
/// Injected into [`CliDispatcher`] so the escape-hatch flags flow to a single,
/// observable seam. Production wraps [`super::run::run`] with the DAG sub-run's
/// fixed arguments ([`default_sub_runner`]); tests substitute a recorder that
/// captures the exact [`SkipRunOptions`] the dispatch passes — so reverting the
/// dispatch to `SkipRunOptions::default()` is caught.
type SubRunner = Arc<
    dyn Fn(
            PathBuf,
            PathBuf,
            String,
            Option<String>,
            SkipRunOptions,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// The production [`SubRunner`]: drives one pipeline through [`super::run::run`]
/// with the DAG sub-run's fixed arguments (no `--defer`/`--var`, config-derived
/// TTL, no idempotency key). Transformation nodes supply their model name so
/// each node materializes only itself; other pipeline nodes pass `None`.
fn default_sub_runner() -> SubRunner {
    Arc::new(
        |config_path: PathBuf,
         state_path: PathBuf,
         pipeline_name: String,
         model_name: Option<String>,
         skip_opts| {
            Box::pin(async move {
                let models_dir = models_dir_for_model_scope(&config_path, model_name.as_deref());
                let partition_opts = super::PartitionRunOptions {
                    partition: None,
                    from: None,
                    to: None,
                    latest: false,
                    missing: false,
                    lookback: None,
                    parallel: 1,
                };
                super::run::run(
                    &config_path,
                    None,
                    Some(&pipeline_name),
                    &state_path,
                    None,
                    false, // json — sub-runs print to stdout if not silenced
                    models_dir.as_deref(),
                    false,
                    None,
                    false,
                    None,
                    &partition_opts,
                    model_name.as_deref(),
                    // DAG sub-runs inherit config-derived TTL.
                    None,
                    // DAG sub-runs do not accept `--idempotency-key`.
                    None,
                    // DAG sub-runs inherit no `--env`.
                    None,
                    &super::run::DeferOptions::default(),
                    // The build-escape-hatch overlay (`--force-rebuild` /
                    // `--no-reuse`) threaded from the outer `rocky run --dag`.
                    &skip_opts,
                    // The unified-DAG driver does not surface `--var`.
                    &rocky_core::run_vars::RunVars::new(),
                    // No run_id override — DAG sub-runs mint their own ids.
                    None,
                    // DAG sub-runs are not governed two-step applies — no gate.
                    None,
                )
                .await
                .map_err(|e| e.to_string())
            })
        },
    )
}

/// Returns the conventional models directory only for a model-scoped
/// transformation sub-run. Supplying it to replication/load sub-runs makes
/// `run()` execute every transformation model after the pipeline itself.
fn models_dir_for_model_scope(config_path: &Path, model_name: Option<&str>) -> Option<PathBuf> {
    model_name.map(|_| {
        config_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("models")
    })
}

/// Execute `rocky run --dag`: run every pipeline in dependency order.
///
/// `state_path` is the canonical state location already resolved by the
/// caller (`main.rs` via `resolve_state_path_ns`, honoring `--state-path` /
/// `--state-namespace` / the `<models>/.rocky-state.redb` default). Each
/// per-pipeline sub-run is driven through `run()` against this same path, so
/// the unified-DAG path shares the project's canonical state with every other
/// `rocky run` invocation — it must never invent its own `.rocky_state` file.
pub async fn run_with_dag(
    config_path: &Path,
    state_path: &Path,
    json: bool,
    // Build-escape-hatch overlay (`--force-rebuild` / `--no-reuse`). Threaded
    // into every sub-run so `rocky run --dag --force-rebuild` actually forces a
    // build (and `--no-reuse` disables content-addressed reuse + column-skip)
    // instead of being silently dropped at the DAG boundary.
    skip_opts: &super::run::SkipRunOptions,
) -> Result<()> {
    // Under `-o json` the orchestrator contract is that stdout is exactly one
    // JSON document (the `DagRunOutput` below). Sub-runs are dispatched with
    // `json = false` so they don't each emit their own JSON payload, which
    // means they take their human-summary branch — route those lines to stderr
    // so they can't precede the JSON on stdout. See `crate::status_line!`.
    if json {
        crate::output::reserve_stdout_for_json();
    }

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

    let mut dag = unified_dag::build_unified_dag(&cfg, &models, &seeds)
        .context("failed to build unified DAG")?;

    // Infer cross-step edges from each model's SQL `FROM` references so a
    // model that reads a seed (or replication load) is ordered *after* it,
    // even when no explicit `depends_on` is declared. Without this, a seed
    // and a model that selects from it both land in layer 0 and race.
    let sql_by_name: HashMap<String, String> = models
        .iter()
        .map(|m| (m.config.name.clone(), m.sql.clone()))
        .collect();
    unified_dag::infer_runtime_dependencies(&mut dag, &sql_by_name);

    info!(
        nodes = dag.node_count(),
        edges = dag.edge_count(),
        "executing unified DAG"
    );

    // Map each node to its owning pipeline (when it has one). The dispatcher
    // needs the pipeline name to drive `run()`; a per-model node's *label* is
    // the model name, not the pipeline, so it can't be used as the pipeline.
    let node_pipelines: HashMap<NodeId, String> = dag
        .nodes
        .iter()
        .filter_map(|n| n.pipeline.as_ref().map(|p| (n.id.clone(), p.clone())))
        .collect();

    let dispatcher = CliDispatcher {
        config_path: config_path.to_path_buf(),
        state_path: state_path.to_path_buf(),
        seeds_dir,
        node_pipelines,
        skip_opts: *skip_opts,
        sub_runner: default_sub_runner(),
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
/// Each pipeline-bound node is dispatched via `super::run::run()` against its
/// owning pipeline. Transformation nodes also pass their model label so each
/// per-model node materializes only itself; load / quality / snapshot nodes
/// continue to run their whole pipeline. `Seed` nodes load their CSV directly,
/// `Test` nodes are no-ops, and `Source` nodes are markers.
struct CliDispatcher {
    config_path: std::path::PathBuf,
    /// Canonical state path threaded from the caller. Every sub-run drives
    /// `run()` against this shared path so the unified-DAG path reads and
    /// writes the project's canonical `.rocky-state.redb` (or the namespaced
    /// / `--state-path` override) — never a private `.rocky_state` file.
    state_path: std::path::PathBuf,
    /// `seeds/` directory next to the config, used to dispatch `Seed` nodes.
    seeds_dir: std::path::PathBuf,
    /// Maps each pipeline-bound node to its owning pipeline name. Seed and
    /// source-marker nodes carry no entry (their `pipeline` is `None`).
    node_pipelines: HashMap<NodeId, String>,
    /// The `--force-rebuild` / `--no-reuse` build-escape-hatch overlay, threaded
    /// from the outer `run_with_dag` so each sub-run honors it. `Copy`, so the
    /// per-node closure captures a value (no borrow escaping the dispatcher).
    skip_opts: super::run::SkipRunOptions,
    /// The injected sub-run driver. Production is [`default_sub_runner`]; tests
    /// substitute a recorder to observe the `skip_opts` each sub-run receives.
    sub_runner: SubRunner,
}

impl NodeDispatcher for CliDispatcher {
    fn dispatch(&self, id: &NodeId, kind: NodeKind, label: &str) -> Option<NodeFuture> {
        let config_path = self.config_path.clone();
        let state_path = self.state_path.clone();
        let skip_opts = self.skip_opts;
        let sub_runner = self.sub_runner.clone();
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
            NodeKind::Seed => {
                // A seed is not a pipeline — driving it through `run()` would
                // fail with "pipeline '<seed>' not found in config". Load the
                // matching CSV directly via `run_seed` (which fires the seed's
                // pre/post hooks). The node label is the seed name, so a name
                // filter selects exactly this seed.
                let seeds_dir = self.seeds_dir.clone();
                Some(Box::pin(async move {
                    super::seed::run_seed(&config_path, &seeds_dir, None, Some(&label), false)
                        .await
                        .map_err(|e| e.to_string())
                }))
            }
            _ => {
                // Pipeline-bound nodes (transformation / load / quality /
                // snapshot, plus the replication sugar's load node). Drive
                // `run()` against the node's *owning pipeline*, not its label:
                // a per-model transformation node's label is the model name,
                // which is not a pipeline.
                let pipeline_name = match self.node_pipelines.get(id) {
                    Some(p) => p.clone(),
                    None => {
                        return Some(Box::pin(async move {
                            Err(format!(
                                "DAG node '{label}' has no associated pipeline to execute"
                            ))
                        }));
                    }
                };
                let model_name = matches!(kind, NodeKind::Transformation).then_some(label.clone());
                // Drive the sub-run through the injected [`SubRunner`] against
                // the canonical state path the caller resolved, threading the
                // build-escape-hatch `skip_opts` (`--force-rebuild` /
                // `--no-reuse`) so they are honored per sub-run rather than
                // dropped at the DAG boundary. Passing anything other than
                // `skip_opts` here reintroduces the silent-drop bug, which the
                // `sub_runner` recorder test catches.
                Some(Box::pin(async move {
                    (sub_runner)(
                        config_path,
                        state_path,
                        pipeline_name,
                        model_name,
                        skip_opts,
                    )
                    .await
                }))
            }
        }
    }
}

#[cfg(test)]
mod skip_opts_threading_tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use rocky_core::dag_executor::NodeDispatcher;
    use rocky_core::unified_dag::{NodeId, NodeKind};

    use super::super::run::SkipRunOptions;
    use super::{CliDispatcher, SubRunner, models_dir_for_model_scope};

    type RecordedSubRun = (Option<String>, SkipRunOptions);

    #[test]
    fn models_dir_is_only_set_for_model_scoped_sub_runs() {
        let config_path = Path::new("project/rocky.toml");

        assert_eq!(
            models_dir_for_model_scope(config_path, Some("dim_orders")),
            Some(PathBuf::from("project/models"))
        );
        assert_eq!(models_dir_for_model_scope(config_path, None), None);
    }

    /// 🔴 DEFECT 3 regression: `rocky run --dag --force-rebuild` / `--no-reuse`
    /// must reach each sub-run. Pre-fix, `run_with_dag` passed
    /// `SkipRunOptions::default()` to every sub-run, silently dropping the
    /// escape hatches (a column-skip-eligible content-addressed model would
    /// still skip under `--dag --force-rebuild`).
    ///
    /// Non-vacuous: a recording [`SubRunner`] captures the EXACT `SkipRunOptions`
    /// the pipeline dispatch passes. Reverting the dispatch call argument to
    /// `SkipRunOptions::default()` makes the recorded value `default()` and
    /// fails the assertion. (The behavioral end-to-end proof — a
    /// content-addressed model not skipping — needs s3 and is exercised by the
    /// live sandbox.)
    #[tokio::test]
    async fn dispatch_passes_the_threaded_skip_opts_to_each_sub_run() {
        let recorded: Arc<Mutex<Vec<RecordedSubRun>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = recorded.clone();
        // A recorder sub-runner: capture the skip_opts and return Ok without
        // running a real pipeline.
        let recorder: SubRunner =
            Arc::new(move |_config, _state, _pipeline, model_name, skip_opts| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().push((model_name, skip_opts));
                    Ok(())
                })
            });

        let skip_opts = SkipRunOptions {
            skip_unchanged: false,
            force_rebuild: true,
            no_reuse: true,
            no_prune: false,
        };
        let mut node_pipelines = HashMap::new();
        let node_id = NodeId::new("transformation", "dim_orders");
        node_pipelines.insert(node_id.clone(), "analytics".to_string());
        let dispatcher = CliDispatcher {
            config_path: std::path::PathBuf::from("rocky.toml"),
            state_path: std::path::PathBuf::from(".rocky-state.redb"),
            seeds_dir: std::path::PathBuf::from("seeds"),
            node_pipelines,
            skip_opts,
            sub_runner: recorder,
        };

        // Drive a real pipeline-node dispatch through the production path.
        let fut = dispatcher
            .dispatch(&node_id, NodeKind::Transformation, "dim_orders")
            .expect("a pipeline node dispatches a future");
        fut.await.expect("recorder sub-runner returns Ok");

        let got = recorded.lock().unwrap();
        assert_eq!(got.len(), 1, "exactly one sub-run was dispatched");
        assert_eq!(got[0].0.as_deref(), Some("dim_orders"));
        assert!(
            got[0].1.force_rebuild,
            "--force-rebuild must reach the sub-run, not be dropped to default()"
        );
        assert!(
            got[0].1.no_reuse,
            "--no-reuse must reach the sub-run, not be dropped to default()"
        );
    }
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;

    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    fn cell_i64(v: &serde_json::Value) -> i64 {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("expected integer cell, got {v:?}"))
    }

    /// End-to-end acceptance for B6: under `run --dag`, a `Seed` node loads its
    /// CSV (firing its pre/post hooks), and a model that reads the seed via SQL
    /// is ordered after it and materializes against the loaded data.
    #[tokio::test]
    async fn seed_node_loads_fires_hooks_and_dependent_model_materializes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models")).unwrap();
        std::fs::create_dir_all(root.join("seeds")).unwrap();

        let db_path = root.join("proj.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter.local]\n\
                 type = \"duckdb\"\n\
                 path = \"{}\"\n\n\
                 [pipeline.silver]\n\
                 type = \"transformation\"\n\n\
                 [pipeline.silver.target]\n\
                 adapter = \"local\"\n\n\
                 [pipeline.silver.target.governance]\n\
                 auto_create_catalogs = true\n\
                 auto_create_schemas = true\n",
                db_path.display()
            ),
        )
        .unwrap();

        std::fs::write(
            root.join("seeds/countries.csv"),
            "code,name\nUS,United States\nGB,United Kingdom\n",
        )
        .unwrap();
        // `post_hook` materializes a marker only reachable if the hook fired
        // after the load; `pre_hook` writes a marker before the load. Both
        // prove the seed's hooks ran under the DAG path.
        std::fs::write(
            root.join("seeds/countries.toml"),
            "name = \"countries\"\n\
             pre_hook = [\"CREATE TABLE proj.pre_marker AS SELECT 1 AS fired\"]\n\
             post_hook = [\"CREATE TABLE proj.seeds.post_marker AS SELECT COUNT(*) AS n FROM proj.seeds.countries\"]\n\n\
             [target]\n\
             catalog = \"proj\"\n\
             schema = \"seeds\"\n\
             table = \"countries\"\n",
        )
        .unwrap();

        // Model reads the seed via SQL (no explicit `depends_on`, which the
        // compiler would reject for a non-model). `run_with_dag` infers the
        // seed→model edge from this `FROM` reference.
        std::fs::write(
            root.join("models/dim_country.sql"),
            "SELECT code, name FROM proj.seeds.countries\n",
        )
        .unwrap();
        std::fs::write(
            root.join("models/dim_country.toml"),
            "name = \"dim_country\"\n\n\
             [target]\n\
             catalog = \"proj\"\n\
             schema = \"silver\"\n\
             table = \"dim_country\"\n",
        )
        .unwrap();

        let config_path = root.join("rocky.toml");
        let state_path = root.join(".rocky-state.redb");
        run_with_dag(
            &config_path,
            &state_path,
            false,
            &crate::commands::run::SkipRunOptions::default(),
        )
        .await
        .expect("run --dag should succeed");

        // Open the resulting database and assert all three conditions.
        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();

        // (1) Seed loaded its 2 data rows.
        let seed_rows = guard
            .execute_sql("SELECT COUNT(*) FROM proj.seeds.countries")
            .unwrap();
        assert_eq!(cell_i64(&seed_rows.rows[0][0]), 2, "seed rows");

        // (2) Pre and post hooks both fired.
        let pre = guard
            .execute_sql("SELECT fired FROM proj.pre_marker")
            .unwrap();
        assert_eq!(cell_i64(&pre.rows[0][0]), 1, "pre_hook marker");
        let post = guard
            .execute_sql("SELECT n FROM proj.seeds.post_marker")
            .unwrap();
        assert_eq!(cell_i64(&post.rows[0][0]), 2, "post_hook marker");

        // (3) The dependent model materialized against the seed's rows. If the
        // seed had not run first, its `FROM` would have failed — so a populated
        // table also proves the inferred seed→model ordering.
        let model_rows = guard
            .execute_sql("SELECT COUNT(*) FROM proj.silver.dim_country")
            .unwrap();
        assert_eq!(cell_i64(&model_rows.rows[0][0]), 2, "model rows");
    }

    #[tokio::test]
    async fn transformation_nodes_execute_only_their_model() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models = root.join("models");
        std::fs::create_dir(&models).unwrap();

        let db_path = root.join("proj.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter.local]\n\
                 type = \"duckdb\"\n\
                 path = \"{}\"\n\n\
                 [pipeline.silver]\n\
                 type = \"transformation\"\n\n\
                 [pipeline.silver.target]\n\
                 adapter = \"local\"\n\n\
                 [pipeline.silver.target.governance]\n\
                 auto_create_catalogs = true\n\
                 auto_create_schemas = true\n",
                db_path.display()
            ),
        )
        .unwrap();

        std::fs::write(
            models.join("a.sql"),
            "SELECT 1 AS id, TIMESTAMP '2026-01-01 00:00:00' AS ts\n",
        )
        .unwrap();
        std::fs::write(
            models.join("a.toml"),
            "name = \"a\"\n\n\
             [strategy]\n\
             type = \"incremental\"\n\
             timestamp_column = \"ts\"\n\n\
             [target]\n\
             catalog = \"proj\"\n\
             schema = \"silver\"\n\
             table = \"a\"\n",
        )
        .unwrap();

        std::fs::write(models.join("b.sql"), "SELECT id, ts FROM proj.silver.a\n").unwrap();
        std::fs::write(
            models.join("b.toml"),
            "name = \"b\"\n\
             depends_on = [\"a\"]\n\n\
             [strategy]\n\
             type = \"incremental\"\n\
             timestamp_column = \"ts\"\n\n\
             [target]\n\
             catalog = \"proj\"\n\
             schema = \"silver\"\n\
             table = \"b\"\n",
        )
        .unwrap();

        run_with_dag(
            &root.join("rocky.toml"),
            &root.join(".rocky-state.redb"),
            false,
            &crate::commands::run::SkipRunOptions::default(),
        )
        .await
        .expect("run --dag should succeed");

        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        for table in ["a", "b"] {
            let rows = guard
                .execute_sql(&format!("SELECT COUNT(*) FROM proj.silver.{table}"))
                .unwrap();
            assert_eq!(
                cell_i64(&rows.rows[0][0]),
                1,
                "{table} must materialize exactly once"
            );
        }
    }
}
