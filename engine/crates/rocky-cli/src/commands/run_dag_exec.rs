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

use super::run::{PartitionRunOptions, SkipRunOptions};
use crate::output::{DagRunNodeOutput, DagRunOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Runs one pipeline sub-run: `(config_path, loaded, state_path,
/// pipeline_name, model_name, partition_opts, skip_opts, shadow_config) ->
/// Result<(), String>`.
///
/// Injected into [`CliDispatcher`] so the outer run options flow to a single,
/// observable seam. Production wraps [`super::run::run`] with the DAG sub-run's
/// fixed arguments ([`default_sub_runner`]); tests substitute a recorder that
/// captures the exact [`PartitionRunOptions`], [`SkipRunOptions`],
/// `ShadowConfig` and `Arc<LoadedConfig>` the dispatch passes — so replacing
/// any of them with defaults, reloading config per node, or widening the
/// partition options back out to include `--parallel`, is caught.
type SubRunner = Arc<
    dyn Fn(
            PathBuf,
            Arc<rocky_core::config::LoadedConfig>,
            PathBuf,
            String,
            Option<String>,
            PartitionRunOptions,
            SkipRunOptions,
            Option<rocky_core::shadow::ShadowConfig>,
        ) -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// The production [`SubRunner`]: drives one pipeline through [`super::run::run`]
/// with the DAG sub-run's fixed arguments (no `--defer`/`--var`, config-derived
/// TTL, no idempotency key). Only `config`, `loaded`, `state`, `pipeline`,
/// `model_name`, `partition_opts`, and `skip_opts` vary per node — `loaded` is
/// always a clone of the ONE snapshot `run_with_dag` captured, never a per-node
/// reload, and transformation nodes supply their model name so each node
/// materializes only itself; other pipeline nodes pass `None`.
///
/// Because each transformation model runs as its own sub-run — with its own
/// [`super::skip_gate::SkipGate`] instance — `--skip-unchanged` cannot observe a
/// sibling model's this-run build/skip verdict across nodes. A model with a
/// Rocky-model upstream therefore always rebuilds under `--dag --skip-unchanged`:
/// the gate treats an upstream carrying no in-run verdict as changed (see
/// `SkipGate::upstream_unchanged`). That is fail-safe — never a stale skip — but
/// more conservative than a single monolithic run, where the per-layer barrier
/// makes every upstream verdict visible. Raw-source freshness skips still apply.
fn default_sub_runner() -> SubRunner {
    Arc::new(
        |config_path: PathBuf,
         loaded: Arc<rocky_core::config::LoadedConfig>,
         state_path: PathBuf,
         pipeline_name: String,
         model_name: Option<String>,
         partition_opts,
         skip_opts,
         shadow_config: Option<rocky_core::shadow::ShadowConfig>| {
            Box::pin(async move {
                let models_dir = models_dir_for_model_scope(
                    &config_path,
                    &loaded.config,
                    &pipeline_name,
                    model_name.as_deref(),
                )
                .map_err(|e| format!("{e:#}"))?;
                super::run::run(
                    &config_path,
                    loaded,
                    None,
                    Some(&pipeline_name),
                    &state_path,
                    None,
                    false, // json — sub-runs print to stdout if not silenced
                    models_dir.as_deref(),
                    false,
                    None,
                    false,
                    shadow_config.as_ref(),
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
                    // `--assume-fresh-state` is not surfaced on the DAG path.
                    false,
                )
                .await
                .map_err(|e| e.to_string())
            })
        },
    )
}

/// Returns the owning pipeline's models directory, only for a model-scoped
/// transformation sub-run. Supplying it to replication/load sub-runs makes
/// `run()` execute every transformation model after the pipeline itself.
///
/// This must resolve the pipeline's CONFIGURED directory, not a hardcoded
/// `models`. The DAG builds a node for every model it discovered, then hands
/// each node's model name to a sub-run along with the directory to find it in —
/// so a pipeline declaring `models = "transforms/**"` built a correct node whose
/// sub-run then died with `models directory '<config>/models' not found
/// (required for --model)`. Discovery and execution have to agree, which is why
/// both go through [`crate::models_loader::resolve_models_dir`].
fn models_dir_for_model_scope(
    config_path: &Path,
    cfg: &rocky_core::config::RockyConfig,
    pipeline_name: &str,
    model_name: Option<&str>,
) -> Result<Option<PathBuf>> {
    use rocky_core::config::PipelineConfig;

    if model_name.is_none() {
        return Ok(None);
    }
    let Some(PipelineConfig::Transformation(t)) = cfg.pipelines.get(pipeline_name) else {
        return Ok(None);
    };
    crate::models_loader::resolve_models_dir(&t.models, config_path)
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
    // The caller's time-interval partition options. Every sub-run must receive
    // the same *selection*; otherwise an explicit historical partition silently
    // degrades to `Latest` (#1283). The `parallel` field is deliberately not
    // honored here — see [`sub_run_partition_opts`].
    partition_opts: &PartitionRunOptions,
    // Build-escape-hatch overlay (`--force-rebuild` / `--no-reuse`). Threaded
    // into every sub-run so `rocky run --dag --force-rebuild` actually forces a
    // build (and `--no-reuse` disables content-addressed reuse + column-skip)
    // instead of being silently dropped at the DAG boundary.
    skip_opts: &super::run::SkipRunOptions,
    // `--shadow` / `--branch`, threaded into every sub-run. Dropping it at the
    // DAG boundary is what let `rocky run --dag --shadow` write production
    // targets (#1272). Each sub-run is an ordinary `run()`, so it gets whatever
    // isolation that pipeline kind supports — and the kinds that support none
    // now refuse rather than write production.
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
) -> Result<()> {
    // Under `-o json` the orchestrator contract is that stdout is exactly one
    // JSON document (the `DagRunOutput` below). Sub-runs are dispatched with
    // `json = false` so they don't each emit their own JSON payload, which
    // means they take their human-summary branch — route those lines to stderr
    // so they can't precede the JSON on stdout. See `crate::status_line!`.
    if json {
        crate::output::reserve_stdout_for_json();
    }

    // ONE fingerprinted config snapshot for the WHOLE DAG (#1120): the DAG
    // build below and every per-node sub-run execute this same instance
    // (`Arc::clone` per node in the dispatcher), so a `rocky.toml` swap
    // mid-DAG cannot make later nodes execute a different config than the
    // one the DAG was built from.
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path)
            .with_context(|| format!("failed to load config from {}", config_path.display()))?,
    );
    let cfg = &loaded.config;

    // Load models from the model set each transformation pipeline actually
    // declares — NOT a hardcoded `<config_dir>/models`. Hardcoding it was wrong
    // in both directions: a pipeline with `models = "transforms/**"` loaded
    // nothing, built zero transformation nodes, and reported success; and a
    // project with no transformation pipeline at all was still forced to
    // validate a `models/` directory that only transformation pipelines
    // consume (`add_transformation_nodes`), so an unrelated broken model there
    // failed a replication-only run that `rocky run` executes happily.
    let models = load_transformation_models(config_path, cfg)?;

    // Seed-discovery errors are NOT recoverable into "no seeds": seed nodes and
    // the seed→model edges that order a model after the seed it reads are built
    // only from this list. Swallowing a malformed seed sidecar drops the seed
    // node, leaves the model unordered, and lets it run against whatever the
    // previous run left in the table — a green DAG over stale data.
    let seeds_dir = config_path.parent().unwrap_or(Path::new(".")).join("seeds");
    let seeds = if seeds_dir.is_dir() {
        rocky_core::seeds::discover_seeds(&seeds_dir)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to discover seeds in {}", seeds_dir.display()))?
    } else {
        Vec::new()
    };

    let mut dag = unified_dag::build_unified_dag(cfg, &models, &seeds)
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

    // `--dag` cannot isolate a run, so it refuses to pretend it can.
    //
    // #1272 was filed because `--dag --shadow` silently wrote production. The
    // obvious repair — thread `shadow_config` into every sub-run — is necessary
    // but NOT sufficient, and shipping it alone would have replaced a visible
    // wrong with an invisible one. Four independent reasons, each verified:
    //
    // 1. **Cross-model reads are not routed.** The DAG dispatches each
    //    transformation model as its own ONE-model sub-run, so
    //    `apply_shadow_rewrite` runs with `model_name_filter = Some(this
    //    model)`. Its rename set is every OTHER routed model, and with a single
    //    routed model that set is empty — nothing is rewritten. For `a -> b`,
    //    `b_shadow` is therefore built by reading PRODUCTION `a`. Measured: with
    //    `a` changed to emit 2, a shadow run yields `a_shadow = 2` and
    //    `b_shadow = 1`, and exits 0. That is a false green — the operator then
    //    compares shadow against production and sees agreement that the run
    //    never established.
    // 2. **Seeds are not routed.** A seed node dispatches to `seed::run_seed`,
    //    which takes no shadow config and DROPs, recreates and repopulates its
    //    CONFIGURED target.
    // 3. **Replication suffix mode corrupts the SOURCE.** `TableTask` carries one
    //    `table_name` for both sides and `run()` stores the SUFFIXED name in it,
    //    so the copy reads `<source_schema>.<table>_rocky_shadow`.
    // 4. **Snapshot and load are unrouted** and already refuse inside `run()`.
    //
    // Refusing whole rather than carving out the narrow survivors (a
    // single-model DAG; replication under `schema_override`) is deliberate:
    // every carve-out is another surface for exactly this class of defect, and
    // this path has now produced four of them. Lift the refusal when the DAG
    // executes its transformation models as one shadow-aware unit — at which
    // point the per-node `shadow_config` threading below is what carries it.
    // Tracked as #1279.
    if shadow_config.is_some() {
        anyhow::bail!(
            "--shadow / --branch is not supported by `rocky run --dag`: the DAG runs each model \
             as its own sub-run, so a model's reads of an upstream built by this same run are \
             NOT redirected to that upstream's shadow target — the downstream shadow table would \
             be built from production data and the run would still report success. Seed, \
             snapshot and load nodes are not routed at all, and replication's suffix mode \
             rewrites the source it reads. Run the shadow pipeline without `--dag` (a single \
             `rocky run --shadow` routes every selected model and rewrites the reads between \
             them), or run the DAG without the flag"
        );
    }

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
        loaded: std::sync::Arc::clone(&loaded),
        state_path: state_path.to_path_buf(),
        seeds_dir,
        node_pipelines,
        partition_opts: partition_opts.clone(),
        skip_opts: *skip_opts,
        shadow_config: shadow_config.cloned(),
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

/// Load the union of every transformation pipeline's declared model set.
///
/// Only transformation pipelines consume models, so a project without one loads
/// nothing and can never fail on a `models/` directory it would not have
/// executed. The base directory is derived from each pipeline's `models` glob
/// exactly the way [`super::run`] derives it (the prefix up to the first `**`),
/// so `--dag` and a plain run agree on which files are in scope.
///
/// A missing directory is not an error here — that matches `run`, which treats
/// an absent models directory as a no-op rather than a failure.
///
/// Directories are deduplicated by canonical path, so two pipelines that write
/// the same directory differently (`models/**` and `./models/**`) load it once
/// instead of twice.
///
/// A model name reached from more than one place is an ERROR, not a silent
/// pick-the-first. `build_unified_dag` keys a transformation node by model name
/// alone, so two models sharing a name cannot both be built — today that
/// surfaces from the executor as `circular dependency detected involving: []`
/// (#1261), which names nothing. Failing here names both files instead.
///
/// Per-pipeline attribution is deliberately not attempted: `build_unified_dag`
/// takes one flat model list and gives every transformation pipeline the same
/// nodes, which is the other half of #1261.
fn load_transformation_models(
    config_path: &Path,
    cfg: &rocky_core::config::RockyConfig,
) -> Result<Vec<rocky_core::models::Model>> {
    use rocky_core::config::PipelineConfig;

    // (canonical key, path as configured) — the key only deduplicates, the
    // configured path is what error messages should show.
    let mut dirs: Vec<(PathBuf, PathBuf)> = Vec::new();
    for pipeline in cfg.pipelines.values() {
        let PipelineConfig::Transformation(t) = pipeline else {
            continue;
        };
        let Some(dir) = crate::models_loader::resolve_models_dir(&t.models, config_path)? else {
            continue;
        };
        let key = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        if !dirs.iter().any(|(seen_key, _)| seen_key == &key) {
            dirs.push((key, dir));
        }
    }

    let mut models: Vec<rocky_core::models::Model> = Vec::new();
    let mut by_name: HashMap<String, String> = HashMap::new();
    for (_, dir) in dirs {
        for model in super::dag::load_all_models(&dir)? {
            if let Some(first) = by_name.get(&model.config.name) {
                anyhow::bail!(
                    "duplicate model name '{}': declared in both {} and {}. A \
                     transformation node is keyed by model name alone, so two \
                     models sharing a name cannot both be built — rename one.",
                    model.config.name,
                    first,
                    model.file_path,
                );
            }
            by_name.insert(model.config.name.clone(), model.file_path.clone());
            models.push(model);
        }
    }
    models.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(models)
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
    /// The ONE fingerprinted config snapshot captured by `run_with_dag`.
    /// Every pipeline-bound sub-run receives an `Arc::clone` of this same
    /// instance — never a per-node reload — so a `rocky.toml` swap mid-DAG
    /// cannot redirect later nodes (#1120).
    loaded: Arc<rocky_core::config::LoadedConfig>,
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
    /// The outer DAG invocation's time-interval partition options, exactly as
    /// the caller passed them. [`Self::dispatch`] narrows them through
    /// [`sub_run_partition_opts`] on the way to each sub-run — the selection
    /// travels, the concurrency does not.
    partition_opts: PartitionRunOptions,
    /// The `--force-rebuild` / `--no-reuse` build-escape-hatch overlay, threaded
    /// from the outer `run_with_dag` so each sub-run honors it. `Copy`, so the
    /// per-node closure captures a value (no borrow escaping the dispatcher).
    skip_opts: super::run::SkipRunOptions,
    /// `--shadow` / `--branch`, threaded from the outer `run_with_dag` so each
    /// sub-run honors it. Dropping it here is what let `rocky run --dag
    /// --shadow` write production targets (#1272).
    shadow_config: Option<rocky_core::shadow::ShadowConfig>,
    /// The injected sub-run driver. Production is [`default_sub_runner`]; tests
    /// substitute a recorder to observe the `skip_opts` each sub-run receives.
    sub_runner: SubRunner,
}

/// The partition options one DAG sub-run receives: the caller's **selection**,
/// with concurrency pinned to 1.
///
/// The two halves of [`PartitionRunOptions`] answer different questions and the
/// DAG owns only one of them.
///
/// `--partition` / `--from`/`--to` / `--latest` / `--missing` / `--lookback`
/// choose *which* partitions to build. They must reach every sub-run: dropping
/// them is what made `rocky run --dag --partition 2020-01-01` rebuild the
/// latest partition and exit 0 (#1283).
///
/// `--parallel` chooses *how many* warehouse queries run at once, and the DAG
/// already owns that dimension — it dispatches every node in a layer
/// concurrently. Honoring the flag per sub-run on top of that would give
/// node-fan-out × per-partition-fan-out, and `run_with_dag` builds its
/// [`DagExecutor`] with no `max_concurrency`, so the left-hand factor is
/// unbounded. `rocky run` refuses that multiplication by construction:
/// `super::run::execute_models` excludes `time_interval` from concurrent model
/// execution precisely because it "already self-parallelizes per-partition".
/// The DAG must not reintroduce what the non-DAG path is careful to avoid, so
/// per-sub-run concurrency stays at 1 — byte-identical to the value the DAG
/// passed before any selection was threaded.
///
/// Giving `--parallel` a single meaning on both paths — by bounding node
/// fan-out here rather than partition fan-out below — is #1288.
fn sub_run_partition_opts(caller: &PartitionRunOptions) -> PartitionRunOptions {
    // Destructured field-by-field rather than `..caller.clone()` ON PURPOSE.
    // Struct-update syntax would silently thread any field added later, which
    // is right for another selection knob and wrong for another concurrency or
    // resource knob — exactly the mistake this function exists to prevent. The
    // exhaustive pattern stops compiling instead, so the next field has to pick
    // a side deliberately.
    let PartitionRunOptions {
        partition,
        from,
        to,
        latest,
        missing,
        lookback,
        parallel: _,
    } = caller;
    PartitionRunOptions {
        partition: partition.clone(),
        from: from.clone(),
        to: to.clone(),
        latest: *latest,
        missing: *missing,
        lookback: *lookback,
        parallel: 1,
    }
}

impl NodeDispatcher for CliDispatcher {
    fn dispatch(&self, id: &NodeId, kind: NodeKind, label: &str) -> Option<NodeFuture> {
        let config_path = self.config_path.clone();
        // `Arc::clone`, NOT a reload: every node executes the one snapshot.
        let loaded = Arc::clone(&self.loaded);
        let state_path = self.state_path.clone();
        // The narrowing happens HERE, at the one seam every sub-run crosses,
        // rather than where the dispatcher is built — so no construction path
        // can hand a sub-run the caller's `--parallel`.
        let partition_opts = sub_run_partition_opts(&self.partition_opts);
        let skip_opts = self.skip_opts;
        let shadow_config = self.shadow_config.clone();
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
                // filter selects exactly this seed. The dispatcher's captured
                // snapshot is threaded in — `run_seed` performs no config
                // re-read (#1120), so a `rocky.toml` swap mid-DAG cannot make
                // a warehouse-mutating seed node execute config B while the
                // rest of the DAG runs A.
                let seeds_dir = self.seeds_dir.clone();
                Some(Box::pin(async move {
                    super::seed::run_seed(&loaded, &seeds_dir, None, Some(&label), false)
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
                // partition selection and the build-escape-hatch options so
                // they are honored per sub-run rather than dropped at the DAG
                // boundary.
                Some(Box::pin(async move {
                    (sub_runner)(
                        config_path,
                        loaded,
                        state_path,
                        pipeline_name,
                        model_name,
                        partition_opts,
                        skip_opts,
                        shadow_config,
                    )
                    .await
                }))
            }
        }
    }
}

#[cfg(test)]
mod run_opts_threading_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use rocky_core::config::LoadedConfig;
    use rocky_core::dag_executor::NodeDispatcher;
    use rocky_core::unified_dag::{NodeId, NodeKind};

    use super::super::run::{PartitionRunOptions, SkipRunOptions};
    use super::{CliDispatcher, SubRunner, models_dir_for_model_scope};

    type RecordedSubRun = (Option<String>, PartitionRunOptions, SkipRunOptions);

    const DEFAULT_GLOB_CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "p.duckdb"

[pipeline.silver]
type = "transformation"

[pipeline.silver.target]
adapter = "default"
"#;

    const CUSTOM_GLOB_CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "p.duckdb"

[pipeline.ingest]
strategy = "full_refresh"
timestamp_column = "_updated_at"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "c"
schema_template = "s"

[pipeline.silver]
type = "transformation"
models = "transforms/**"

[pipeline.silver.target]
adapter = "default"
"#;

    /// Write a project on disk whose `rocky.toml` holds `toml_body`, creating
    /// each named directory. `resolve_models_dir` returns `None` for a directory
    /// that does not exist, so these have to be real paths.
    fn project_with(toml_body: &str, dirs: &[&str]) -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("rocky.toml");
        std::fs::write(&path, toml_body).unwrap();
        for d in dirs {
            std::fs::create_dir_all(tmp.path().join(d)).unwrap();
        }
        (tmp, path)
    }

    #[test]
    fn models_dir_is_only_set_for_model_scoped_sub_runs() {
        let (tmp, config_path) = project_with(DEFAULT_GLOB_CONFIG, &["models"]);
        let loaded =
            Arc::new(rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap());
        let cfg = &loaded.config;

        assert_eq!(
            models_dir_for_model_scope(&config_path, cfg, "silver", Some("dim_orders")).unwrap(),
            Some(tmp.path().join("models"))
        );
        // No model name means the whole pipeline runs, and `run()` must not be
        // handed a models directory for that.
        assert_eq!(
            models_dir_for_model_scope(&config_path, cfg, "silver", None).unwrap(),
            None
        );
    }

    /// The sub-run must be pointed at the pipeline's CONFIGURED directory.
    /// Handing it a hardcoded `models` made a correctly-discovered node die with
    /// "models directory not found (required for --model)".
    #[test]
    fn models_dir_follows_the_pipelines_configured_glob() {
        let (tmp, config_path) = project_with(CUSTOM_GLOB_CONFIG, &["transforms", "models"]);
        let loaded =
            Arc::new(rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap());
        let cfg = &loaded.config;

        // `transforms`, not the `models` directory that also exists beside it.
        assert_eq!(
            models_dir_for_model_scope(&config_path, cfg, "silver", Some("dim_orders")).unwrap(),
            Some(tmp.path().join("transforms"))
        );
        // A replication pipeline owns no models, so nothing is supplied even if a
        // model name somehow arrives — that would make `run()` execute every
        // transformation model after the replication itself.
        assert_eq!(
            models_dir_for_model_scope(&config_path, cfg, "ingest", Some("dim_orders")).unwrap(),
            None
        );
        // An unknown pipeline resolves to nothing rather than a guessed default.
        assert_eq!(
            models_dir_for_model_scope(&config_path, cfg, "nope", Some("dim_orders")).unwrap(),
            None
        );
    }

    /// A loaded snapshot for dispatcher tests, built through the real
    /// fingerprinted loader over a minimal temp config.
    fn test_loaded_config() -> Arc<LoadedConfig> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(
            &path,
            "[adapter.db]\ntype = \"duckdb\"\npath = \"wh.duckdb\"\n",
        )
        .unwrap();
        Arc::new(rocky_core::config::load_rocky_config_fingerprinted(&path).unwrap())
    }

    /// Build a dispatcher over `n` transformation nodes wired to `recorder`,
    /// returning the dispatcher and the node ids in dispatch order.
    fn dispatcher_with_nodes(
        loaded: Arc<LoadedConfig>,
        partition_opts: PartitionRunOptions,
        skip_opts: SkipRunOptions,
        shadow_config: Option<rocky_core::shadow::ShadowConfig>,
        recorder: SubRunner,
        n: usize,
    ) -> (CliDispatcher, Vec<NodeId>) {
        let mut node_pipelines = HashMap::new();
        let node_ids: Vec<NodeId> = (0..n)
            .map(|i| {
                let id = NodeId::new("transformation", &format!("dim_orders_{i}"));
                node_pipelines.insert(id.clone(), "analytics".to_string());
                id
            })
            .collect();
        let dispatcher = CliDispatcher {
            config_path: std::path::PathBuf::from("rocky.toml"),
            loaded,
            state_path: std::path::PathBuf::from(".rocky-state.redb"),
            seeds_dir: std::path::PathBuf::from("seeds"),
            node_pipelines,
            partition_opts,
            skip_opts,
            shadow_config,
            sub_runner: recorder,
        };
        (dispatcher, node_ids)
    }

    /// The whole sub-run options contract in one test: the partition
    /// **selection** and the build escape hatches reach each sub-run verbatim,
    /// and `--parallel` does **not**.
    ///
    /// Three defects meet here. `SkipRunOptions::default()` used to replace the
    /// escape hatches. An empty `PartitionRunOptions` used to replace the
    /// selection, so `--partition <historical>` silently rebuilt the latest
    /// partition (#1283). And threading the options wholesale would carry
    /// `--parallel` — whose `rocky run` default is 4, against the 1 this
    /// boundary has always passed — into a fan-out the DAG does not bound
    /// (#1288, and see [`sub_run_partition_opts`]).
    ///
    /// Non-vacuous, and it guards the call site rather than a helper: the
    /// recorder captures what a real `dispatch()` hands the sub-runner, so
    /// reverting the narrowing to `self.partition_opts.clone()` fails on
    /// `parallel`, and dropping it to `default()` fails on the selection. The
    /// dispatcher is deliberately given `parallel: 3` — a value no assertion
    /// below expects to survive. (The historical-partition behavior is proven
    /// end-to-end further down; the content-addressed escape-hatch behavior
    /// needs S3 and is exercised by the live sandbox.)
    #[tokio::test]
    async fn dispatch_passes_the_selection_but_not_the_concurrency_to_each_sub_run() {
        let recorded: Arc<Mutex<Vec<RecordedSubRun>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = recorded.clone();
        // A recorder sub-runner: capture the skip_opts and return Ok without
        // running a real pipeline.
        let recorder: SubRunner = Arc::new(
            move |_config,
                  _loaded,
                  _state,
                  _pipeline,
                  model_name,
                  partition_opts,
                  skip_opts,
                  _shadow| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock()
                        .unwrap()
                        .push((model_name, partition_opts, skip_opts));
                    Ok(())
                })
            },
        );

        let skip_opts = SkipRunOptions {
            skip_unchanged: false,
            force_rebuild: true,
            no_reuse: true,
            no_prune: false,
        };
        let partition_opts = PartitionRunOptions {
            partition: Some("2020-01-02".into()),
            from: Some("2020-01-01".into()),
            to: Some("2020-01-03".into()),
            latest: true,
            missing: true,
            lookback: Some(2),
            parallel: 3,
        };
        let (dispatcher, node_ids) = dispatcher_with_nodes(
            test_loaded_config(),
            partition_opts,
            skip_opts,
            None,
            recorder,
            1,
        );

        // Drive a real pipeline-node dispatch through the production path.
        let fut = dispatcher
            .dispatch(&node_ids[0], NodeKind::Transformation, "dim_orders_0")
            .expect("a pipeline node dispatches a future");
        fut.await.expect("recorder sub-runner returns Ok");

        let got = recorded.lock().unwrap();
        assert_eq!(got.len(), 1, "exactly one sub-run was dispatched");
        assert_eq!(
            got[0].0.as_deref(),
            Some("dim_orders_0"),
            "a transformation node's dispatched label is its model scope"
        );
        let opts = &got[0].1;
        // Every selection field survives verbatim — this is #1283.
        assert_eq!(opts.partition.as_deref(), Some("2020-01-02"));
        assert_eq!(opts.from.as_deref(), Some("2020-01-01"));
        assert_eq!(opts.to.as_deref(), Some("2020-01-03"));
        assert!(opts.latest);
        assert!(opts.missing);
        assert_eq!(opts.lookback, Some(2));
        // ...and concurrency does not. The dispatcher holds 3; the sub-run
        // must see 1, because the DAG already fans out per node and does not
        // cap that fan-out. Threading it wholesale makes this 3.
        assert_eq!(
            opts.parallel, 1,
            "--parallel must NOT reach the sub-run: the DAG owns node fan-out \
             and does not bound it, so honoring it here multiplies (#1288)"
        );
        assert!(
            got[0].2.force_rebuild,
            "--force-rebuild must reach the sub-run, not be dropped to default()"
        );
        assert!(
            got[0].2.no_reuse,
            "--no-reuse must reach the sub-run, not be dropped to default()"
        );
    }

    /// #1272 regression: `rocky run --dag --shadow` / `--branch` must reach
    /// EVERY sub-run. Pre-fix, `default_sub_runner` passed a hardcoded `None`
    /// into `run()`'s `shadow_config` slot, so the flag was silently dropped at
    /// the DAG boundary and every node wrote its PRODUCTION target — the run
    /// reporting success the whole time.
    ///
    /// Non-vacuous, and deliberately mirroring
    /// [`dispatch_passes_the_threaded_skip_opts_to_each_sub_run`], which exists
    /// to catch a silent drop at this same boundary: a recording [`SubRunner`]
    /// captures the EXACT `ShadowConfig` each dispatch passes. Reverting the
    /// dispatch argument to `None` records `None` and fails the first assertion.
    ///
    /// The values asserted are deliberately NOT `ShadowConfig::default()` — a
    /// revert that substituted a default-constructed config rather than `None`
    /// would still fail on the suffix and the schema override. Three nodes,
    /// because the drop this guards against was per-node: one node proves the
    /// argument is wired, three prove no node is served a different value.
    ///
    /// (The behavioural end-to-end proof — a `--dag --shadow` run leaving
    /// production untouched — is the transformation arm's own coverage in
    /// `run.rs`; this test owns the boundary, which is where the defect was.)
    #[tokio::test]
    async fn dispatch_passes_the_threaded_shadow_config_to_each_sub_run() {
        type RecordedShadow = (Option<String>, Option<rocky_core::shadow::ShadowConfig>);
        let recorded: Arc<Mutex<Vec<RecordedShadow>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = recorded.clone();
        let recorder: SubRunner = Arc::new(
            move |_config,
                  _loaded,
                  _state,
                  _pipeline,
                  model_name,
                  _partition,
                  _skip_opts,
                  shadow| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().push((model_name, shadow));
                    Ok(())
                })
            },
        );

        let shadow_config = rocky_core::shadow::ShadowConfig {
            suffix: "_pr1272_shadow".to_string(),
            schema_override: Some("isolated_ns".to_string()),
            cleanup_after: false,
        };
        let (dispatcher, node_ids) = dispatcher_with_nodes(
            test_loaded_config(),
            PartitionRunOptions::default(),
            SkipRunOptions::default(),
            Some(shadow_config),
            recorder,
            3,
        );

        for (i, id) in node_ids.iter().enumerate() {
            let fut = dispatcher
                .dispatch(id, NodeKind::Transformation, &format!("dim_orders_{i}"))
                .expect("a pipeline node dispatches a future");
            fut.await.expect("recorder sub-runner returns Ok");
        }

        let got = recorded.lock().unwrap();
        assert_eq!(got.len(), 3, "every node dispatched a sub-run");
        for (i, (model_name, shadow)) in got.iter().enumerate() {
            let shadow = shadow.as_ref().unwrap_or_else(|| {
                panic!(
                    "node {i} ({model_name:?}) received no shadow config: `--shadow` / `--branch` \
                     was dropped at the DAG boundary, so this sub-run would write its production \
                     target"
                )
            });
            assert_eq!(
                shadow.suffix, "_pr1272_shadow",
                "node {i} must receive the threaded suffix, not a default-constructed config"
            );
            assert_eq!(
                shadow.schema_override.as_deref(),
                Some("isolated_ns"),
                "node {i} must receive the threaded schema override"
            );
        }
    }

    /// WP-01 PR-B (#1120): every DAG sub-run receives the SAME
    /// `Arc<LoadedConfig>` instance — an `Arc::clone` of the one snapshot
    /// `run_with_dag` captured, never a per-node reload.
    ///
    /// Non-vacuous: the recorder captures the exact `Arc` each dispatch
    /// passes; `Arc::ptr_eq` fails if the dispatch is ever reverted to
    /// loading (or rebuilding) a config per node, even if the contents were
    /// equal.
    #[tokio::test]
    async fn dispatch_passes_the_same_loaded_config_arc_to_every_sub_run() {
        let recorded: Arc<Mutex<Vec<Arc<LoadedConfig>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = recorded.clone();
        let recorder: SubRunner = Arc::new(
            move |_config, loaded, _state, _pipeline, _model, _partition, _skip, _shadow| {
                let sink = sink.clone();
                Box::pin(async move {
                    sink.lock().unwrap().push(loaded);
                    Ok(())
                })
            },
        );

        let loaded = test_loaded_config();
        let (dispatcher, node_ids) = dispatcher_with_nodes(
            Arc::clone(&loaded),
            PartitionRunOptions::default(),
            SkipRunOptions::default(),
            None,
            recorder,
            3,
        );

        for (i, id) in node_ids.iter().enumerate() {
            let fut = dispatcher
                .dispatch(id, NodeKind::Transformation, &format!("dim_orders_{i}"))
                .expect("a pipeline node dispatches a future");
            fut.await.expect("recorder sub-runner returns Ok");
        }

        let got = recorded.lock().unwrap();
        assert_eq!(got.len(), 3, "all three sub-runs were dispatched");
        for (i, received) in got.iter().enumerate() {
            assert!(
                Arc::ptr_eq(received, &loaded),
                "sub-run {i} must receive the dispatcher's own Arc (one snapshot per DAG), \
                 not a reloaded/rebuilt config"
            );
        }
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

    /// Red-team (#1120, seed leg): a `Seed` node dispatch executes the
    /// dispatcher's CAPTURED config snapshot, not a fresh `rocky.toml` read.
    /// The drill: capture the snapshot (adapter → warehouse A), swap
    /// `rocky.toml` to point at warehouse B, then dispatch the seed node —
    /// the rows must land in A and B must never be created. Pre-fix,
    /// `run_seed` took the config PATH and reloaded it internally, so the
    /// warehouse-mutating seed node executed config B while the rest of the
    /// DAG ran A. (The signature change — `run_seed(&LoadedConfig, …)` —
    /// enforces "no config read" at compile time; this proves the dispatch
    /// threads the right instance.)
    #[tokio::test]
    async fn seed_dispatch_executes_the_captured_snapshot_not_a_reload() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("a")).unwrap();
        std::fs::create_dir_all(root.join("b")).unwrap();
        std::fs::create_dir_all(root.join("seeds")).unwrap();
        std::fs::write(
            root.join("seeds/countries.csv"),
            "code,name\nUS,United States\nGB,United Kingdom\n",
        )
        .unwrap();
        std::fs::write(
            root.join("seeds/countries.toml"),
            "name = \"countries\"\n\n\
             [target]\n\
             catalog = \"wh\"\n\
             schema = \"seeds\"\n\
             table = \"countries\"\n",
        )
        .unwrap();

        // Both configs use the SAME catalog name (`wh` — the DuckDB file
        // stem), so the seed SQL is valid under either; only the physical
        // warehouse file differs. Whichever config executes decides where
        // the rows land.
        let config_path = root.join("rocky.toml");
        let db_a = root.join("a/wh.duckdb");
        let db_b = root.join("b/wh.duckdb");
        let config_for = |db: &std::path::Path| {
            format!(
                "[adapter.local]\n\
                 type = \"duckdb\"\n\
                 path = \"{}\"\n\n\
                 [pipeline.silver]\n\
                 type = \"transformation\"\n\n\
                 [pipeline.silver.target]\n\
                 adapter = \"local\"\n",
                db.display()
            )
        };
        std::fs::write(&config_path, config_for(&db_a)).unwrap();
        let loaded = Arc::new(
            rocky_core::config::load_rocky_config_fingerprinted(&config_path)
                .expect("load snapshot A"),
        );
        // The mid-DAG swap: after the capture, rocky.toml points at B.
        std::fs::write(&config_path, config_for(&db_b)).unwrap();

        let dispatcher = CliDispatcher {
            config_path: config_path.clone(),
            loaded,
            state_path: root.join(".rocky-state.redb"),
            seeds_dir: root.join("seeds"),
            node_pipelines: HashMap::new(),
            partition_opts: PartitionRunOptions::default(),
            skip_opts: SkipRunOptions::default(),
            shadow_config: None,
            sub_runner: default_sub_runner(),
        };
        let id = NodeId::new("seed", "countries");
        let fut = dispatcher
            .dispatch(&id, NodeKind::Seed, "countries")
            .expect("a seed node dispatches a future");
        fut.await.expect("seed node loads");

        assert!(
            !db_b.exists(),
            "the seed must never touch the swapped-in config's warehouse (B)"
        );
        let adapter = DuckDbWarehouseAdapter::open(&db_a).expect("warehouse A exists");
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let rows = guard
            .execute_sql("SELECT COUNT(*) FROM wh.seeds.countries")
            .unwrap();
        assert_eq!(
            cell_i64(&rows.rows[0][0]),
            2,
            "the seed's rows must land in the CAPTURED snapshot's warehouse (A)"
        );
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
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
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

    /// #1272 sentinel: `rocky run --dag --shadow` must not touch a production
    /// seed table.
    ///
    /// A seed node dispatches to `run_seed`, which DROPs and repopulates the
    /// seed's CONFIGURED target and accepts no shadow config — so before the
    /// refusal, a shadow DAG isolated the models and destroyed the seed tables
    /// beside them, exit 0.
    ///
    /// Non-vacuous by construction, and deliberately NOT resting on the error
    /// string: the CSV grows to three rows between the two runs, so a seed node
    /// that executed would leave three rows behind. Asserting the production
    /// table still holds the original two proves the seed did not run, rather
    /// than proving an error was formatted. Deleting the refusal makes the row
    /// count 3 and fails it.
    #[tokio::test]
    async fn shadow_dag_refuses_seeds_and_leaves_the_production_seed_table_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
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
            root.join("seeds/countries.toml"),
            "name = \"countries\"\n\n\
             [target]\n\
             catalog = \"proj\"\n\
             schema = \"seeds\"\n\
             table = \"countries\"\n",
        )
        .unwrap();
        std::fs::write(
            root.join("seeds/countries.csv"),
            "code,name\nUS,United States\nGB,United Kingdom\n",
        )
        .unwrap();

        let config_path = root.join("rocky.toml");
        let state_path = root.join(".rocky-state.redb");

        // Establish the production seed table: two rows.
        run_with_dag(
            &config_path,
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
        )
        .await
        .expect("the non-shadow DAG seeds production");

        // Grow the CSV. A seed node that runs now would leave three rows.
        std::fs::write(
            root.join("seeds/countries.csv"),
            "code,name\nUS,United States\nGB,United Kingdom\nFR,France\n",
        )
        .unwrap();

        let shadow_config = rocky_core::shadow::ShadowConfig {
            suffix: "_rocky_shadow".to_string(),
            schema_override: None,
            cleanup_after: false,
        };
        let err = run_with_dag(
            &config_path,
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            Some(&shadow_config),
        )
        .await
        .expect_err("a shadow DAG containing a seed must be refused");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--shadow / --branch is not supported by `rocky run --dag`"),
            "the DAG refuses shadow before executing anything: {msg}"
        );

        // The sentinel: production still holds the ORIGINAL two rows, so the
        // seed node never executed.
        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let rows = guard
            .execute_sql("SELECT COUNT(*) FROM proj.seeds.countries")
            .unwrap();
        assert_eq!(
            cell_i64(&rows.rows[0][0]),
            2,
            "the shadow run must not have repopulated the production seed table"
        );
    }

    /// #1272: `rocky run --dag --shadow` must refuse rather than build a
    /// downstream shadow table from PRODUCTION data.
    ///
    /// This is the measurement that decided the refusal. `b` reads `a`; the DAG
    /// dispatches each as its own one-model sub-run, so `apply_shadow_rewrite`
    /// sees a single routed model and its rename set — every OTHER routed model
    /// — is empty. Nothing in `b`'s SQL is rewritten.
    ///
    /// Before the refusal this test recorded, with `a` changed to emit 2:
    ///   proj.silver.a = 1, proj.silver.b = 1        (production, untouched)
    ///   proj.silver.a_shadow = 2                    (isolated correctly)
    ///   proj.silver.b_shadow = 1                    <-- read PRODUCTION a
    /// and `run_with_dag` returned Ok. A false green: the operator compares
    /// shadow against production and sees an agreement the run never
    /// established.
    ///
    /// The assertion is on the ABSENCE of the shadow tables, not on the error
    /// text, so it fails if the refusal is ever lifted without the routing
    /// landing first — `b_shadow` reappearing with the stale value 1 is exactly
    /// the defect.
    #[tokio::test]
    async fn shadow_dag_refuses_rather_than_build_a_downstream_from_production() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models")).unwrap();
        let db_path = root.join("proj.duckdb");
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter.local]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.silver]\ntype = \"transformation\"\n\n\
                 [pipeline.silver.target]\nadapter = \"local\"\n\n\
                 [pipeline.silver.target.governance]\n\
                 auto_create_catalogs = true\nauto_create_schemas = true\n",
                db_path.display()
            ),
        )
        .unwrap();
        std::fs::write(root.join("models/a.sql"), "SELECT 1 AS v\n").unwrap();
        std::fs::write(
            root.join("models/a.toml"),
            "name = \"a\"\n\n[target]\ncatalog = \"proj\"\nschema = \"silver\"\ntable = \"a\"\n",
        )
        .unwrap();
        std::fs::write(root.join("models/b.sql"), "SELECT v FROM proj.silver.a\n").unwrap();
        std::fs::write(
            root.join("models/b.toml"),
            "name = \"b\"\n\n[target]\ncatalog = \"proj\"\nschema = \"silver\"\ntable = \"b\"\n",
        )
        .unwrap();

        let config_path = root.join("rocky.toml");
        let state_path = root.join(".rocky-state.redb");
        run_with_dag(
            &config_path,
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
        )
        .await
        .expect("the non-shadow DAG builds production");

        // Divergence: an isolated run would have to compute 2 all the way
        // through, so a downstream holding 1 proves it read production.
        std::fs::write(root.join("models/a.sql"), "SELECT 2 AS v\n").unwrap();

        let shadow_config = rocky_core::shadow::ShadowConfig {
            suffix: "_shadow".to_string(),
            schema_override: None,
            cleanup_after: false,
        };
        run_with_dag(
            &config_path,
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            Some(&shadow_config),
        )
        .await
        .expect_err("a shadow DAG must be refused, not silently mis-isolated");

        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        for table in ["proj.silver.a_shadow", "proj.silver.b_shadow"] {
            assert!(
                guard
                    .execute_sql(&format!("SELECT v FROM {table}"))
                    .is_err(),
                "{table} must not exist: the refusal fires before the executor runs"
            );
        }
        // Production is untouched by the refused run.
        let prod_b = guard.execute_sql("SELECT v FROM proj.silver.b").unwrap();
        assert_eq!(cell_i64(&prod_b.rows[0][0]), 1, "production b untouched");
    }

    #[tokio::test]
    async fn historical_partition_selection_reaches_dag_model_sub_run() {
        use rocky_core::traits::WarehouseAdapter;

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
            models.join("daily_orders.sql"),
            "SELECT CAST(order_at AS DATE) AS order_date, COUNT(*) AS order_count \
             FROM raw__orders.orders \
             WHERE order_at >= @start_date AND order_at < @end_date \
             GROUP BY 1\n",
        )
        .unwrap();
        std::fs::write(
            models.join("daily_orders.toml"),
            "depends_on = []\n\n\
             [[sources]]\ncatalog = \"\"\nschema = \"raw__orders\"\ntable = \"orders\"\n\n\
             [strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
             granularity = \"day\"\nfirst_partition = \"2020-01-01\"\nlookback = 0\n\n\
             [target]\ncatalog = \"proj\"\nschema = \"marts\"\ntable = \"daily_orders\"\n",
        )
        .unwrap();

        {
            let seed = DuckDbWarehouseAdapter::open(&db_path).unwrap();
            seed.execute_statement("CREATE SCHEMA raw__orders")
                .await
                .unwrap();
            seed.execute_statement(
                "CREATE TABLE raw__orders.orders AS \
                 SELECT TIMESTAMP '2020-01-01 12:00:00' AS order_at",
            )
            .await
            .unwrap();
        }

        let partition_opts = PartitionRunOptions {
            partition: Some("2020-01-01".into()),
            parallel: 1,
            ..Default::default()
        };
        run_with_dag(
            &root.join("rocky.toml"),
            &root.join(".rocky-state.redb"),
            false,
            &partition_opts,
            &SkipRunOptions::default(),
            None,
        )
        .await
        .expect("historical DAG partition run should succeed");

        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        let rows = guard
            .execute_sql(
                "SELECT CAST(order_date AS VARCHAR), order_count \
                 FROM proj.marts.daily_orders",
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 1, "the requested partition materializes");
        assert_eq!(rows.rows[0][0].as_str(), Some("2020-01-01"));
        assert_eq!(cell_i64(&rows.rows[0][1]), 1);
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
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
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
