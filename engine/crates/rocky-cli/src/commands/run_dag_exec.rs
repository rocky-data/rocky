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

/// Grants each pipeline-bound sub-run an exclusive turn on its state file
/// (#1312).
///
/// Every sub-run drives [`super::run::run`], which holds the state store's
/// exclusive writer flock for its **whole duration**, and the executor
/// dispatches a layer's nodes concurrently, bounded only when `--parallel`
/// asks for it (#1288) and unbounded otherwise. Without a turnstile
/// the first sub-run to open the store wins and every same-layer sibling
/// sharing the state file dies on `LockHeldByOther` — deterministically, for a
/// project as small as two independent `SELECT 1` models. The flock-side retry
/// (#1234/#1314) cannot absorb this: its budget is milliseconds, and the
/// winner legitimately holds the lock for as long as its models take.
///
/// So sub-runs sharing a state file take turns instead of racing. The map is
/// keyed by the state path because *that* is the invariant — one writer per
/// state file — not "one sub-run at a time": nodes writing genuinely distinct
/// state files still overlap, and a test pins that so this can never quietly
/// become a global serializer. Today [`CliDispatcher`] threads one canonical
/// path to every pipeline-bound node, so in practice the map holds one entry
/// and the DAG's pipeline-bound nodes execute one at a time — which is
/// strictly better than what it replaces: before this gate, "concurrency"
/// between such nodes meant one ran and the rest crashed.
///
/// Seed nodes bypass the turnstile: [`super::seed::run_seed`] never opens the
/// state store, so serializing seeds would forfeit real parallelism for no
/// soundness gain.
struct StateTurnstile {
    turns: std::sync::Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl StateTurnstile {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            turns: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Wait for, and take, the exclusive turn for `path`. The turn is released
    /// when the returned guard drops — callers hold it across the whole
    /// sub-run, because that is exactly how long `run()` holds the flock.
    async fn turn(&self, path: &Path) -> tokio::sync::OwnedMutexGuard<()> {
        let gate = {
            let mut turns = self.turns.lock().expect("state turnstile poisoned");
            Arc::clone(turns.entry(path.to_path_buf()).or_default())
        };
        gate.lock_owned().await
    }
}

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
                super::run::run(
                    &config_path,
                    loaded,
                    None,
                    Some(&pipeline_name),
                    &state_path,
                    None,
                    false, // json — sub-runs print to stdout if not silenced
                    None,
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
                .map_err(|e| format!("{e:#}"))
            })
        },
    )
}

/// Execute `rocky run --dag`: run every pipeline in dependency order.
///
/// `state_path` is the canonical state location already resolved by the
/// caller (`main.rs` via `resolve_state_path_ns`, honoring `--state-path` /
/// `--state-namespace` / the `<models>/.rocky-state.redb` default). Each
/// per-pipeline sub-run is driven through `run()` against this same path, so
/// the unified-DAG path shares the project's canonical state with every other
/// `rocky run` invocation — it must never invent its own `.rocky_state` file.
// Eight parameters, one over clippy's threshold. Grouping them into a struct
// would be a wider change than #1288 warrants and would obscure the one
// property that matters here: `node_concurrency` is passed SEPARATELY from
// `partition_opts`, so a caller cannot accidentally hand over the
// default-folded value. `super::run::run` carries the same allow for the same
// reason.
#[allow(clippy::too_many_arguments)]
pub async fn run_with_dag(
    config_path: &Path,
    // The caller's ONE fingerprinted config snapshot (#1120). Taken as a
    // parameter rather than loaded here so a governed `rocky apply` of a
    // `--dag` plan executes the *same instance* its apply gate read and
    // fingerprinted: this function performs NO config load of its own, and a
    // `rocky.toml` swap between the gate and DAG execution can no longer make
    // the DAG run a different config than the one that was approved (#1289).
    //
    // Non-optional on purpose. An `Option` would leave a fallback load in
    // place, so a future caller could silently reopen the window; requiring
    // the snapshot enforces "no config read" at compile time, the same shape
    // `run_seed` / `run_load` adopted for #1120.
    loaded: std::sync::Arc<rocky_core::config::LoadedConfig>,
    state_path: &Path,
    json: bool,
    // The caller's time-interval partition options. Every sub-run must receive
    // the same *selection*; otherwise an explicit historical partition silently
    // degrades to `Latest` (#1283). The `parallel` field is deliberately not
    // honored *per sub-run* — see [`sub_run_partition_opts`]; it is honored
    // once, here, as a bound on node fan-out (`node_concurrency` below).
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
    // `--parallel` as the caller spelled it: `None` when the flag was not
    // passed. It bounds how many NODES execute at once.
    //
    // Deliberately NOT `partition_opts.parallel`, which has the non-`--dag`
    // default of 4 folded in and so cannot say whether a bound was requested.
    // Adopting that default here would cap a wide DAG that runs unbounded
    // today — a silent slowdown for a correctness fix, which is why #1288 was
    // split out of #1290 rather than ridden along with it. Unset therefore
    // keeps the historical unbounded fan-out.
    node_concurrency: Option<u32>,
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
    //
    // It now arrives from the caller (#1289) rather than being loaded here.
    // Loading it here made this the ONE apply path that re-read `rocky.toml`
    // after its gate had already read and fingerprinted it, so a swap in that
    // window executed a config the gate never approved while still reporting
    // success against the reviewed plan id.
    let cfg = &loaded.config;

    // Load models from the model set each transformation pipeline actually
    // declares — NOT a hardcoded `<config_dir>/models`. Hardcoding it was wrong
    // in both directions: a pipeline with `models = "transforms/**"` loaded
    // nothing, built zero transformation nodes, and reported success; and a
    // project with no transformation pipeline at all was still forced to
    // validate a `models/` directory that only transformation pipelines
    // consume (`add_transformation_nodes`), so an unrelated broken model there
    // failed a replication-only run that `rocky run` executes happily.
    let models_by_pipeline = load_transformation_models(config_path, cfg)?.by_pipeline;

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

    let mut dag = unified_dag::build_unified_dag(cfg, &models_by_pipeline, &seeds)
        .context("failed to build unified DAG")?;

    // Infer cross-step edges from each model's SQL `FROM` references so a
    // model that reads a seed (or replication load) is ordered *after* it,
    // even when no explicit `depends_on` is declared. Without this, a seed
    // and a model that selects from it both land in layer 0 and race.
    //
    // Flattened across pipelines: `build_unified_dag` has already refused any
    // model claimed by two of them, so a name appears at most once here.
    let sql_by_name: HashMap<String, String> = models_by_pipeline
        .values()
        .flatten()
        .map(|m| (m.config.name.clone(), m.sql.clone()))
        .collect();
    let label_report = unified_dag::infer_runtime_dependencies(&mut dag, &sql_by_name);

    // Physical-read ordering (#1275): the label heuristic above is blind to
    // configured `[target]`s — a model reading another model's physical
    // `schema.table` needs its edge derived from rendered target components,
    // or the two race in one executor phase. Shared derivation with the
    // plain-run layer computation; cycle-closing candidates are skipped
    // deterministically inside (the executor refuses cyclic graphs, and a
    // derived edge must never make a runnable project un-runnable).
    let physical_inputs: Vec<rocky_core::physical_edges::PhysicalEdgeModel<'_>> =
        models_by_pipeline
            .values()
            .flatten()
            .map(rocky_core::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
    let derived = unified_dag::infer_physical_dependencies(&mut dag, &physical_inputs);
    let mut physical_edge_warnings = label_report.warnings();
    physical_edge_warnings.extend(rocky_core::physical_edges::derivation_warnings(&derived));
    for w in &physical_edge_warnings {
        tracing::warn!("{w}");
    }
    // Same refusal the plain path applies, through the same function — "strict"
    // must not mean two different things depending on how the run started
    // (#1355). Placed before the executor is built, so a refused run dispatches
    // no nodes.
    super::run::refuse_on_scheduling_warnings(cfg.run.strict_scheduling, &physical_edge_warnings)?;

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
        state_turns: StateTurnstile::new(),
    };
    let executor = dag_executor_with_bound(dispatcher, node_concurrency);
    let result = executor
        .execute(&dag)
        .await
        .context("DAG execution failed")?;

    if json {
        let output = DagRunOutput {
            version: VERSION.into(),
            command: "run --dag".into(),
            warnings: physical_edge_warnings.clone(),
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

/// Load each transformation pipeline's own model set, keyed by pipeline name.
///
/// Only transformation pipelines consume models, so a project without one loads
/// nothing and can never fail on a `models/` directory it would not have
/// executed. Each pipeline's base directory and complete file glob are derived
/// from its `models` setting exactly the way [`super::run`] derives them, so
/// `--dag` and a plain run apply the same **glob** to the files they see.
///
/// They do **not** yet see the same files. This walk reads the base directory
/// plus one level of subdirectories, while a plain run compiles through
/// `Project::load_models_with_db`, which reads the base directory only — so a
/// `models/staging/` layout is a DAG node here and invisible to `rocky run`.
/// The glob is not what limits either: `models/**` matches at every depth.
/// Unifying the two traversals is #1262; this doc must not claim it is done.
///
/// A missing directory is not an error here — that matches `run`, which treats
/// an absent models directory as a no-op rather than a failure.
///
/// Attribution is per pipeline, which is the half of #1261 that
/// `build_unified_dag` needs: given one flat list it gave every transformation
/// pipeline the same nodes and the DAG collapsed into
/// `circular dependency detected involving: []`. Two pipelines pointed at the
/// SAME directory therefore both load it — deliberately not deduplicated across
/// pipelines, because that shared claim is exactly what `build_unified_dag` has
/// to see in order to refuse it by name.
///
/// A model name reached from more than one DIRECTORY is still an ERROR, not a
/// silent pick-the-first: a transformation node is keyed by model name alone, so
/// two distinct files sharing a name cannot both be built. Failing here names
/// both files. (Two pipelines sharing ONE directory is a different situation and
/// is diagnosed by `build_unified_dag` instead, which can name the pipelines.)
pub(super) fn load_transformation_models(
    config_path: &Path,
    cfg: &rocky_core::config::RockyConfig,
) -> Result<TransformationModels> {
    use rocky_core::config::PipelineConfig;

    let mut by_pipeline = rocky_core::unified_dag::ModelsByPipeline::new();
    // Distinct roots that actually yielded at least one model, and their
    // canonical forms for the distinctness test. Recorded during this single
    // pass rather than by a second walk of the config: a separate resolver pass
    // can disagree with this one whenever the filesystem changes underneath, and
    // a root that resolves but contributes nothing is not a root the lineage
    // compile needs to cover.
    let mut contributing_roots: Vec<PathBuf> = Vec::new();
    let mut canonical_roots: Vec<PathBuf> = Vec::new();
    // model name -> the canonical FILE it was first loaded from.
    //
    // Keyed on the file, not on the pipeline's base directory. Two pipelines
    // whose roots nest (`transforms/**` and `transforms/staging/**`) reach the
    // SAME file through different bases, and a base-keyed check called that a
    // duplicate — reporting "declared in both X and X", naming one path twice.
    // It is not a duplicate name; it is one model claimed by two pipelines,
    // which `build_unified_dag` reports properly because it can name them.
    let mut file_of_name: HashMap<String, (PathBuf, String)> = HashMap::new();

    for (pipeline_name, pipeline) in &cfg.pipelines {
        let PipelineConfig::Transformation(t) = pipeline else {
            continue;
        };
        let Some(dir) = crate::models_loader::resolve_models_dir(&t.models, config_path)? else {
            continue;
        };
        let models_glob = crate::models_loader::resolved_models_glob(&t.models, config_path);

        let mut models: Vec<rocky_core::models::Model> = Vec::new();
        // Names already seen inside THIS pipeline's own tree. The loader reads a
        // base directory plus one level below it, so one pipeline can reach the
        // same name twice (`transforms/orders` and `transforms/staging/orders`)
        // — that is a duplicate no matter how many pipelines exist.
        let mut seen_here: HashMap<String, String> = HashMap::new();
        for model in crate::models_loader::load_project_models_matching(&dir, &models_glob)? {
            let canonical = std::fs::canonicalize(&model.file_path)
                .unwrap_or_else(|_| PathBuf::from(&model.file_path));

            // Two genuinely distinct FILES sharing a model name: within this
            // pipeline, or against one already loaded for another. Either way
            // the DAG could only build one of them, so name both files.
            let clash = seen_here.get(&model.config.name).or_else(|| {
                file_of_name
                    .get(&model.config.name)
                    .filter(|(seen_canonical, _)| seen_canonical != &canonical)
                    .map(|(_, seen_display)| seen_display)
            });
            if let Some(first) = clash {
                anyhow::bail!(
                    "duplicate model name '{}': declared in both {} and {}. A \
                     transformation node is keyed by model name alone, so two \
                     models sharing a name cannot both be built — rename one.",
                    model.config.name,
                    first,
                    model.file_path,
                );
            }

            seen_here.insert(model.config.name.clone(), model.file_path.clone());
            file_of_name.insert(
                model.config.name.clone(),
                (canonical, model.file_path.clone()),
            );
            models.push(model);
        }
        models.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
        if !models.is_empty() {
            let canonical = std::fs::canonicalize(&dir).unwrap_or_else(|_| dir.clone());
            if !canonical_roots.contains(&canonical) {
                canonical_roots.push(canonical);
                contributing_roots.push(dir.clone());
            }
        }
        by_pipeline.insert(pipeline_name.clone(), models);
    }
    Ok(TransformationModels {
        by_pipeline,
        contributing_roots,
    })
}

/// Every transformation model in the project, attributed to the pipeline that
/// declared it, plus the roots those models were actually loaded from.
#[derive(Debug)]
pub(super) struct TransformationModels {
    pub by_pipeline: rocky_core::unified_dag::ModelsByPipeline,
    /// The distinct directories that contributed at least one model, in
    /// `cfg.pipelines` order. A pipeline whose directory is absent, or present
    /// but empty, contributes nothing and is not listed — so "how many roots
    /// are there" answers the question the lineage compile actually asks
    /// ("whose models must I cover"), not "how many were configured".
    pub contributing_roots: Vec<PathBuf>,
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
    /// Serializes pipeline-bound sub-runs per state file — see
    /// [`StateTurnstile`] (#1312). One turnstile per DAG run, shared by every
    /// node the dispatcher creates.
    state_turns: Arc<StateTurnstile>,
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
/// `--parallel` chooses *how many* things run at once, and the DAG owns that
/// dimension at the node level — [`run_with_dag`] now applies the flag there
/// (#1288). Honoring it per sub-run *as well* would give node-fan-out ×
/// per-partition-fan-out, so the narrowing below stays exactly as it was:
/// applying one bound twice is what makes it a multiplication.
/// `rocky run` refuses that multiplication by construction:
/// `super::run::execute_models` excludes `time_interval` from concurrent model
/// execution precisely because it "already self-parallelizes per-partition".
/// The DAG must not reintroduce what the non-DAG path is careful to avoid, so
/// per-sub-run concurrency stays at 1 — byte-identical to the value the DAG
/// passed before any selection was threaded.
///
/// `--parallel` now has a single meaning on both paths, and this function is
/// still where the DAG half is *refused*: the bound is applied once, to node
/// fan-out, in [`run_with_dag`]. Threading it here as well is what would give
/// node-fan-out x per-partition-fan-out (#1288).
/// The node-fan-out bound to replay from a stored plan's `parallel` field.
///
/// `rocky plan`'s own `--parallel` is `default_value = "1"`, so a stored **1**
/// cannot be told apart from a flag the planner never typed — bounding on it
/// would serialize every plan whose author simply omitted it, which is most of
/// them. Any other value can only have been typed, and discarding it ignores an
/// intent the plan unambiguously records.
///
/// Lives here, beside [`node_concurrency_limit`], rather than inline at the
/// `apply` call site, so the rule has one definition that a test can call.
pub(crate) fn replayed_node_concurrency(stored: u32) -> Option<u32> {
    (stored != 1).then_some(stored)
}

/// Build the DAG executor with the caller's node-fan-out bound applied.
///
/// Separate from [`run_with_dag`] so the wiring is assertable: dropping the
/// bound here is invisible to any test that merely executes a DAG, because
/// unbounded and bounded-above-the-node-count run identically.
fn dag_executor_with_bound<D: rocky_core::dag_executor::NodeDispatcher + 'static>(
    dispatcher: D,
    node_concurrency: Option<u32>,
) -> DagExecutor<D> {
    DagExecutor::new(dispatcher, node_concurrency_limit(node_concurrency))
}

/// How many DAG nodes may execute at once, from `--parallel` as the caller
/// spelled it.
///
/// One bound, applied once. Each sub-run executes a single partition at a time
/// ([`sub_run_partition_opts`] pins it to 1), so this is a ceiling on *nodes*
/// and the flag is not additionally multiplied inside each one.
///
/// It is **not** a ceiling on warehouse queries in general, and the difference
/// matters for a replication node: `run` builds its own table-level semaphore
/// from `[execution] concurrency` (default 32), which `--parallel` has never
/// governed on either path. So `--parallel 1` under `--dag` admits one node at
/// a time; a replication Load node may still run many tables inside it.
///
/// - `None` (flag absent) keeps the historical **unbounded** fan-out. Adopting
///   the non-`--dag` default of 4 here would silently cap a wide DAG that runs
///   today, which is why #1288 was split out of #1290 instead of riding along.
/// - `Some(0)` becomes 1. A zero-permit semaphore never admits anyone, so the
///   run would hang; `--parallel 0` reads as "no concurrency", never as
///   "unbounded", and the neighbouring meaning is the safe one.
fn node_concurrency_limit(parallel: Option<u32>) -> Option<usize> {
    parallel.map(|n| n.max(1) as usize)
}

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
                        .map_err(|e| format!("{e:#}"))
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
                let state_turns = Arc::clone(&self.state_turns);
                Some(Box::pin(async move {
                    // Take the exclusive turn for this state file BEFORE the
                    // sub-run opens the store, and hold it until the sub-run
                    // finishes — `run()` holds the writer flock for exactly
                    // that long (#1312). Waiting here pends the node future;
                    // the executor's other layers of work are unaffected.
                    let _turn = state_turns.turn(&state_path).await;
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
    use std::sync::{Arc, Mutex};

    use rocky_core::config::LoadedConfig;
    use rocky_core::dag_executor::NodeDispatcher;
    use rocky_core::unified_dag::{NodeId, NodeKind};

    use super::super::run::{PartitionRunOptions, SkipRunOptions};
    use super::{CliDispatcher, StateTurnstile, SubRunner};

    type RecordedSubRun = (Option<String>, PartitionRunOptions, SkipRunOptions);

    #[test]
    fn dag_discovery_honors_a_literal_prefix_model_glob() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models = root.join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n",
        )
        .unwrap();
        std::fs::write(models.join("customers.sql"), "SELECT 2 AS id\n").unwrap();
        std::fs::write(models.join("customers.toml"), "name = [\n").unwrap();
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"p.duckdb\"\n\n\
             [pipeline.silver]\ntype = \"transformation\"\nmodels = \"models/ord*.sql\"\n\n\
             [pipeline.silver.target]\nadapter = \"default\"\n",
        )
        .unwrap();
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();

        let loaded = super::load_transformation_models(&config_path, &cfg)
            .expect("a malformed non-matching sidecar must not block DAG discovery");
        let models = loaded
            .by_pipeline
            .get("silver")
            .expect("silver pipeline must be present");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].config.name, "orders");
    }

    /// A loaded snapshot for dispatcher tests, built through the real
    /// fingerprinted loader over a minimal temp config.
    pub(super) fn test_loaded_config() -> Arc<LoadedConfig> {
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
            state_turns: StateTurnstile::new(),
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
        // must see 1, because the DAG applies the bound ONCE, at the node
        // level (#1288). Threading it wholesale makes this 3 — the same bound
        // applied twice, which is a multiplication.
        assert_eq!(
            opts.parallel, 1,
            "--parallel must NOT reach the sub-run: the DAG already applies it \
             to node fan-out, so applying it again here multiplies (#1288)"
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

#[cfg(test)]
mod state_turnstile_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use rocky_core::config::LoadedConfig;
    use rocky_core::dag_executor::NodeDispatcher;
    use rocky_core::unified_dag::{NodeId, NodeKind};

    use super::super::run::{PartitionRunOptions, SkipRunOptions};
    use super::{CliDispatcher, StateTurnstile, SubRunner};

    /// A dispatcher over `n` transformation nodes with an explicit state path
    /// and a caller-supplied turnstile — the two knobs these tests turn.
    fn dispatcher(
        loaded: Arc<LoadedConfig>,
        runner: SubRunner,
        state_turns: Arc<StateTurnstile>,
        state_path: &str,
        n: usize,
    ) -> (CliDispatcher, Vec<NodeId>) {
        let mut node_pipelines = HashMap::new();
        let node_ids: Vec<NodeId> = (0..n)
            .map(|i| {
                let id = NodeId::new("transformation", &format!("m{i}"));
                node_pipelines.insert(id.clone(), "analytics".to_string());
                id
            })
            .collect();
        let dispatcher = CliDispatcher {
            config_path: std::path::PathBuf::from("rocky.toml"),
            loaded,
            state_path: std::path::PathBuf::from(state_path),
            seeds_dir: std::path::PathBuf::from("seeds"),
            node_pipelines,
            partition_opts: PartitionRunOptions::default(),
            skip_opts: SkipRunOptions::default(),
            shadow_config: None,
            sub_runner: runner,
            state_turns,
        };
        (dispatcher, node_ids)
    }

    /// The #1312 invariant at the dispatch seam: two nodes naming the SAME
    /// state file never run concurrently.
    ///
    /// The probe holds each "run" open for 40ms while recording the in-flight
    /// high-water mark; `tokio::join!` polls both futures together, so without
    /// the turnstile both enter the window and the mark reads 2 (verified by
    /// neutering the `turn()` acquisition — this assertion goes red). With it,
    /// the mark cannot exceed 1: the second future pends on the turn until the
    /// first guard drops.
    #[tokio::test]
    async fn same_state_file_sub_runs_take_turns() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let (inf, maxs) = (in_flight.clone(), max_seen.clone());
        let runner: SubRunner = Arc::new(move |_c, _l, _s, _p, _m, _po, _so, _sh| {
            let (inf, maxs) = (inf.clone(), maxs.clone());
            Box::pin(async move {
                let now = inf.fetch_add(1, Ordering::SeqCst) + 1;
                maxs.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        });
        let loaded = super::run_opts_threading_tests::test_loaded_config();
        let (dispatcher, ids) = dispatcher(
            loaded,
            runner,
            StateTurnstile::new(),
            ".rocky-state.redb",
            2,
        );
        let f0 = dispatcher
            .dispatch(&ids[0], NodeKind::Transformation, "m0")
            .expect("transformation node must dispatch");
        let f1 = dispatcher
            .dispatch(&ids[1], NodeKind::Transformation, "m1")
            .expect("transformation node must dispatch");
        let (r0, r1) = tokio::join!(f0, f1);
        r0.expect("first sub-run must succeed");
        r1.expect("second sub-run must succeed");
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "sub-runs sharing a state file must take turns, not race the \
             writer flock"
        );
    }

    /// The over-serialization guard: nodes naming DISTINCT state files still
    /// overlap. Each sub-run waits at a two-party barrier that only releases
    /// when both are inside a run simultaneously — if the turnstile ever
    /// collapsed into a global serializer, the second sub-run could not start
    /// while the first waited, the rendezvous would never complete, and the
    /// timeout turns that hang into a failure.
    #[tokio::test]
    async fn distinct_state_files_still_run_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let b = barrier.clone();
        let runner: SubRunner = Arc::new(move |_c, _l, _s, _p, _m, _po, _so, _sh| {
            let b = b.clone();
            Box::pin(async move {
                b.wait().await;
                Ok(())
            })
        });
        let loaded = super::run_opts_threading_tests::test_loaded_config();
        let turns = StateTurnstile::new();
        let (d_a, ids_a) = dispatcher(
            loaded.clone(),
            runner.clone(),
            Arc::clone(&turns),
            "a-state.redb",
            1,
        );
        let (d_b, ids_b) = dispatcher(loaded, runner, turns, "b-state.redb", 1);
        let f_a = d_a
            .dispatch(&ids_a[0], NodeKind::Transformation, "m0")
            .expect("transformation node must dispatch");
        let f_b = d_b
            .dispatch(&ids_b[0], NodeKind::Transformation, "m0")
            .expect("transformation node must dispatch");
        let (r_a, r_b) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(f_a, f_b) })
                .await
                .expect(
                    "distinct state files must run concurrently — cross-path \
             serialization would deadlock this rendezvous",
                );
        r_a.expect("sub-run a must succeed");
        r_b.expect("sub-run b must succeed");
    }
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;

    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    /// `--parallel` bounds DAG node fan-out, and an absent flag does not.
    ///
    /// Before #1288 `run_with_dag` built its executor with no bound at all, so
    /// `rocky run --dag --parallel 1` ran every node in a layer concurrently —
    /// the flag documented as "pass `--parallel 1` to force fully serial" did
    /// not force anything. The `None` case is the control *and* the
    /// compatibility guarantee: adopting the non-`--dag` default of 4 would cap
    /// a wide DAG that runs unbounded today.
    ///
    /// Mutation that must turn this red: `parallel.map(|n| n.max(1) as usize)`
    /// → `Some(parallel.unwrap_or(4).max(1) as usize)`, i.e. folding in the
    /// non-`--dag` default.
    #[test]
    fn parallel_bounds_node_fan_out_only_when_it_is_asked_for() {
        assert_eq!(
            node_concurrency_limit(None),
            None,
            "an absent --parallel must keep the historical unbounded fan-out"
        );
        assert_eq!(node_concurrency_limit(Some(1)), Some(1));
        assert_eq!(node_concurrency_limit(Some(8)), Some(8));
    }

    /// The bound reaches the executor `run_with_dag` actually builds.
    ///
    /// Review caught that `node_concurrency_limit` alone proves nothing about
    /// production: deleting the `with_max_concurrency` call left every test
    /// green, because a DAG executes identically whether it is unbounded or
    /// bounded above its node count. This asserts the wiring itself.
    ///
    /// Mutation that must turn this red: drop the `Some(n) =>` arm of
    /// `dag_executor_with_bound` so it always builds an unbounded executor.
    #[test]
    fn the_bound_reaches_the_executor_that_is_built() {
        struct NoopDispatcher;
        impl rocky_core::dag_executor::NodeDispatcher for NoopDispatcher {
            fn dispatch(
                &self,
                _id: &rocky_core::unified_dag::NodeId,
                _kind: rocky_core::unified_dag::NodeKind,
                _label: &str,
            ) -> Option<rocky_core::dag_executor::NodeFuture> {
                None
            }
        }

        assert_eq!(
            dag_executor_with_bound(NoopDispatcher, Some(3)).max_concurrency(),
            Some(3),
            "an explicit --parallel must reach the executor"
        );
        assert_eq!(
            dag_executor_with_bound(NoopDispatcher, Some(0)).max_concurrency(),
            Some(1),
            "--parallel 0 must reach it as 1, not as unbounded"
        );
        assert_eq!(
            dag_executor_with_bound(NoopDispatcher, None).max_concurrency(),
            None,
            "an absent --parallel must leave the executor unbounded"
        );
    }

    /// A stored plan's `--parallel` is replayed when it carries intent.
    ///
    /// `rocky plan`'s own `--parallel` is `default_value = "1"`, so a stored
    /// **1** cannot be told from a flag the planner never typed — bounding on
    /// it would serialize every plan whose author simply omitted it. Any other
    /// value can only have been typed, and discarding it ignores an intent the
    /// plan unambiguously records. Review caught the first revision discarding
    /// *all* stored values on the strength of the ambiguous one.
    ///
    /// Mutation that must turn this red: replay unconditionally
    /// (`Some(parallel)`), or discard unconditionally (`None`).
    #[test]
    fn a_planned_bound_is_replayed_unless_it_is_the_ambiguous_default() {
        use super::replayed_node_concurrency as replayed;

        assert_eq!(
            replayed(1),
            None,
            "a stored 1 is indistinguishable from an omitted flag and must not \
             silently serialize a plan whose author never asked"
        );
        assert_eq!(
            replayed(2),
            Some(2),
            "an unambiguous bound must be replayed"
        );
        assert_eq!(replayed(8), Some(8));
    }

    /// `--parallel 0` is one, never unbounded.
    ///
    /// `DagExecutor` turns the limit into a semaphore, and a zero-permit
    /// semaphore admits nobody — the run would hang rather than go fast. Of the
    /// two neighbouring readings, "no concurrency" is the safe one, and it is
    /// also what `--parallel 0` says.
    #[test]
    fn a_zero_parallel_is_serial_not_unbounded() {
        assert_eq!(node_concurrency_limit(Some(0)), Some(1));
    }

    /// The one config snapshot every [`run_with_dag`] caller must supply
    /// (#1289). Production callers get theirs from the apply gate; a test that
    /// is not exercising the gate loads it the same way `rocky run --dag`
    /// does. Tests that need to prove the snapshot is *honored* build it
    /// themselves and then mutate `rocky.toml` — see
    /// `a_stored_dag_plan_executes_the_gates_snapshot_not_a_reload`.
    fn dag_snapshot(config_path: &Path) -> std::sync::Arc<rocky_core::config::LoadedConfig> {
        std::sync::Arc::new(
            rocky_core::config::load_rocky_config_fingerprinted(config_path)
                .expect("test fixture config must load"),
        )
    }

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
            state_turns: StateTurnstile::new(),
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
            dag_snapshot(&config_path),
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
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
            dag_snapshot(&config_path),
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
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
            dag_snapshot(&config_path),
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            Some(&shadow_config),
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
            None,
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
            dag_snapshot(&config_path),
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
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
            dag_snapshot(&config_path),
            &state_path,
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            Some(&shadow_config),
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
            None,
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
            dag_snapshot(&root.join("rocky.toml")),
            &root.join(".rocky-state.redb"),
            false,
            &partition_opts,
            &SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
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

    /// 🔴 #1261 regression, CLI seam: a project with TWO transformation
    /// pipelines, each with its own models directory, must build and execute.
    ///
    /// The `unified_dag` unit tests own the graph shape; this owns the wiring
    /// that feeds it — `load_transformation_models` keying models by the
    /// pipeline whose configured directory they came from. Pre-fix this project
    /// could not run at all: every model got a node under both pipelines and the
    /// DAG failed with `circular dependency detected involving: [...]` before
    /// executing anything.
    ///
    /// Non-vacuous: it asserts BOTH targets materialize, so collapsing the
    /// attribution back to one flat list fails at DAG build, and mis-attributing
    /// a model to the wrong pipeline fails at execution (the sub-run is scoped
    /// to its pipeline, which would not find the model).
    /// Nested roots reach ONE file through two pipelines — that is a shared
    /// claim, not a duplicate name, and must be reported as such.
    ///
    /// `alpha = "transforms/**"` and `beta = "transforms/staging/**"`: the
    /// one-level loader returns `transforms/staging/orders` for BOTH. Keying the
    /// duplicate check on each pipeline's base directory saw two different bases
    /// and reported "declared in both <path> and <same path>" — naming one file
    /// twice and never reaching the error that can name `alpha` and `beta`.
    /// Keying on the resolved file instead makes the two cases distinguishable.
    #[test]
    fn nested_pipeline_roots_reaching_one_file_report_the_shared_claim() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("transforms/staging")).unwrap();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \"p.duckdb\"\n\n\
             [pipeline.alpha]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.alpha.target]\nadapter = \"default\"\n\n\
             [pipeline.beta]\ntype = \"transformation\"\nmodels = \"transforms/staging/**\"\n\n\
             [pipeline.beta.target]\nadapter = \"default\"\n",
        )
        .unwrap();
        std::fs::write(
            root.join("transforms/staging/orders.sql"),
            "SELECT 1 AS id\n",
        )
        .unwrap();
        std::fs::write(
            root.join("transforms/staging/orders.toml"),
            "depends_on = []\n\n[target]\ncatalog = \"p\"\nschema = \"s\"\ntable = \"orders\"\n",
        )
        .unwrap();

        let config_path = root.join("rocky.toml");
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        // Loading must SUCCEED — one file reached twice is not a duplicate name.
        let by_pipeline = load_transformation_models(&config_path, &cfg)
            .expect("one file reached through two roots is not a duplicate name")
            .by_pipeline;

        // ...and the shared claim is then reported by the DAG, which can name
        // the pipelines.
        let err = rocky_core::unified_dag::build_unified_dag(&cfg, &by_pipeline, &[])
            .expect_err("two pipelines claiming one model must be refused");
        let msg = err.to_string();
        assert!(msg.contains("alpha") && msg.contains("beta"), "got: {msg}");
        assert!(
            !msg.contains("duplicate model name"),
            "must not be reported as a duplicate name: {msg}"
        );
    }

    /// Guards the seam that per-pipeline attribution nearly broke: a duplicate
    /// model name inside ONE pipeline's tree.
    ///
    /// The loader reads a base directory plus one level below it, so a single
    /// pipeline can reach `orders` at both `transforms/orders` and
    /// `transforms/staging/orders`. Keying the duplicate check on the resolved
    /// DIRECTORY — needed so two pipelines sharing a directory reach
    /// `build_unified_dag`, which names them — silently let that through,
    /// because both copies resolve to the same directory. The check is per
    /// pipeline as well as across directories.
    #[test]
    fn a_duplicate_name_inside_one_pipelines_tree_is_still_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("transforms/staging")).unwrap();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \"p.duckdb\"\n\n\
             [pipeline.silver]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.silver.target]\nadapter = \"default\"\n",
        )
        .unwrap();
        for sub in ["transforms", "transforms/staging"] {
            std::fs::write(root.join(sub).join("orders.sql"), "SELECT 1 AS id\n").unwrap();
            std::fs::write(
                root.join(sub).join("orders.toml"),
                "depends_on = []\n\n[target]\ncatalog = \"p\"\nschema = \"s\"\ntable = \"orders\"\n",
            )
            .unwrap();
        }

        let cfg = rocky_core::config::load_rocky_config(&root.join("rocky.toml")).unwrap();
        let err = load_transformation_models(&root.join("rocky.toml"), &cfg)
            .expect_err("one pipeline reaching the same model name twice must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("duplicate model name 'orders'"),
            "must name the model, got: {msg}"
        );
    }

    #[tokio::test]
    async fn two_transformation_pipelines_with_distinct_model_dirs_both_execute() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models/silver")).unwrap();
        std::fs::create_dir_all(root.join("models/gold")).unwrap();
        let db_path = root.join("proj.duckdb");

        std::fs::write(
            root.join("rocky.toml"),
            format!(
                "[adapter.local]\n\
                 type = \"duckdb\"\n\
                 path = \"{}\"\n\n\
                 [pipeline.silver]\n\
                 type = \"transformation\"\n\
                 models = \"models/silver/**\"\n\n\
                 [pipeline.silver.target]\n\
                 adapter = \"local\"\n\n\
                 [pipeline.silver.target.governance]\n\
                 auto_create_catalogs = true\n\
                 auto_create_schemas = true\n\n\
                 [pipeline.gold]\n\
                 type = \"transformation\"\n\
                 models = \"models/gold/**\"\n\
                 depends_on = [\"silver\"]\n\n\
                 [pipeline.gold.target]\n\
                 adapter = \"local\"\n\n\
                 [pipeline.gold.target.governance]\n\
                 auto_create_catalogs = true\n\
                 auto_create_schemas = true\n",
                db_path.display()
            ),
        )
        .unwrap();

        std::fs::write(root.join("models/silver/stg.sql"), "SELECT 1 AS id\n").unwrap();
        std::fs::write(
            root.join("models/silver/stg.toml"),
            "depends_on = []\n\n[target]\ncatalog = \"proj\"\nschema = \"s\"\ntable = \"stg\"\n",
        )
        .unwrap();
        std::fs::write(root.join("models/gold/fct.sql"), "SELECT 2 AS id\n").unwrap();
        std::fs::write(
            root.join("models/gold/fct.toml"),
            "depends_on = []\n\n[target]\ncatalog = \"proj\"\nschema = \"g\"\ntable = \"fct\"\n",
        )
        .unwrap();

        run_with_dag(
            &root.join("rocky.toml"),
            dag_snapshot(&root.join("rocky.toml")),
            &root.join(".rocky-state.redb"),
            false,
            &PartitionRunOptions::default(),
            &SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
            None,
        )
        .await
        .expect("a two-transformation-pipeline DAG must build and run");

        let adapter = DuckDbWarehouseAdapter::open(&db_path).unwrap();
        let conn = adapter.shared_connector();
        let guard = conn.lock().unwrap();
        for (table, expected) in [("proj.s.stg", 1i64), ("proj.g.fct", 2i64)] {
            let rows = guard
                .execute_sql(&format!("SELECT id FROM {table}"))
                .unwrap_or_else(|e| panic!("{table} should have materialized: {e}"));
            assert_eq!(rows.rows.len(), 1, "{table} row count");
            assert_eq!(cell_i64(&rows.rows[0][0]), expected, "{table} value");
        }
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
            dag_snapshot(&root.join("rocky.toml")),
            &root.join(".rocky-state.redb"),
            false,
            &PartitionRunOptions::default(),
            &crate::commands::run::SkipRunOptions::default(),
            None,
            // Unbounded node fan-out: these tests predate `--parallel`
            // bounding it and do not exercise concurrency (#1288).
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
