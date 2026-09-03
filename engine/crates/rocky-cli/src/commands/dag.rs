//! `rocky dag` — emit the full unified DAG as enriched JSON.
//!
//! Projects the engine's internal [`UnifiedDag`] into an orchestrator-friendly
//! shape: every pipeline stage becomes a node with its target table
//! coordinates, materialization strategy, freshness SLA, partition shape,
//! and direct upstream dependencies.
//!
//! Consumers (dagster-rocky) can build a complete, connected Dagster asset
//! graph from a single `rocky dag --output json` call.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::models::{Model, StrategyConfig};
use rocky_core::seeds::SeedFile;
use rocky_core::unified_dag::{self, NodeKind, UnifiedDag};
use rocky_ir::TimeGrain;

use crate::output::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Version of the `rocky dag` graph-export contract. Bumped only on a
/// backward-incompatible change to the node/edge/lineage shape orchestrators
/// consume; see [`crate::output::DagOutput::schema_version`].
///
/// This is the version the CURRENT engine emits, and is intentionally SEPARATE
/// from `default_dag_schema_version()` in `output.rs` (the value a consumer
/// assumes when the field is absent, which stays `"1"` across future bumps
/// because an absent field means a pre-field engine emitting the original v1
/// shape). The two must NOT be collapsed into one shared const.
const DAG_SCHEMA_VERSION: &str = "1";

/// Which already-loaded models `--column-lineage` should compile.
///
/// Three states rather than an `Option`, because "no models" and "several
/// roots" both produce no compile today but mean opposite things: the first has
/// no lineage to report, while the second is the pre-existing #1262 limitation.
enum LineageSource {
    /// Compile this exact glob-selected set without re-reading its directory.
    Models(Vec<Model>),
    /// No transformation models exist, so no lineage does either. Empty is the
    /// complete answer.
    NoModels,
}

/// Execute `rocky dag`.
///
/// `cache_ttl_override`: CLI `--cache-ttl` flag override for the
/// `[cache.schemas] ttl_seconds` setting. Only relevant when
/// `include_column_lineage` drives a compile.
#[allow(clippy::too_many_arguments)]
pub fn run_dag(
    config_path: &Path,
    state_path: &Path,
    models_dir: Option<&Path>,
    seeds_dir: Option<&Path>,
    contracts_dir: Option<&Path>,
    include_column_lineage: bool,
    json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let output = dag_output(
        config_path,
        state_path,
        models_dir,
        seeds_dir,
        contracts_dir,
        include_column_lineage,
        cache_ttl_override,
    )?;

    if json {
        print_json(&output)?;
    } else {
        print_dag_table(&output);
    }
    Ok(())
}

/// Side-effect-free core of `rocky dag`: load models + seeds, build the
/// unified DAG, compute execution phases, and assemble the enriched
/// [`DagOutput`]. Does no printing, so other surfaces (`rocky serve`'s
/// `GET /api/v1/dag`) can serve the identical bytes `rocky dag --output json`
/// emits.
///
/// `cache_ttl_override`: CLI `--cache-ttl` flag override for the
/// `[cache.schemas] ttl_seconds` setting. Only relevant when
/// `include_column_lineage` drives a compile.
#[allow(clippy::too_many_arguments)]
pub fn dag_output(
    config_path: &Path,
    state_path: &Path,
    // `Some(dir)` is an explicit whole-project override (`rocky dag --models`):
    // every transformation pipeline is read from that one directory. `None`
    // means "no override" — each pipeline resolves its own configured
    // directory, and the enrichment list and lineage root are derived from
    // those rather than from any fallback path.
    //
    // The two used to be a `(&Path, bool)` pair, which let a caller pass a
    // directory and *contradict* it with the flag. `rocky serve` did exactly
    // that: its `--models` carried `default_value = "models"`, so the untouched
    // default arrived flagged as explicit and overrode every pipeline (#1261).
    // An `Option` makes that pair unrepresentable.
    models_dir: Option<&Path>,
    seeds_dir: Option<&Path>,
    contracts_dir: Option<&Path>,
    include_column_lineage: bool,
    cache_ttl_override: Option<u64>,
) -> Result<DagOutput> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;
    // Apply `--cache-ttl` once up-front; the column-lineage compile
    // below consumes the already-overridden `SchemaCacheConfig`.
    let schema_cache_cfg = cfg
        .cache
        .schemas
        .clone()
        .with_ttl_override(cache_ttl_override);

    // Attribution for the graph is per transformation pipeline. Handing every
    // transformation pipeline the same list gave each model a node under each
    // of them, all sharing one id, and the DAG collapsed into
    // `circular dependency detected involving: []` (#1261).
    //
    // Three things come out of this: the per-pipeline attribution the graph is
    // built from, the flat name-keyed list the nodes are *enriched* from, and
    // which exact set the optional column-lineage compile receives.
    //
    // The first two describe exactly the same set of models, and must: deriving
    // the graph per pipeline while enriching from a fallback `models/` left a
    // project with a custom root (`models = "transforms/**"`) holding
    // correctly-shaped nodes whose target, strategy and freshness were all
    // silently `None`.
    //
    // The third is the same already-loaded set whenever it came from one
    // contributing root. Re-reading the directory here would ignore the
    // configured file glob and could emit lineage for a model absent from the
    // DAG, or lose all lineage to a malformed non-matching sidecar.
    let (models, models_by_pipeline, lineage_source, missing_roots) = match models_dir {
        // An explicit whole-project override: every transformation pipeline
        // genuinely does declare this one directory — and a project with two of
        // them is then refused by name, which is the honest answer to "these two
        // pipelines both build this model".
        Some(dir) => {
            // The override replaces every pipeline's root, so "missing" is
            // one question: does the named directory exist?
            let missing_roots: Vec<(String, std::path::PathBuf)> = if dir.is_dir() {
                Vec::new()
            } else {
                vec![("--models".to_string(), dir.to_path_buf())]
            };
            let models = load_all_models(dir, Some(&cfg.freshness))?;
            let mut by_pipeline = rocky_core::unified_dag::ModelsByPipeline::new();
            for (name, pipeline) in &cfg.pipelines {
                if matches!(
                    pipeline,
                    rocky_core::config::PipelineConfig::Transformation(_)
                ) {
                    by_pipeline.insert(name.clone(), models.clone());
                }
            }
            let lineage_models = models.clone();
            (
                models,
                by_pipeline,
                LineageSource::Models(lineage_models),
                missing_roots,
            )
        }
        // No override: each pipeline resolves its own directory, which is also
        // what `rocky run --dag` has always done. Before #1261, `rocky dag
        // --models models` and `rocky run --dag` disagreed, the former giving a
        // node to a model no pipeline declared.
        None => {
            let loaded = super::run_dag_exec::load_transformation_models(config_path, &cfg)?;
            let models = union_by_model_name(&loaded.by_pipeline);
            // Root count no longer decides anything. Lineage compiles the
            // model set the DAG was built from (`compile_preloaded_models`),
            // not a directory re-read, so a set spanning several roots is as
            // compilable as one from a single root. That is exactly the
            // correctness fix the old `SeveralRoots` arm deferred to #1262 —
            // which has since closed, leaving that arm dropping real
            // cross-pipeline lineage for no remaining reason.
            //
            // Zero roots is genuinely different: a project with no
            // transformation models has no lineage, and empty is the complete
            // answer rather than an unavailable one. Conflating the two once
            // made `rocky dag --column-lineage` fail on every replication-only
            // project with a message about "different model directories" it
            // does not have.
            let source = if loaded.contributing_roots.is_empty() {
                LineageSource::NoModels
            } else {
                LineageSource::Models(models.clone())
            };
            (models, loaded.by_pipeline, source, loaded.missing_roots)
        }
    };

    // Load seeds if the directory exists. A discovery failure is propagated, not
    // flattened to "no seeds": the seed nodes and the seed→model edges are built
    // only from this list, so swallowing a malformed sidecar prints a DAG that
    // is missing both — an answer that looks complete and is not.
    let discover = |dir: &Path| -> Result<Vec<rocky_core::seeds::SeedFile>> {
        rocky_core::seeds::discover_seeds(dir)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("failed to discover seeds in {}", dir.display()))
    };
    let seeds = match seeds_dir {
        Some(dir) if dir.exists() => discover(dir)?,
        _ => {
            // Try default "seeds" relative to config dir.
            let default_seeds = config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("seeds");
            if default_seeds.is_dir() {
                discover(&default_seeds)?
            } else {
                vec![]
            }
        }
    };

    // Build the unified DAG.
    let dag = unified_dag::build_unified_dag(&cfg, &models_by_pipeline, &seeds)
        .context("failed to build unified DAG")?;

    // Compute execution phases (topological layers).
    let phases =
        unified_dag::execution_phases(&dag).context("failed to compute execution phases")?;

    // Build the enriched output.
    build_dag_output(
        &dag,
        &phases,
        &models,
        &seeds,
        include_column_lineage,
        lineage_source,
        contracts_dir,
        &cfg,
        state_path,
        &schema_cache_cfg,
        &missing_roots,
    )
}

/// Build the full `DagOutput` from the unified DAG plus model/seed metadata.
#[allow(clippy::too_many_arguments)]
fn build_dag_output(
    dag: &UnifiedDag,
    phases: &[Vec<&rocky_core::unified_dag::UnifiedNode>],
    models: &[Model],
    seeds: &[SeedFile],
    include_column_lineage: bool,
    lineage_source: LineageSource,
    contracts_dir: Option<&Path>,
    cfg: &rocky_core::config::RockyConfig,
    state_path: &Path,
    schema_cache_cfg: &rocky_core::config::SchemaCacheConfig,
    missing_roots: &[(String, std::path::PathBuf)],
) -> Result<DagOutput> {
    // Build lookup maps.
    let model_map: HashMap<&str, &Model> =
        models.iter().map(|m| (m.config.name.as_str(), m)).collect();

    let seed_map: HashMap<&str, &SeedFile> = seeds.iter().map(|s| (s.name.as_str(), s)).collect();

    // Index incoming edges by target node once. The per-node
    // `UnifiedDag::incoming_edges` call scanned every edge (O(nodes × edges));
    // a single pass builds the same lookup in O(nodes + edges). Edge order
    // within a node is preserved, so `depends_on` ordering is unchanged.
    let mut incoming_by_node: HashMap<&unified_dag::NodeId, Vec<&unified_dag::UnifiedEdge>> =
        HashMap::new();
    for edge in &dag.edges {
        incoming_by_node.entry(&edge.to).or_default().push(edge);
    }

    // Project nodes.
    let nodes: Vec<DagNodeOutput> = dag
        .nodes
        .iter()
        .map(|node| {
            let depends_on: Vec<String> = incoming_by_node
                .get(&node.id)
                .map(|edges| edges.iter().map(|e| e.from.0.clone()).collect())
                .unwrap_or_default();

            let (target, strategy, freshness, partition_shape) = match node.kind {
                NodeKind::Transformation => {
                    if let Some(model) = model_map.get(node.label.as_str()) {
                        (
                            Some(model.config.target.clone()),
                            Some(model.config.strategy.clone()),
                            model.config.freshness.clone(),
                            extract_partition_shape(&model.config.strategy),
                        )
                    } else {
                        (None, None, None, None)
                    }
                }
                NodeKind::Seed => {
                    if let Some(seed) = seed_map.get(node.label.as_str()) {
                        let seed_target = seed.config.target.as_ref();
                        (
                            Some(rocky_core::models::TargetConfig {
                                catalog: seed_target
                                    .and_then(|t| t.catalog.clone())
                                    .unwrap_or_default(),
                                schema: seed_target.map(|t| t.schema.clone()).unwrap_or_default(),
                                table: seed_target
                                    .and_then(|t| t.table.clone())
                                    .unwrap_or_else(|| seed.name.clone()),
                            }),
                            None,
                            None,
                            None,
                        )
                    } else {
                        (None, None, None, None)
                    }
                }
                _ => (None, None, None, None),
            };

            DagNodeOutput {
                id: node.id.0.clone(),
                kind: node.kind.to_string(),
                label: node.label.clone(),
                pipeline: node.pipeline.clone(),
                target,
                strategy,
                freshness,
                partition_shape,
                depends_on,
            }
        })
        .collect();

    // Project edges.
    let edges: Vec<DagEdgeOutput> = dag
        .edges
        .iter()
        .map(|e| DagEdgeOutput {
            from: e.from.0.clone(),
            to: e.to.0.clone(),
            edge_type: e.edge_type.to_string(),
        })
        .collect();

    // Execution layers (list of node ID lists).
    let execution_layers: Vec<Vec<String>> = phases
        .iter()
        .map(|layer| layer.iter().map(|n| n.id.0.clone()).collect())
        .collect();

    // Summary.
    let mut counts_by_kind: BTreeMap<String, usize> = BTreeMap::new();
    for node in &dag.nodes {
        *counts_by_kind.entry(node.kind.to_string()).or_default() += 1;
    }

    let summary = DagSummaryOutput {
        total_nodes: dag.nodes.len(),
        total_edges: dag.edges.len(),
        execution_layers: phases.len(),
        counts_by_kind,
    };

    // Column lineage (optional).
    //
    // The second element says whether the empty case is an *answer*. A project
    // with no models genuinely has no lineage; a failed compile means the
    // question was not answered. Returning `vec![]` for both let a consumer
    // read "nothing to trace" off a project Rocky could not parse (#1320).
    let (column_lineage, column_lineage_unavailable) =
        match (include_column_lineage, lineage_source) {
            // Not requested, so absence is expected rather than unavailable.
            (false, _) => (vec![], None),
            (true, LineageSource::Models(lineage_models)) => build_column_lineage_from_models(
                &lineage_models,
                contracts_dir,
                cfg,
                state_path,
                schema_cache_cfg,
            )?,
            // Nothing to compile — empty IS the complete answer here.
            (true, LineageSource::NoModels) => (vec![], None),
        };

    // A configured project must produce SOME graph — unless every declared
    // root exists and is simply empty, which is a supported no-op (a fresh
    // scaffold, an intentionally empty pipeline). The refusal fires only
    // when the graph is empty AND a declared transformation root does not
    // exist: that is how a project whose models root went missing fed the
    // dagster component a clean empty asset graph, cached with
    // `dag_status: "success"` and indistinguishable from a project that
    // has no models (#1397). Keyed on the final node set, so a seed-only
    // pipeline, a replication pipeline, or a sibling root holding the
    // models all still produce nodes and stay untouched.
    if nodes.is_empty()
        && let Some((pipeline, path)) = missing_roots.first()
    {
        anyhow::bail!(
            "the DAG has zero nodes, and the models root {} ('{}') does not \
             exist. Create the directory, fix the `models = ...` glob in \
             rocky.toml, or fix the --models override if one was passed",
            path.display(),
            pipeline,
        );
    }

    Ok(DagOutput {
        version: VERSION.to_string(),
        schema_version: DAG_SCHEMA_VERSION.to_string(),
        command: "dag".to_string(),
        nodes,
        edges,
        execution_layers,
        summary,
        column_lineage,
        column_lineage_unavailable,
    })
}

/// Map a `TimeGrain` to a human-friendly granularity string.
fn grain_to_string(grain: &TimeGrain) -> String {
    match grain {
        TimeGrain::Hour => "hourly",
        TimeGrain::Day => "daily",
        TimeGrain::Month => "monthly",
        TimeGrain::Year => "yearly",
    }
    .to_string()
}

/// Extract partition shape from a model's strategy config.
fn extract_partition_shape(strategy: &StrategyConfig) -> Option<PartitionShapeOutput> {
    match strategy {
        StrategyConfig::TimeInterval {
            granularity,
            first_partition,
            ..
        } => Some(PartitionShapeOutput {
            granularity: grain_to_string(granularity),
            first_partition: first_partition.clone(),
        }),
        StrategyConfig::Microbatch { granularity, .. } => Some(PartitionShapeOutput {
            granularity: grain_to_string(granularity),
            first_partition: None,
        }),
        _ => None,
    }
}

/// Build column-level lineage edges by compiling models and extracting
/// the semantic graph.
fn build_column_lineage_from_models(
    models: &[Model],
    contracts_dir: Option<&Path>,
    _cfg: &rocky_core::config::RockyConfig,
    state_path: &Path,
    schema_cache_cfg: &rocky_core::config::SchemaCacheConfig,
) -> Result<(Vec<LineageEdgeRecord>, Option<String>)> {
    let compile_config = rocky_compiler::compile::CompilerConfig {
        models_dir: std::path::PathBuf::new(),
        contracts_dir: contracts_dir.map(Path::to_path_buf),
        // Typed columns flow from the persisted schema cache (populated
        // by `rocky run` / `rocky discover --with-schemas`) straight
        // into column lineage extraction, so downstream edges carry
        // real types instead of `RockyType::Unknown`.
        // `schema_cache_cfg` already has the
        // `--cache-ttl` override applied by `run_dag`.
        source_schemas: crate::source_schemas::load_cached_source_schemas(
            schema_cache_cfg,
            state_path,
        ),
        ..Default::default()
    };

    // Compile the exact model objects used to build the DAG. Filtering already
    // happened before parsing, so an excluded malformed sidecar cannot erase
    // selected-model lineage and an excluded valid model cannot add an edge.
    // Preserve the existing tolerant surface: a lineage-only compile failure
    // yields no lineage rather than failing `rocky dag`. What changes is that
    // it now *says so* — the tolerance was never the problem, reporting the
    // failure as an empty answer was (#1320).
    let result =
        match rocky_compiler::compile::compile_preloaded_models(models.to_vec(), &compile_config) {
            Ok(r) => r,
            Err(e) => {
                return Ok((
                    vec![],
                    Some(format!("column lineage could not be computed: {e:#}")),
                ));
            }
        };

    let graph = &result.semantic_graph;
    let edges: Vec<LineageEdgeRecord> = graph
        .edges
        .iter()
        .map(|e| LineageEdgeRecord {
            source: LineageQualifiedColumn {
                model: e.source.model.to_string(),
                column: e.source.column.to_string(),
            },
            target: LineageQualifiedColumn {
                model: e.target.model.to_string(),
                column: e.target.column.to_string(),
            },
            transform: format!("{}", e.transform),
        })
        .collect();

    Ok((edges, None))
}

/// Flatten a per-pipeline attribution back into the single name-keyed list the
/// node enrichment and the `model_map` lookup want, sorted by name to match
/// [`load_all_models`].
///
/// Deduping by name is safe rather than lossy: `load_transformation_models`
/// has already refused two *distinct files* sharing a model name, so a name
/// reaching this twice is one file claimed by two pipelines — same `Model`
/// either way. That claim is itself refused downstream by `build_unified_dag`,
/// which can name both pipelines; this only has to not panic before it does.
fn union_by_model_name(by_pipeline: &rocky_core::unified_dag::ModelsByPipeline) -> Vec<Model> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut all: Vec<Model> = Vec::new();
    for models in by_pipeline.values() {
        for model in models {
            if seen.insert(model.config.name.clone()) {
                all.push(model.clone());
            }
        }
    }
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    all
}

/// Load models from a directory and its immediate subdirectories
/// (including `.rocky` DSL files), sorted by name.
pub(super) fn load_all_models(
    models_dir: &Path,
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
) -> Result<Vec<Model>> {
    let mut all = crate::models_loader::load_project_models(models_dir, project_freshness)?;
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Print a human-readable table view of the DAG.
fn print_dag_table(output: &DagOutput) {
    println!(
        "Unified DAG: {} nodes, {} edges, {} execution layers\n",
        output.summary.total_nodes, output.summary.total_edges, output.summary.execution_layers,
    );

    for (i, layer) in output.execution_layers.iter().enumerate() {
        println!("Layer {}:", i);
        for node_id in layer {
            if let Some(node) = output.nodes.iter().find(|n| &n.id == node_id) {
                let deps = if node.depends_on.is_empty() {
                    String::from("-")
                } else {
                    node.depends_on.join(", ")
                };
                println!(
                    "  {:<16} {:<30} depends_on: {}",
                    node.kind, node.label, deps
                );
            }
        }
    }

    if !output.column_lineage.is_empty() {
        println!("\nColumn lineage: {} edges", output.column_lineage.len());
    }
    // The table is the default surface, so it needs the same distinction the
    // JSON gained: printing nothing for an uncomputed lineage reads as "there
    // is none" to the person who just asked for it (#1320).
    if let Some(reason) = &output.column_lineage_unavailable {
        println!("\nColumn lineage: unavailable — {reason}");
    }
}
