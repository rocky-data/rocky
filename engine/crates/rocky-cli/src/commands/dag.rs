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

/// Where `--column-lineage` should compile from, once the models have been
/// loaded and attributed.
///
/// Three states, because "no root" and "several roots" both lack a single
/// directory to compile but mean opposite things: the first has no lineage to
/// report, the second has lineage this command cannot report correctly.
enum LineageSource {
    /// Compile this directory. Note this still inherits #1262: the compiler
    /// reads the directory itself, while the model loader also reads one level
    /// below it, so a project keeping its models in `<root>/staging/` gets a
    /// correct DAG and empty lineage. Pre-existing and unchanged here — closing
    /// it means teaching `rocky-compiler` to walk, not widening this refusal
    /// into the common nested layout.
    Root(std::path::PathBuf),
    /// No transformation models exist, so no lineage does either. Empty is the
    /// complete answer.
    NoModels,
    /// Models came from several roots and the compiler reads one.
    SeveralRoots,
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
    // the single directory the optional column-lineage compile reads. All three
    // must describe the same set of models — deriving the graph per pipeline
    // while enriching from a fallback `models/` left a project with a custom
    // root (`models = "transforms/**"`) holding correctly-shaped nodes whose
    // target, strategy and freshness were all silently `None`.
    let (models, models_by_pipeline, lineage_source) = match models_dir {
        // An explicit whole-project override: every transformation pipeline
        // genuinely does declare this one directory — and a project with two of
        // them is then refused by name, which is the honest answer to "these two
        // pipelines both build this model".
        Some(dir) => {
            let models = load_all_models(dir)?;
            let mut by_pipeline = rocky_core::unified_dag::ModelsByPipeline::new();
            for (name, pipeline) in &cfg.pipelines {
                if matches!(
                    pipeline,
                    rocky_core::config::PipelineConfig::Transformation(_)
                ) {
                    by_pipeline.insert(name.clone(), models.clone());
                }
            }
            (models, by_pipeline, LineageSource::Root(dir.to_path_buf()))
        }
        // No override: each pipeline resolves its own directory, which is also
        // what `rocky run --dag` has always done. Before #1261, `rocky dag
        // --models models` and `rocky run --dag` disagreed, the former giving a
        // node to a model no pipeline declared.
        None => {
            let loaded = super::run_dag_exec::load_transformation_models(config_path, &cfg)?;
            let models = union_by_model_name(&loaded.by_pipeline);
            // The compiler reads exactly one root (#1262), so lineage is only
            // answerable when the models came from one. Picking an arbitrary
            // root would emit lineage that silently omits every other root's
            // models; `column_lineage` has no "unavailable" encoding, so a
            // partial list is indistinguishable from a complete one.
            //
            // Zero roots is NOT that situation and must not be folded into it:
            // a project with no transformation models has no lineage, and empty
            // is the complete answer rather than a withheld one. Conflating the
            // two made `rocky dag --column-lineage` fail on every
            // replication-only project with a message about "different model
            // directories" it does not have.
            let source = match loaded.contributing_roots.as_slice() {
                [] => LineageSource::NoModels,
                [only] => LineageSource::Root(only.clone()),
                _ => LineageSource::SeveralRoots,
            };
            (models, loaded.by_pipeline, source)
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
    let column_lineage = match (include_column_lineage, lineage_source) {
        (false, _) => vec![],
        (true, LineageSource::Root(root)) => build_column_lineage_from_models(
            &root,
            contracts_dir,
            cfg,
            state_path,
            schema_cache_cfg,
        )?,
        // Nothing to compile, and nothing withheld.
        (true, LineageSource::NoModels) => vec![],
        // Lineage was asked for and cannot be answered correctly: the compiler
        // reads one model root (#1262) and this project's models came from
        // several. Refusing beats returning the edges of whichever root won a
        // coin flip — `column_lineage` is a bare list, so a caller cannot tell a
        // partial answer from a complete one. Narrow by construction: only
        // `--column-lineage` reaches here, so plain `rocky dag` and
        // `GET /api/v1/dag` are untouched, and the SDK always passes `--models`.
        (true, LineageSource::SeveralRoots) => anyhow::bail!(
            "cannot compute column lineage: this project's transformation \
             models come from more than one directory, and lineage is compiled \
             from a single root (#1262). Re-run scoped to one root with \
             `rocky dag --column-lineage --models <dir>`, or drop \
             `--column-lineage` for the structural DAG.",
        ),
    };

    Ok(DagOutput {
        version: VERSION.to_string(),
        schema_version: DAG_SCHEMA_VERSION.to_string(),
        command: "dag".to_string(),
        nodes,
        edges,
        execution_layers,
        summary,
        column_lineage,
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
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    _cfg: &rocky_core::config::RockyConfig,
    state_path: &Path,
    schema_cache_cfg: &rocky_core::config::SchemaCacheConfig,
) -> Result<Vec<LineageEdgeRecord>> {
    let compile_config = rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
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
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };

    let result = match rocky_compiler::compile::compile(&compile_config) {
        Ok(r) => r,
        Err(_) => return Ok(vec![]),
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

    Ok(edges)
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
pub(super) fn load_all_models(models_dir: &Path) -> Result<Vec<Model>> {
    let mut all = crate::models_loader::load_project_models(models_dir)?;
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
}
