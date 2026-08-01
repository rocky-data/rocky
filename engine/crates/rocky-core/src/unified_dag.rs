//! Unified ELT DAG — single graph representing all pipeline stages.
//!
//! Rocky currently models pipelines as separate [`PipelineConfig`] variants
//! (replication, transformation, quality, snapshot, load). This module
//! provides a unified DAG abstraction where every stage is a node with
//! typed edges expressing data-flow, governance, and check dependencies.
//!
//! ## Key features
//!
//! - **Parse-layer sugar:** a `type = "replication"` pipeline automatically
//!   expands into a `Source` + `Load` node pair, making the EL steps
//!   explicit in the DAG without requiring config changes.
//! - **Cross-step dependencies:** a model can depend on a seed, a test
//!   depends on its model, and a quality pipeline depends on upstream
//!   transformation outputs — all resolved into typed edges.
//! - **Validation:** cycle detection, duplicate node IDs, dangling edges,
//!   and invalid edge semantics (e.g., a test producing data downstream).
//! - **Execution phases:** Kahn's algorithm groups nodes into parallel
//!   layers respecting all dependency edges.
//!
//! [`PipelineConfig`]: crate::config::PipelineConfig

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{PipelineConfig, RockyConfig};
use crate::models::Model;
use crate::seeds::SeedFile;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from unified DAG construction or analysis.
#[derive(Debug, Error)]
pub enum UnifiedDagError {
    #[error("circular dependency detected involving: {nodes:?}")]
    CyclicDependency { nodes: Vec<String> },

    #[error("unknown dependency '{dependency}' referenced by node '{node}'")]
    UnknownDependency { node: String, dependency: String },

    #[error("pipeline '{pipeline}' referenced by model '{model}' not found in config")]
    PipelineNotFound { pipeline: String, model: String },

    #[error("duplicate node ID: '{id}'")]
    DuplicateNodeId { id: String },

    #[error("edge references non-existent node: from='{from}', to='{to}'")]
    DanglingEdge { from: String, to: String },

    #[error("invalid edge from '{from}' to '{to}': {reason}")]
    InvalidEdge {
        from: String,
        to: String,
        reason: String,
    },

    #[error("self-loop detected on node '{node}'")]
    SelfLoop { node: String },

    /// Two or more transformation pipelines resolve to a model set containing
    /// the same model. The unified DAG keys a transformation node by model
    /// name, so it cannot represent one model as two nodes — see
    /// [`build_unified_dag`].
    #[error(
        "model '{model}' is claimed by transformation pipelines {}. The unified DAG builds \
         each model once, so it cannot run the same model under two pipelines. Give each \
         pipeline its own `models` directory, or run them separately with \
         `rocky run --pipeline <name>` (which supports this).",
        .pipelines.join(" and ")
    )]
    ModelClaimedByMultiplePipelines {
        model: String,
        /// Sorted, so the message is deterministic.
        pipelines: Vec<String>,
    },

    #[error(
        "models {} resolve to the same physical table '{target}' on adapter '{adapter}'. The \
         unified DAG has no edge between them, so nothing orders the two writes and the \
         surviving rows would be decided by whichever finishes last. Give one of them its \
         own `[target] table`, point them at different \
         adapters, or run the pipelines separately with `rocky run --pipeline <name>`.",
        .claimants.iter().map(|(p, m)| format!("'{m}' (pipeline '{p}')"))
            .collect::<Vec<_>>().join(" and ")
    )]
    DuplicatePhysicalTargetAcrossPipelines {
        /// The target as the user spelled it.
        target: String,
        /// The adapter both resolve through.
        adapter: String,
        /// `(pipeline, model)` pairs, sorted, so the message is deterministic.
        claimants: Vec<(String, String)>,
    },
}

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

/// Unique identifier for a node in the unified DAG.
///
/// Format: `{kind}:{name}` (e.g., `replication:raw_ingest`,
/// `transformation:stg_orders`, `test:stg_orders::not_null_order_id`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    /// Creates a new node ID from a kind prefix and a name.
    pub fn new(kind: &str, name: &str) -> Self {
        Self(format!("{kind}:{name}"))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A node in the unified ELT DAG, representing a single pipeline stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedNode {
    /// Unique identifier within the DAG.
    pub id: NodeId,
    /// What kind of work this node represents.
    pub kind: NodeKind,
    /// Human-readable label (usually the pipeline or model name).
    pub label: String,
    /// Name of the originating pipeline in `rocky.toml` (if applicable).
    pub pipeline: Option<String>,
}

/// The kind of work a unified node represents.
///
/// Maps 1:1 to the current pipeline types, plus model-level node types
/// (Transformation, Seed, Test) that live inside a transformation pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// External data source (Fivetran connector, manual source definition).
    Source,
    /// Table replication (incremental copy, full refresh).
    ///
    /// Retained for deserialization compatibility with stored DAGs. New DAGs
    /// expand replication pipelines into [`Source`](Self::Source) +
    /// [`Load`](Self::Load) node pairs via parse-layer sugar.
    Replication,
    /// SQL/Rocky model execution.
    Transformation,
    /// Standalone data quality checks.
    Quality,
    /// SCD Type 2 snapshot capture.
    Snapshot,
    /// File ingestion (CSV, Parquet, JSONL).
    Load,
    /// CSV seed loading (static reference data).
    Seed,
    /// Declarative model test (not_null, unique, expression, etc.).
    Test,
}

impl fmt::Display for NodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Source => "source",
            Self::Replication => "replication",
            Self::Transformation => "transformation",
            Self::Quality => "quality",
            Self::Snapshot => "snapshot",
            Self::Load => "load",
            Self::Seed => "seed",
            Self::Test => "test",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// Edge types
// ---------------------------------------------------------------------------

/// A directed edge between two nodes in the unified DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedEdge {
    /// Node that must complete before `to`.
    pub from: NodeId,
    /// Node that depends on `from`.
    pub to: NodeId,
    /// Semantic type of the dependency.
    pub edge_type: EdgeType,
}

/// Semantic classification of a DAG edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Data flows from the upstream node to the downstream node.
    /// Examples: replication -> transformation, model -> model.
    DataDependency,
    /// The downstream node validates the upstream node's output.
    /// Examples: quality checks after replication, tests after model execution.
    CheckDependency,
    /// The downstream node enforces governance constraints on the upstream
    /// node (permissions, contracts, isolation).
    GovernanceDependency,
}

impl fmt::Display for EdgeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::DataDependency => "data",
            Self::CheckDependency => "check",
            Self::GovernanceDependency => "governance",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// DAG container
// ---------------------------------------------------------------------------

/// A unified directed acyclic graph representing all pipeline stages.
///
/// This is a read-only view built from the current [`RockyConfig`] and
/// loaded [`Model`] definitions. It does not own or modify any state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedDag {
    /// All nodes in the DAG.
    pub nodes: Vec<UnifiedNode>,
    /// All directed edges (from -> to).
    pub edges: Vec<UnifiedEdge>,
}

impl UnifiedDag {
    /// Returns a node by its ID, or `None` if not found.
    pub fn node(&self, id: &NodeId) -> Option<&UnifiedNode> {
        self.nodes.iter().find(|n| n.id == *id)
    }

    /// Returns all edges originating from the given node.
    pub fn outgoing_edges(&self, id: &NodeId) -> Vec<&UnifiedEdge> {
        self.edges.iter().filter(|e| e.from == *id).collect()
    }

    /// Returns all edges targeting the given node.
    pub fn incoming_edges(&self, id: &NodeId) -> Vec<&UnifiedEdge> {
        self.edges.iter().filter(|e| e.to == *id).collect()
    }

    /// Returns IDs of all root nodes (no incoming edges).
    pub fn roots(&self) -> Vec<&NodeId> {
        let has_incoming: HashSet<&NodeId> = self.edges.iter().map(|e| &e.to).collect();
        self.nodes
            .iter()
            .map(|n| &n.id)
            .filter(|id| !has_incoming.contains(id))
            .collect()
    }

    /// Returns IDs of all leaf nodes (no outgoing edges).
    pub fn leaves(&self) -> Vec<&NodeId> {
        let has_outgoing: HashSet<&NodeId> = self.edges.iter().map(|e| &e.from).collect();
        self.nodes
            .iter()
            .map(|n| &n.id)
            .filter(|id| !has_outgoing.contains(id))
            .collect()
    }

    /// Returns the total number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the total number of edges.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns a summary with counts per node kind.
    pub fn summary(&self) -> DagSummary {
        let mut counts: HashMap<NodeKind, usize> = HashMap::new();
        for node in &self.nodes {
            *counts.entry(node.kind).or_insert(0) += 1;
        }
        DagSummary {
            total_nodes: self.nodes.len(),
            total_edges: self.edges.len(),
            counts_by_kind: counts,
        }
    }
}

/// High-level summary of a unified DAG, useful for display in `rocky plan`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagSummary {
    /// Total number of nodes in the DAG.
    pub total_nodes: usize,
    /// Total number of edges in the DAG.
    pub total_edges: usize,
    /// Node counts grouped by kind.
    pub counts_by_kind: HashMap<NodeKind, usize>,
}

// ---------------------------------------------------------------------------
// DAG construction
// ---------------------------------------------------------------------------

/// Each transformation pipeline's own models, keyed by pipeline name.
///
/// `BTreeMap` rather than `HashMap` so node and error ordering is deterministic
/// across runs — the DAG payload is compared in CI fixtures and diffed by users.
pub type ModelsByPipeline = std::collections::BTreeMap<String, Vec<Model>>;

/// Refuses a model that more than one transformation pipeline resolved to.
///
/// Two pipelines whose `models` locations overlap — most easily by both taking
/// the `models/**` default — each claim every model in the shared directory.
/// See [`build_unified_dag`] for why that is refused rather than represented.
fn reject_models_claimed_twice(
    models_by_pipeline: &ModelsByPipeline,
) -> Result<(), UnifiedDagError> {
    // BTreeMap iteration is sorted, so `claimants` is built in pipeline-name
    // order and the message reads the same every run.
    let mut claimants: std::collections::BTreeMap<&str, Vec<String>> = Default::default();
    for (pipeline, models) in models_by_pipeline {
        for model in models {
            claimants
                .entry(model.config.name.as_str())
                .or_default()
                .push(pipeline.clone());
        }
    }
    // Report the first offender by model name rather than collecting all of
    // them: the fix is per-pipeline config, so a user resolves them one at a
    // time anyway, and the sorted map makes "first" deterministic.
    for (model, pipelines) in claimants {
        if pipelines.len() > 1 {
            return Err(UnifiedDagError::ModelClaimedByMultiplePipelines {
                model: model.to_string(),
                pipelines,
            });
        }
    }
    Ok(())
}

/// Everything claiming one physical table, while
/// [`reject_duplicate_physical_targets`] groups them.
struct TargetClaim {
    /// The target as the user spelled it — published in the message, never
    /// compared. Comparison is the folded map key.
    spelled: String,
    /// The adapter name — which is also half the map key.
    adapter: String,
    /// `(pipeline, model)` pairs resolving to this table.
    writers: Vec<(String, String)>,
}

/// Refuse two *different* pipelines whose models resolve to one physical table.
///
/// E036 (#1291) catches this within a single compile, which covers a plain
/// `rocky run`, `rocky compile`, the LSP, and each `--dag` sub-run. It cannot
/// catch it across pipelines: `--dag` discovers models per pipeline and each
/// sub-run compiles only its own root, so neither compile ever holds both
/// writers. The unified DAG is the only place that does — and it previously
/// rejected duplicate model *names* while saying nothing about duplicate
/// *targets*, so the two models became independent nodes in layer zero and
/// both wrote one table (#1301).
///
/// The defect is *unordered ownership*, not a data race in the narrow sense:
/// pipeline-bound sub-runs sharing a state path already take turns at the
/// dispatch turnstile (#1312), so the two writes are serialized today. They
/// are still two full replacements of one table with nothing deciding which
/// survives, and the turnstile is a state-store detail that could change.
/// Neither makes duplicate ownership correct.
///
/// **Keyed on the adapter NAME as well as `catalog.schema.table`.** That is the
/// difference from the per-project check, and it is a real one: two pipelines
/// may legitimately write `c.s.orders` on two different warehouses, and
/// refusing that would be wrong.
///
/// The name is the right key, and a more "thorough" one is not. An earlier
/// revision keyed on the adapter's canonically serialized config, reasoning
/// that aliasing one warehouse under two names should not launder a collision.
/// That is unsound in **both** directions, and the second one breaks working
/// projects:
///
/// - Two Databricks blocks identical but for `timeout_secs` — unset on one,
///   `120` on the other — serialize differently and read as two warehouses,
///   yet `registry` normalizes both with `unwrap_or(120)` to the same host.
///   A real collision, missed.
/// - Two **pathless** DuckDB blocks serialize identically and read as one
///   warehouse, yet `registry` builds `DuckDbWarehouseAdapter::in_memory()`
///   *per adapter name*, so they are two independent databases. A legitimate
///   project, refused.
///
/// A name, by contrast, is unambiguous in the direction that matters: one name
/// is one entry in `config.adapters`, hence one destination. Two names may
/// still alias one warehouse, so this fails **open** on aliasing — it misses
/// that collision rather than inventing one. Closing it needs a per-adapter
/// notion of physical destination (host + http_path, canonicalized path,
/// account + warehouse …) that does not exist yet and cannot be approximated
/// by serializing the config.
///
/// The `catalog.schema.table` half is [`rocky_sql::defer::CollisionIdentity`],
/// shared with E036 and `branch promote` rather than respelled here: two gates
/// disagreeing about what counts as one physical object is the failure this
/// exists to prevent.
///
/// `Ephemeral` models are excluded for the reason the compiler excludes them —
/// their target is populated but phantom and nothing is materialized there, so
/// one can never be the second writer.
///
/// Only cross-pipeline groups are reported. A collision *within* one pipeline
/// is E036's, and reporting it here as well would give one mistake two
/// different errors from two different layers.
fn reject_duplicate_physical_targets(
    config: &RockyConfig,
    models_by_pipeline: &ModelsByPipeline,
) -> Result<(), UnifiedDagError> {
    // Keyed by (adapter identity, folded target).
    let mut claimants: std::collections::BTreeMap<(String, String), TargetClaim> =
        Default::default();

    for (pipeline_name, models) in models_by_pipeline {
        let Some(pipeline) = config.pipelines.get(pipeline_name) else {
            continue;
        };
        let adapter_name = pipeline.target_adapter();

        for model in models {
            if matches!(
                model.config.strategy,
                crate::models::StrategyConfig::Ephemeral
            ) {
                continue;
            }
            let t = &model.config.target;
            let folded = rocky_sql::defer::CollisionIdentity::of(&t.catalog, &t.schema, &t.table)
                .to_string();
            let spelled = if t.catalog.is_empty() {
                format!("{}.{}", t.schema, t.table)
            } else {
                format!("{}.{}.{}", t.catalog, t.schema, t.table)
            };
            claimants
                .entry((adapter_name.to_string(), folded))
                .or_insert_with(|| TargetClaim {
                    spelled,
                    adapter: adapter_name.to_string(),
                    writers: Vec::new(),
                })
                .writers
                .push((pipeline_name.clone(), model.config.name.clone()));
        }
    }

    for claim in claimants.into_values() {
        let distinct_pipelines: std::collections::BTreeSet<&str> =
            claim.writers.iter().map(|(p, _)| p.as_str()).collect();
        if distinct_pipelines.len() > 1 {
            let mut writers = claim.writers;
            writers.sort();
            return Err(UnifiedDagError::DuplicatePhysicalTargetAcrossPipelines {
                target: claim.spelled,
                adapter: claim.adapter,
                claimants: writers,
            });
        }
    }
    Ok(())
}

/// Builds a unified DAG from the current configuration, loaded models, and seeds.
///
/// Each pipeline in `config.pipelines` becomes one or more nodes:
/// - **Replication** pipelines expand into a `Source` + `Load` node pair
///   (parse-layer sugar — makes the EL steps explicit in the graph).
/// - **Transformation** pipelines expand into per-model `Transformation`
///   nodes (and `Seed` / `Test` nodes when applicable).
/// - **Quality** pipelines become a single `Quality` node.
/// - **Snapshot** pipelines become a single `Snapshot` node.
/// - **Load** pipelines become a single `Load` node.
///
/// Seeds provided in `seeds` become standalone `Seed` nodes. Cross-step
/// dependencies (e.g., a model depending on a seed by name) are resolved
/// after all nodes are created.
///
/// Edges are derived from:
/// - `depends_on` at the pipeline level (inter-pipeline chaining).
/// - `depends_on` at the model level (intra-pipeline model ordering
///   plus cross-step references to seeds and loads).
/// - Implicit test-after-model relationships.
/// - Replication sugar (Source → Load within each replication pipeline).
///
/// # Model attribution
///
/// `models_by_pipeline` maps each transformation pipeline to **its own** models
/// — the ones its configured `models` location resolved to, not the project's
/// whole model set. Passing one flat list for every transformation pipeline is
/// what #1261 was: each model then got a node under every transformation
/// pipeline, all sharing the id `transformation:<model>`, and the duplicates
/// made phase computation report `circular dependency detected involving: []`.
/// The unified DAG was therefore unusable for any project with two or more
/// transformation pipelines — the standard silver/gold layering — which is also
/// the only way to run such a project, since `rocky run` refuses a multi-pipeline
/// project without `--pipeline`.
///
/// A node id stays `transformation:<model>`; it is *not* qualified by pipeline.
/// That id is a published contract (`DagNodeOutput.id`, `execution_layers`, and
/// `DagOutput.schema_version`, which orchestrators pin against), and because
/// every transformation pipeline previously received the same flat list, any
/// project that produces a valid DAG today has at most one transformation
/// pipeline with models — so no working project's ids change here.
///
/// The cost of keeping ids unqualified is that one model cannot appear under two
/// pipelines. That is refused with
/// [`UnifiedDagError::ModelClaimedByMultiplePipelines`] rather than represented.
/// `rocky run --pipeline <name>` does support it (the same model can be built
/// into two different warehouses that way), and nothing that works today is lost
/// — such a config errors out at DAG build already, just unintelligibly.
/// Representing it here would need a collision-safe occurrence identity carried
/// through runtime dependency inference, per-partition state keys, lineage
/// output, and Dagster asset keys; that is deliberately not attempted.
///
/// # Errors
///
/// Returns [`UnifiedDagError::ModelClaimedByMultiplePipelines`] when two
/// transformation pipelines resolve to the same model, plus the validation
/// errors [`validate_dag`] raises.
pub fn build_unified_dag(
    config: &RockyConfig,
    models_by_pipeline: &ModelsByPipeline,
    seeds: &[SeedFile],
) -> Result<UnifiedDag, UnifiedDagError> {
    reject_models_claimed_twice(models_by_pipeline)?;
    reject_duplicate_physical_targets(config, models_by_pipeline)?;

    // The union, for the cross-step dependency pass below. A model appears
    // under exactly one pipeline (the check above guarantees it), so this
    // neither duplicates nor drops anything.
    let all_models: Vec<&Model> = models_by_pipeline.values().flatten().collect();

    // model name -> owning transformation pipeline. Unambiguous because
    // `reject_models_claimed_twice` above guarantees one claimant per model.
    let pipeline_of: HashMap<&str, &str> = models_by_pipeline
        .iter()
        .flat_map(|(pipeline, models)| {
            models
                .iter()
                .map(move |m| (m.config.name.as_str(), pipeline.as_str()))
        })
        .collect();

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    // Track pipeline-name -> list of node IDs, so inter-pipeline depends_on
    // can wire edges from the *last* node of the upstream pipeline.
    let mut pipeline_node_ids: HashMap<String, Vec<NodeId>> = HashMap::new();

    // Logical-name -> NodeId map for cross-step dependency resolution.
    // Populated as nodes are created.
    let mut name_to_node: HashMap<String, NodeId> = HashMap::new();

    // --- Add seed nodes (pipeline-independent) ---
    for seed in seeds {
        let node_id = NodeId::new("seed", &seed.name);
        nodes.push(UnifiedNode {
            id: node_id.clone(),
            kind: NodeKind::Seed,
            label: seed.name.clone(),
            pipeline: None,
        });
        name_to_node.insert(seed.name.clone(), node_id);
    }

    for (pipeline_name, pipeline_cfg) in &config.pipelines {
        match pipeline_cfg {
            PipelineConfig::Replication(_) => {
                // Parse-layer sugar: expand replication into Source + Load.
                let source_id = NodeId::new("source", pipeline_name);
                let load_id = NodeId::new("load", pipeline_name);

                nodes.push(UnifiedNode {
                    id: source_id.clone(),
                    kind: NodeKind::Source,
                    label: format!("{pipeline_name} (source)"),
                    pipeline: Some(pipeline_name.clone()),
                });
                nodes.push(UnifiedNode {
                    id: load_id.clone(),
                    kind: NodeKind::Load,
                    label: format!("{pipeline_name} (load)"),
                    pipeline: Some(pipeline_name.clone()),
                });

                edges.push(UnifiedEdge {
                    from: source_id.clone(),
                    to: load_id.clone(),
                    edge_type: EdgeType::DataDependency,
                });

                // The Load node is the "output" of a replication pipeline.
                name_to_node.insert(pipeline_name.clone(), load_id.clone());

                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .extend([source_id, load_id]);
            }
            PipelineConfig::Transformation(_) => {
                // THIS pipeline's models, not the project's. A pipeline the
                // caller resolved no models for contributes no nodes, which is
                // how a transformation pipeline whose directory is absent or
                // empty behaves — the same no-op `run` treats it as.
                let own_models = models_by_pipeline
                    .get(pipeline_name)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                add_transformation_nodes(
                    pipeline_name,
                    own_models,
                    &mut nodes,
                    &mut edges,
                    &mut pipeline_node_ids,
                    &mut name_to_node,
                );
            }
            PipelineConfig::Quality(_) => {
                let node_id = NodeId::new("quality", pipeline_name);
                nodes.push(UnifiedNode {
                    id: node_id.clone(),
                    kind: NodeKind::Quality,
                    label: pipeline_name.clone(),
                    pipeline: Some(pipeline_name.clone()),
                });
                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .push(node_id);
            }
            PipelineConfig::Snapshot(_) => {
                let node_id = NodeId::new("snapshot", pipeline_name);
                nodes.push(UnifiedNode {
                    id: node_id.clone(),
                    kind: NodeKind::Snapshot,
                    label: pipeline_name.clone(),
                    pipeline: Some(pipeline_name.clone()),
                });
                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .push(node_id);
            }
            PipelineConfig::Load(_) => {
                let node_id = NodeId::new("load", pipeline_name);
                nodes.push(UnifiedNode {
                    id: node_id.clone(),
                    kind: NodeKind::Load,
                    label: pipeline_name.clone(),
                    pipeline: Some(pipeline_name.clone()),
                });
                name_to_node.insert(pipeline_name.clone(), node_id.clone());
                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .push(node_id);
            }
        }
    }

    // --- Resolve cross-step dependencies for models ---
    // A model's depends_on may reference seeds or other step types by name.
    // Intra-pipeline model deps were already wired in add_transformation_nodes;
    // here we wire cross-step references (seed, replication load, etc.).
    resolve_cross_step_deps(&all_models, &pipeline_of, &name_to_node, &mut edges);

    // Wire inter-pipeline depends_on edges.
    for (pipeline_name, pipeline_cfg) in &config.pipelines {
        let deps = pipeline_cfg.depends_on();
        if deps.is_empty() {
            continue;
        }

        // The downstream pipeline's "entry" nodes (first node(s) that should
        // wait on the upstream). For non-transformation pipelines this is the
        // single pipeline node. For transformation pipelines we connect to
        // model root nodes (those with no intra-pipeline dependencies).
        let downstream_ids: Vec<NodeId> = pipeline_node_ids
            .get(pipeline_name)
            .cloned()
            .unwrap_or_default();

        // Find nodes in the downstream pipeline that have no intra-pipeline
        // incoming edges — these are the entry points.
        let intra_targets: HashSet<&NodeId> = edges
            .iter()
            .filter(|e: &&UnifiedEdge| {
                downstream_ids.contains(&e.to) && downstream_ids.contains(&e.from)
            })
            .map(|e| &e.to)
            .collect();

        let entry_ids: Vec<&NodeId> = downstream_ids
            .iter()
            .filter(|id| !intra_targets.contains(id))
            // Only connect to non-test, non-seed-like primary nodes for
            // inter-pipeline edges. Tests hang off their model, not the
            // pipeline boundary.
            .filter(|id| {
                nodes
                    .iter()
                    .find(|n| n.id == **id)
                    .map(|n| n.kind != NodeKind::Test && n.kind != NodeKind::Source)
                    .unwrap_or(true)
            })
            .collect();

        for dep_pipeline in deps {
            let upstream_ids = pipeline_node_ids.get(dep_pipeline).ok_or_else(|| {
                UnifiedDagError::UnknownDependency {
                    node: pipeline_name.clone(),
                    dependency: dep_pipeline.clone(),
                }
            })?;

            // Find leaf nodes (no outgoing intra-pipeline data edges) in the
            // upstream pipeline — these must finish before the downstream
            // pipeline starts.
            let exit_ids: Vec<&NodeId> = upstream_ids
                .iter()
                .filter(|id| {
                    // A node is an exit node if it has no outgoing edges to
                    // other nodes *within the same pipeline*.
                    !edges.iter().any(|e| {
                        e.from == **id
                            && upstream_ids.contains(&e.to)
                            && e.edge_type != EdgeType::CheckDependency
                    })
                })
                // Exclude test and source nodes from exit — tests don't gate
                // downstream pipelines, and source nodes are internal to the
                // replication sugar.
                .filter(|id| {
                    nodes
                        .iter()
                        .find(|n| n.id == **id)
                        .map(|n| n.kind != NodeKind::Test && n.kind != NodeKind::Source)
                        .unwrap_or(true)
                })
                .collect();

            for upstream_id in &exit_ids {
                for downstream_id in &entry_ids {
                    edges.push(UnifiedEdge {
                        from: (*upstream_id).clone(),
                        to: (*downstream_id).clone(),
                        edge_type: EdgeType::DataDependency,
                    });
                }
            }
        }
    }

    Ok(UnifiedDag { nodes, edges })
}

/// Resolves cross-step dependencies for models.
///
/// For each model, checks if any `depends_on` entry refers to a seed, load,
/// or other non-model node via the `name_to_node` map.
///
/// `pipeline_of` gives each model's owning transformation pipeline. A dependency
/// on a model in the SAME pipeline was already wired by
/// [`add_transformation_nodes`] and is skipped here; a dependency on a model in
/// a DIFFERENT pipeline was not, and must be wired here.
///
/// That distinction is load-bearing. `add_transformation_nodes` only sees its
/// own pipeline's models, so it cannot wire `gold.fct depends_on = ["stg"]` when
/// `stg` belongs to `silver`. Skipping every name that appears anywhere in the
/// project — which is what "already wired" used to mean, back when every
/// pipeline received the whole model list — drops that edge entirely and lets
/// `fct` run alongside the `stg` it reads. Silent wrong ordering, which is worse
/// than the loud failure this fix replaced.
fn resolve_cross_step_deps(
    models: &[&Model],
    pipeline_of: &HashMap<&str, &str>,
    name_to_node: &HashMap<String, NodeId>,
    edges: &mut Vec<UnifiedEdge>,
) {
    for model in models {
        let model_id = NodeId::new("transformation", &model.config.name);
        let own_pipeline = pipeline_of.get(model.config.name.as_str()).copied();
        for dep in &model.config.depends_on {
            // Already wired intra-pipeline: same owning pipeline, model→model.
            if let Some(dep_pipeline) = pipeline_of.get(dep.as_str())
                && Some(*dep_pipeline) == own_pipeline
            {
                continue;
            }
            // A seed, a replication load, or a model in ANOTHER transformation
            // pipeline — all of which resolve through `name_to_node`.
            if let Some(dep_node_id) = name_to_node.get(dep.as_str()) {
                edges.push(UnifiedEdge {
                    from: dep_node_id.clone(),
                    to: model_id.clone(),
                    edge_type: EdgeType::DataDependency,
                });
            }
        }
    }
}

/// Expands a transformation pipeline into per-model nodes, seed nodes,
/// and test nodes, wiring intra-pipeline edges.
fn add_transformation_nodes(
    pipeline_name: &str,
    models: &[Model],
    nodes: &mut Vec<UnifiedNode>,
    edges: &mut Vec<UnifiedEdge>,
    pipeline_node_ids: &mut HashMap<String, Vec<NodeId>>,
    name_to_node: &mut HashMap<String, NodeId>,
) {
    // Build a set of model names for resolving depends_on within the pipeline.
    let model_names: HashSet<&str> = models.iter().map(|m| m.config.name.as_str()).collect();

    for model in models {
        let model_name = &model.config.name;
        let node_id = NodeId::new("transformation", model_name);

        nodes.push(UnifiedNode {
            id: node_id.clone(),
            kind: NodeKind::Transformation,
            label: model_name.clone(),
            pipeline: Some(pipeline_name.to_string()),
        });
        pipeline_node_ids
            .entry(pipeline_name.to_string())
            .or_default()
            .push(node_id.clone());
        name_to_node.insert(model_name.clone(), node_id.clone());

        // Intra-pipeline model dependencies.
        for dep in &model.config.depends_on {
            if model_names.contains(dep.as_str()) {
                let dep_id = NodeId::new("transformation", dep);
                edges.push(UnifiedEdge {
                    from: dep_id,
                    to: node_id.clone(),
                    edge_type: EdgeType::DataDependency,
                });
            }
        }

        // Declarative tests become downstream Test nodes.
        for (idx, test) in model.config.tests.iter().enumerate() {
            let test_label = format_test_label(model_name, test, idx);
            let test_id = NodeId::new("test", &test_label);

            nodes.push(UnifiedNode {
                id: test_id.clone(),
                kind: NodeKind::Test,
                label: test_label,
                pipeline: Some(pipeline_name.to_string()),
            });
            pipeline_node_ids
                .entry(pipeline_name.to_string())
                .or_default()
                .push(test_id.clone());

            edges.push(UnifiedEdge {
                from: node_id.clone(),
                to: test_id,
                edge_type: EdgeType::CheckDependency,
            });
        }
    }
}

/// Builds a deterministic label for a test node.
fn format_test_label(model_name: &str, test: &crate::tests::TestDecl, index: usize) -> String {
    use crate::tests::TestType;

    let type_str = match &test.test_type {
        TestType::NotNull => "not_null",
        TestType::Unique => "unique",
        TestType::UniqueExpr { .. } => "unique_expr",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Relationships { .. } => "relationships",
        TestType::Expression { .. } => "expression",
        TestType::RowCountRange { .. } => "row_count_range",
        TestType::InRange { .. } => "in_range",
        TestType::RegexMatch { .. } => "regex_match",
        TestType::Aggregate { .. } => "aggregate",
        TestType::Composite { .. } => "composite",
        TestType::NotInFuture => "not_in_future",
        TestType::OlderThanNDays { .. } => "older_than_n_days",
    };

    match &test.column {
        Some(col) => format!("{model_name}::{type_str}_{col}"),
        None => format!("{model_name}::{type_str}_{index}"),
    }
}

// ---------------------------------------------------------------------------
// Runtime cross-pipeline dependency inference
// ---------------------------------------------------------------------------

/// Augment a DAG with edges inferred from model SQL `FROM` references.
///
/// `build_unified_dag` resolves only edges from explicit `depends_on` config.
/// This pass parses each model's SQL, extracts the tables it references, and
/// adds [`EdgeType::DataDependency`] edges from the producing nodes (other
/// transformations, seeds, replication loads) to the consuming model — even
/// when no explicit `depends_on` is declared.
///
/// `model_sql_by_name` maps model name → compiled SQL text. The caller is
/// responsible for compiling models first; this function does no IO.
///
/// Inferred edges are de-duplicated against existing ones, so calling this
/// repeatedly is idempotent.
pub fn infer_runtime_dependencies(
    dag: &mut UnifiedDag,
    model_sql_by_name: &HashMap<String, String>,
) {
    // Build a set of producing node names (everything that creates a table:
    // transformations, seeds, loads). Maps logical table name → NodeId.
    let mut producers: HashMap<String, NodeId> = HashMap::new();
    for node in &dag.nodes {
        match node.kind {
            NodeKind::Transformation | NodeKind::Seed | NodeKind::Load | NodeKind::Replication => {
                // Index by lowercase label so case-insensitive SQL refs match.
                producers.insert(node.label.to_lowercase(), node.id.clone());
            }
            _ => {}
        }
    }

    // Existing edges as a set so we don't double-add.
    let mut existing: HashSet<(NodeId, NodeId)> = dag
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();

    let mut new_edges = Vec::new();

    for node in &dag.nodes {
        if node.kind != NodeKind::Transformation {
            continue;
        }
        let Some(sql) = model_sql_by_name.get(&node.label) else {
            continue;
        };
        let Ok(refs) = rocky_sql::lineage::referenced_tables(sql) else {
            continue;
        };
        for table_name in refs {
            // Match by the bare table name (last segment of any qualified ref).
            let bare = table_name
                .rsplit('.')
                .next()
                .unwrap_or(&table_name)
                .to_lowercase();
            let Some(producer_id) = producers.get(&bare) else {
                continue;
            };
            // Skip self-references and already-known edges.
            if *producer_id == node.id {
                continue;
            }
            let key = (producer_id.clone(), node.id.clone());
            if existing.insert(key) {
                new_edges.push(UnifiedEdge {
                    from: producer_id.clone(),
                    to: node.id.clone(),
                    edge_type: EdgeType::DataDependency,
                });
            }
        }
    }

    dag.edges.extend(new_edges);
}

// ---------------------------------------------------------------------------
// Execution phases (parallel layers)
// ---------------------------------------------------------------------------

/// Computes parallel execution phases from the unified DAG.
///
/// Returns groups of node references that can execute concurrently. Within
/// each phase, all upstream dependencies (across all edge types) have been
/// satisfied by previous phases. This is the unified-DAG equivalent of
/// [`rocky_ir::dag::execution_layers`].
///
/// Returns an error if the DAG contains a cycle.
pub fn execution_phases(dag: &UnifiedDag) -> Result<Vec<Vec<&UnifiedNode>>, UnifiedDagError> {
    // Build adjacency structures keyed by NodeId.
    let node_map: HashMap<&NodeId, &UnifiedNode> = dag.nodes.iter().map(|n| (&n.id, n)).collect();

    let mut in_degree: HashMap<&NodeId, usize> = HashMap::new();
    let mut dependents: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();
    let mut predecessors: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();

    for node in &dag.nodes {
        in_degree.entry(&node.id).or_insert(0);
    }

    for edge in &dag.edges {
        *in_degree.entry(&edge.to).or_insert(0) += 1;
        dependents.entry(&edge.from).or_default().push(&edge.to);
        predecessors.entry(&edge.to).or_default().push(&edge.from);
    }

    // Kahn's algorithm with layer tracking.
    let mut queue: Vec<&NodeId> = in_degree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();
    // Sort for deterministic output.
    queue.sort_by(|a, b| a.0.cmp(&b.0));

    let mut node_layer: HashMap<&NodeId, usize> = HashMap::new();
    let mut layers: Vec<Vec<&UnifiedNode>> = Vec::new();
    let mut processed = 0usize;

    // BFS-style processing, one "frontier" at a time.
    while !queue.is_empty() {
        let mut next_queue: Vec<&NodeId> = Vec::new();

        for id in &queue {
            // Determine the layer: max(layer of all predecessors) + 1, or 0.
            let layer = predecessors
                .get(id)
                .map(|preds| {
                    preds
                        .iter()
                        .filter_map(|pred_id| node_layer.get(pred_id))
                        .max()
                        .map(|&max_dep| max_dep + 1)
                        .unwrap_or(0)
                })
                .unwrap_or(0);

            node_layer.insert(id, layer);

            while layers.len() <= layer {
                layers.push(Vec::new());
            }

            if let Some(node) = node_map.get(id) {
                layers[layer].push(node);
            }

            processed += 1;

            if let Some(deps) = dependents.get(id) {
                for dep_id in deps {
                    if let Some(deg) = in_degree.get_mut(dep_id) {
                        *deg -= 1;
                        if *deg == 0 {
                            next_queue.push(dep_id);
                        }
                    }
                }
            }
        }

        next_queue.sort_by(|a, b| a.0.cmp(&b.0));
        queue = next_queue;
    }

    if processed != dag.nodes.len() {
        let processed_ids: HashSet<&NodeId> = node_layer.keys().copied().collect();
        let cyclic: Vec<String> = dag
            .nodes
            .iter()
            .filter(|n| !processed_ids.contains(&n.id))
            .map(|n| n.id.0.clone())
            .collect();
        return Err(UnifiedDagError::CyclicDependency { nodes: cyclic });
    }

    // Sort nodes within each layer for deterministic output.
    for layer in &mut layers {
        layer.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    }

    Ok(layers)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Validates the structural integrity of a unified DAG.
///
/// Returns a list of validation errors. An empty list means the DAG is valid.
/// Checks performed:
/// - No duplicate `NodeId`s.
/// - All edge endpoints reference existing nodes (no dangling edges).
/// - No self-loops (an edge from a node to itself).
/// - No `DataDependency` edges originating from `Test` nodes (tests don't
///   produce data for downstream steps).
/// - No cycles (detected via `execution_phases`).
pub fn validate(dag: &UnifiedDag) -> Vec<UnifiedDagError> {
    let mut errors = Vec::new();

    // 1. Duplicate node IDs.
    let mut seen_ids: HashSet<&str> = HashSet::new();
    for node in &dag.nodes {
        if !seen_ids.insert(&node.id.0) {
            errors.push(UnifiedDagError::DuplicateNodeId {
                id: node.id.0.clone(),
            });
        }
    }

    let node_ids: HashSet<&str> = dag.nodes.iter().map(|n| n.id.0.as_str()).collect();
    let node_map: HashMap<&str, &UnifiedNode> =
        dag.nodes.iter().map(|n| (n.id.0.as_str(), n)).collect();

    for edge in &dag.edges {
        // 2. Dangling edges.
        if !node_ids.contains(edge.from.0.as_str()) || !node_ids.contains(edge.to.0.as_str()) {
            errors.push(UnifiedDagError::DanglingEdge {
                from: edge.from.0.clone(),
                to: edge.to.0.clone(),
            });
            continue;
        }

        // 3. Self-loops.
        if edge.from == edge.to {
            errors.push(UnifiedDagError::SelfLoop {
                node: edge.from.0.clone(),
            });
            continue;
        }

        // 4. Test nodes must not have outgoing DataDependency edges.
        if edge.edge_type == EdgeType::DataDependency
            && let Some(from_node) = node_map.get(edge.from.0.as_str())
            && from_node.kind == NodeKind::Test
        {
            errors.push(UnifiedDagError::InvalidEdge {
                from: edge.from.0.clone(),
                to: edge.to.0.clone(),
                reason: "test nodes cannot have outgoing data dependencies".into(),
            });
        }
    }

    // 5. Cycle detection via execution_phases (which uses Kahn's algorithm).
    if let Err(e) = execution_phases(dag) {
        errors.push(e);
    }

    errors
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Attributes a flat model list to the config's single transformation
    /// pipeline — the shape every test here uses.
    ///
    /// Panics when the config has more than one, deliberately: attribution is
    /// exactly what #1261 was about, so a multi-transformation-pipeline test
    /// has to state it explicitly rather than inherit a guess.
    fn owned_by_sole_transformation(config: &RockyConfig, models: Vec<Model>) -> ModelsByPipeline {
        let names: Vec<&String> = config
            .pipelines
            .iter()
            .filter(|(_, p)| matches!(p, PipelineConfig::Transformation(_)))
            .map(|(n, _)| n)
            .collect();
        match names.as_slice() {
            [one] => ModelsByPipeline::from([((*one).clone(), models)]),
            [] => {
                assert!(
                    models.is_empty(),
                    "models supplied but the config declares no transformation pipeline"
                );
                ModelsByPipeline::new()
            }
            many => panic!(
                "{} transformation pipelines — build the ModelsByPipeline explicitly",
                many.len()
            ),
        }
    }
    use crate::config::*;
    use crate::models::{ModelConfig, StrategyConfig, TargetConfig};
    use crate::seeds::SeedConfig;
    use crate::tests::{TestDecl, TestSeverity, TestType};
    use indexmap::IndexMap;
    use std::path::PathBuf;

    /// Helper: build a minimal RockyConfig with the given pipelines.
    fn config_with_pipelines(pipelines: Vec<(&str, PipelineConfig)>) -> RockyConfig {
        let mut map = IndexMap::new();
        for (name, cfg) in pipelines {
            map.insert(name.to_string(), cfg);
        }
        RockyConfig {
            state: StateConfig::default(),
            adapters: IndexMap::new(),
            pipelines: map,
            hooks: Default::default(),
            cost: CostSection {
                storage_cost_per_gb_month: 0.023,
                compute_cost_per_dbu: 0.40,
                warehouse_size: "Medium".to_string(),
                min_history_runs: 5,
            },
            budget: Default::default(),
            schema_evolution: Default::default(),
            retry: None,
            portability: Default::default(),
            cache: Default::default(),
            mask: Default::default(),
            classifications: Default::default(),
            roles: Default::default(),
            ai: Default::default(),
            branch: Default::default(),
            freshness: Default::default(),
            imports: Default::default(),
            run: Default::default(),
            reuse: Default::default(),
            gc: Default::default(),
            policy: None,
            resilience: Default::default(),
            schedule: Default::default(),
        }
    }

    /// Helper: build a minimal replication pipeline config.
    fn repl_pipeline(depends_on: Vec<&str>) -> PipelineConfig {
        PipelineConfig::Replication(Box::new(ReplicationPipelineConfig {
            strategy: "incremental".into(),
            timestamp_column: "_fivetran_synced".into(),
            merge_keys: None,
            merge_keys_fallback: None,
            metadata_columns: vec![],
            source: PipelineSourceConfig {
                adapter: "default".into(),
                catalog: None,
                schema_pattern: SchemaPatternConfig {
                    prefix: "src__".into(),
                    separator: "__".into(),
                    components: vec!["client".into(), "regions...".into(), "connector".into()],
                },
                discovery: None,
            },
            target: PipelineTargetConfig {
                adapter: "default".into(),
                catalog_template: "{client}_warehouse".into(),
                schema_template: "raw__{regions}__{source}".into(),
                separator: None,
                governance: GovernanceConfig::default(),
            },
            checks: ChecksConfig::default(),
            execution: ExecutionConfig::default(),
            depends_on: depends_on.into_iter().map(String::from).collect(),
            table_overrides: vec![],
            prune_unchanged: false,
            schedule: None,
        }))
    }

    /// Helper: build a minimal transformation pipeline config.
    fn transform_pipeline(depends_on: Vec<&str>) -> PipelineConfig {
        PipelineConfig::Transformation(Box::new(TransformationPipelineConfig {
            models: "models/**".into(),
            target: TransformationTargetConfig {
                adapter: "default".into(),
                governance: GovernanceConfig::default(),
            },
            checks: ChecksConfig::default(),
            execution: ExecutionConfig::default(),
            depends_on: depends_on.into_iter().map(String::from).collect(),
            schedule: None,
        }))
    }

    /// Helper: build a minimal quality pipeline config.
    fn quality_pipeline(depends_on: Vec<&str>) -> PipelineConfig {
        PipelineConfig::Quality(Box::new(QualityPipelineConfig {
            target: QualityTargetConfig {
                adapter: "default".into(),
            },
            tables: vec![],
            checks: ChecksConfig {
                enabled: true,
                ..Default::default()
            },
            execution: ExecutionConfig::default(),
            depends_on: depends_on.into_iter().map(String::from).collect(),
            schedule: None,
        }))
    }

    /// Helper: build a minimal Model.
    fn model(name: &str, depends_on: Vec<&str>, tests: Vec<TestDecl>) -> Model {
        Model {
            config: ModelConfig {
                name: name.into(),
                depends_on: depends_on.into_iter().map(String::from).collect(),
                strategy: StrategyConfig::FullRefresh,
                target: TargetConfig {
                    catalog: "warehouse".into(),
                    schema: "silver".into(),
                    table: name.into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests,
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: format!("SELECT * FROM upstream_{name}"),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    /// Helper: build a minimal SeedFile.
    fn seed(name: &str) -> SeedFile {
        SeedFile {
            name: name.into(),
            file_path: PathBuf::from(format!("seeds/{name}.csv")),
            format: crate::seeds::SeedFormat::Csv,
            config: SeedConfig {
                name: Some(name.into()),
                ..Default::default()
            },
        }
    }

    // -----------------------------------------------------------------------
    // DAG construction tests — replication sugar
    // -----------------------------------------------------------------------

    #[test]
    fn test_single_replication_pipeline_expands_to_source_and_load() {
        let config = config_with_pipelines(vec![("raw_ingest", repl_pipeline(vec![]))]);
        let dag = build_unified_dag(&config, &ModelsByPipeline::new(), &[]).unwrap();

        // Replication sugar: Source + Load = 2 nodes
        assert_eq!(dag.node_count(), 2);

        let source = dag.node(&NodeId::new("source", "raw_ingest"));
        assert!(source.is_some());
        assert_eq!(source.unwrap().kind, NodeKind::Source);

        let load = dag.node(&NodeId::new("load", "raw_ingest"));
        assert!(load.is_some());
        assert_eq!(load.unwrap().kind, NodeKind::Load);

        // One internal edge: Source -> Load
        assert_eq!(dag.edge_count(), 1);
        assert_eq!(dag.edges[0].from, NodeId::new("source", "raw_ingest"));
        assert_eq!(dag.edges[0].to, NodeId::new("load", "raw_ingest"));
        assert_eq!(dag.edges[0].edge_type, EdgeType::DataDependency);
    }

    /// 🔴 #1261 regression: two transformation pipelines with DISTINCT model
    /// sets must produce one node per model, attributed to the owning pipeline.
    ///
    /// Pre-fix `build_unified_dag` handed every transformation pipeline the same
    /// flat list, so each model got a node under BOTH — duplicate ids, duplicate
    /// edges, and phase computation reporting a cycle. Measured on the release
    /// before this fix, this exact shape produced
    /// `circular dependency detected involving: ["transformation:fct",
    /// "transformation:stg", "transformation:fct", "transformation:stg"]`.
    ///
    /// This is the silver/gold layering, and `rocky run` refuses a
    /// multi-pipeline project without `--pipeline`, so `--dag` was the only way
    /// to run it and it did not work.
    ///
    /// Non-vacuous on both counts: reverting to one flat list makes this 4
    /// nodes (and `execution_phases` fail), and dropping the attribution makes
    /// the `pipeline` assertions fail.
    #[test]
    fn two_transformation_pipelines_each_own_only_their_models() {
        let config = config_with_pipelines(vec![
            ("silver", transform_pipeline(vec![])),
            ("gold", transform_pipeline(vec!["silver"])),
        ]);
        let models_by_pipeline = ModelsByPipeline::from([
            ("silver".to_string(), vec![model("stg", vec![], vec![])]),
            ("gold".to_string(), vec![model("fct", vec![], vec![])]),
        ]);

        let dag = build_unified_dag(&config, &models_by_pipeline, &[]).unwrap();

        assert_eq!(dag.node_count(), 2, "one node per model, not one per pair");
        let owner = |label: &str| {
            dag.nodes
                .iter()
                .find(|n| n.label == label)
                .unwrap_or_else(|| panic!("no node labelled {label}"))
                .pipeline
                .clone()
        };
        assert_eq!(owner("stg").as_deref(), Some("silver"));
        assert_eq!(owner("fct").as_deref(), Some("gold"));

        // Ids stay UNQUALIFIED. `DagNodeOutput.id` is a published contract that
        // orchestrators pin against via `schema_version`; qualifying them here
        // would break every consumer for no gain.
        let mut ids: Vec<String> = dag.nodes.iter().map(|n| n.id.to_string()).collect();
        ids.sort();
        assert_eq!(ids, vec!["transformation:fct", "transformation:stg"]);

        // And the graph is actually usable — this is what regressed.
        execution_phases(&dag).expect("phases must compute for a 2-transformation-pipeline DAG");
    }

    /// A model-level `depends_on` that crosses pipelines must still produce an
    /// edge, with NO pipeline-level `depends_on` to fall back on.
    ///
    /// This is the seam per-pipeline attribution nearly broke silently.
    /// `add_transformation_nodes` only sees its own pipeline's models, so it
    /// cannot wire `gold.fct -> silver.stg`; and `resolve_cross_step_deps` used
    /// to skip every dependency naming any model in the project, on the premise
    /// that intra-pipeline wiring had already handled it — true only while every
    /// pipeline received the whole model list. Together they dropped the edge
    /// entirely and let `fct` run in the same layer as the `stg` it reads.
    ///
    /// Deliberately no `transform_pipeline(vec!["silver"])`: a pipeline-level
    /// dependency would order these two anyway and hide the defect. The first
    /// version of this fix shipped without this test and the red team caught it.
    #[test]
    fn a_model_depends_on_across_pipelines_still_gets_an_edge() {
        let config = config_with_pipelines(vec![
            ("silver", transform_pipeline(vec![])),
            ("gold", transform_pipeline(vec![])),
        ]);
        let models_by_pipeline = ModelsByPipeline::from([
            ("silver".to_string(), vec![model("stg", vec![], vec![])]),
            ("gold".to_string(), vec![model("fct", vec!["stg"], vec![])]),
        ]);

        let dag = build_unified_dag(&config, &models_by_pipeline, &[]).unwrap();

        let stg = NodeId::new("transformation", "stg");
        let fct = NodeId::new("transformation", "fct");
        assert!(
            dag.edges
                .iter()
                .any(|e| e.from == stg && e.to == fct && e.edge_type == EdgeType::DataDependency),
            "gold.fct depends_on silver.stg must be an edge; edges were {:?}",
            dag.edges
        );

        // And it has to actually order them — an edge that does not separate the
        // layers would still let them run together.
        let phases = execution_phases(&dag).expect("phases");
        let layer_of = |id: &NodeId| {
            phases
                .iter()
                .position(|layer| layer.iter().any(|n| &n.id == id))
                .unwrap_or_else(|| panic!("{id} is in no layer"))
        };
        let layers: Vec<Vec<String>> = phases
            .iter()
            .map(|l| l.iter().map(|n| n.id.to_string()).collect())
            .collect();
        assert!(
            layer_of(&stg) < layer_of(&fct),
            "stg must run before fct, got layers {layers:?}"
        );
    }

    /// A model with a caller-chosen physical target, for the cross-pipeline
    /// collision tests. `model()` derives the table from the name, which is
    /// exactly what these need to override.
    fn model_targeting(name: &str, catalog: &str, schema: &str, table: &str) -> Model {
        let mut m = model(name, vec![], vec![]);
        m.config.target = TargetConfig {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        };
        m
    }

    /// A transformation pipeline writing through a named adapter.
    fn transform_pipeline_on(adapter: &str) -> PipelineConfig {
        let PipelineConfig::Transformation(mut t) = transform_pipeline(vec![]) else {
            unreachable!("transform_pipeline builds a transformation");
        };
        t.target.adapter = adapter.into();
        PipelineConfig::Transformation(t)
    }

    /// Built by deserialization rather than by naming ~30 `None` fields, so
    /// the fixture also exercises the parse path a real `rocky.toml` takes.
    fn duckdb_adapter(path: &str) -> AdapterConfig {
        serde_json::from_value(serde_json::json!({ "type": "duckdb", "path": path }))
            .expect("duckdb adapter fixture must deserialize")
    }

    /// #1301: two pipelines whose models resolve to one physical table are
    /// refused at DAG build.
    ///
    /// E036 cannot see this — `--dag` compiles each pipeline's models
    /// separately, so neither compile holds both writers. The unified DAG is
    /// the only place that does, and it previously checked duplicate model
    /// *names* only, so these became independent layer-zero nodes and fanned
    /// out concurrently against one table.
    #[test]
    fn two_pipelines_writing_one_table_on_one_adapter_are_refused() {
        let mut config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("wh")),
            ("gold", transform_pipeline_on("wh")),
        ]);
        config
            .adapters
            .insert("wh".into(), duckdb_adapter("wh.duckdb"));

        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "c", "s", "shared")],
            ),
            (
                "gold".to_string(),
                vec![model_targeting("b", "c", "s", "shared")],
            ),
        ]);

        let err = build_unified_dag(&config, &models_by_pipeline, &[])
            .expect_err("two pipelines writing one table must be refused");
        match &err {
            UnifiedDagError::DuplicatePhysicalTargetAcrossPipelines {
                target, claimants, ..
            } => {
                assert_eq!(target, "c.s.shared");
                assert_eq!(
                    claimants,
                    &vec![
                        ("gold".to_string(), "b".to_string()),
                        ("silver".to_string(), "a".to_string()),
                    ],
                    "sorted, so the message is stable"
                );
            }
            other => panic!("expected DuplicatePhysicalTargetAcrossPipelines, got {other:?}"),
        }
        let msg = err.to_string();
        assert!(
            msg.contains("'a' (pipeline 'silver')"),
            "must name both: {msg}"
        );
        assert!(
            msg.contains("'b' (pipeline 'gold')"),
            "must name both: {msg}"
        );
    }

    /// The control, and the reason this keys on adapter identity rather than
    /// `catalog.schema.table` alone: the same triple on two *different*
    /// warehouses is two different tables, and refusing it would be wrong.
    #[test]
    fn two_pipelines_writing_one_table_on_different_adapters_are_allowed() {
        let mut config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("wh_a")),
            ("gold", transform_pipeline_on("wh_b")),
        ]);
        config
            .adapters
            .insert("wh_a".into(), duckdb_adapter("a.duckdb"));
        config
            .adapters
            .insert("wh_b".into(), duckdb_adapter("b.duckdb"));

        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "c", "s", "shared")],
            ),
            (
                "gold".to_string(),
                vec![model_targeting("b", "c", "s", "shared")],
            ),
        ]);

        build_unified_dag(&config, &models_by_pipeline, &[])
            .expect("the same triple on two different warehouses is two different tables");
    }

    /// Two **pathless** DuckDB adapters are two databases, not one.
    ///
    /// This is the case that killed the earlier canonical-config key: pathless
    /// blocks serialize identically, so keying on the serialized config read
    /// them as one warehouse and refused a legitimate project. `registry`
    /// builds `DuckDbWarehouseAdapter::in_memory()` per adapter *name*, so
    /// they are genuinely independent.
    #[test]
    fn two_pathless_duckdb_adapters_are_two_warehouses() {
        let pathless = || -> AdapterConfig {
            serde_json::from_value(serde_json::json!({ "type": "duckdb" }))
                .expect("pathless duckdb fixture")
        };
        let mut config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("mem_a")),
            ("gold", transform_pipeline_on("mem_b")),
        ]);
        config.adapters.insert("mem_a".into(), pathless());
        config.adapters.insert("mem_b".into(), pathless());

        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "main", "s", "shared")],
            ),
            (
                "gold".to_string(),
                vec![model_targeting("b", "main", "s", "shared")],
            ),
        ]);

        build_unified_dag(&config, &models_by_pipeline, &[])
            .expect("two in-memory databases are two warehouses, not one");
    }

    /// Case-variant targets on one adapter are one physical table.
    ///
    /// The `catalog.schema.table` half is `CollisionIdentity`, shared with
    /// E036 and `branch promote`. Without the fold, every fixture here uses a
    /// single spelling and a case-sensitive key would pass them all.
    #[test]
    fn case_variant_targets_on_one_adapter_still_collide() {
        let mut config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("wh")),
            ("gold", transform_pipeline_on("wh")),
        ]);
        config
            .adapters
            .insert("wh".into(), duckdb_adapter("wh.duckdb"));

        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "C", "S", "Shared")],
            ),
            (
                "gold".to_string(),
                vec![model_targeting("b", "c", "s", "shared")],
            ),
        ]);

        assert!(
            build_unified_dag(&config, &models_by_pipeline, &[]).is_err(),
            "two spellings of one warehouse object are one table"
        );
    }

    /// An adapter name with no matching block still groups by that name.
    ///
    /// Two pipelines naming the same missing adapter collide with each other;
    /// two naming different missing adapters do not. Neither project runs —
    /// but `rocky dag` does not run the config validation that reports the
    /// missing adapter, so this path is reachable and must not panic or
    /// silently merge unrelated pipelines.
    #[test]
    fn an_unresolvable_adapter_name_still_groups_by_name() {
        let config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("ghost")),
            ("gold", transform_pipeline_on("ghost")),
        ]);
        // Deliberately no `adapters` entry for "ghost".
        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "c", "s", "shared")],
            ),
            (
                "gold".to_string(),
                vec![model_targeting("b", "c", "s", "shared")],
            ),
        ]);
        assert!(
            build_unified_dag(&config, &models_by_pipeline, &[]).is_err(),
            "one missing adapter name is still one destination"
        );

        let split = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("ghost_a")),
            ("gold", transform_pipeline_on("ghost_b")),
        ]);
        build_unified_dag(&split, &models_by_pipeline, &[])
            .expect("different adapter names are different destinations");
    }

    /// An `ephemeral` model cannot be the second writer, so it cannot collide.
    ///
    /// Its target is populated but phantom and nothing is materialized there —
    /// the same reason the compiler excludes ephemerals from E036. Counting
    /// one here would refuse a project that races nothing.
    #[test]
    fn an_ephemeral_model_does_not_collide_across_pipelines() {
        let mut config = config_with_pipelines(vec![
            ("silver", transform_pipeline_on("wh")),
            ("gold", transform_pipeline_on("wh")),
        ]);
        config
            .adapters
            .insert("wh".into(), duckdb_adapter("wh.duckdb"));

        let mut ephemeral = model_targeting("b", "c", "s", "shared");
        ephemeral.config.strategy = StrategyConfig::Ephemeral;

        let models_by_pipeline = ModelsByPipeline::from([
            (
                "silver".to_string(),
                vec![model_targeting("a", "c", "s", "shared")],
            ),
            ("gold".to_string(), vec![ephemeral]),
        ]);

        build_unified_dag(&config, &models_by_pipeline, &[])
            .expect("an ephemeral model materializes nothing and cannot race");
    }

    /// A collision *within* one pipeline is E036's, not this check's.
    ///
    /// Reporting it here as well would give one mistake two different errors
    /// from two different layers, and the compile-time one is both earlier and
    /// better placed to explain it.
    #[test]
    fn a_collision_inside_one_pipeline_is_left_to_the_compiler() {
        let mut config = config_with_pipelines(vec![("silver", transform_pipeline_on("wh"))]);
        config
            .adapters
            .insert("wh".into(), duckdb_adapter("wh.duckdb"));

        let models_by_pipeline = ModelsByPipeline::from([(
            "silver".to_string(),
            vec![
                model_targeting("a", "c", "s", "shared"),
                model_targeting("b", "c", "s", "shared"),
            ],
        )]);

        build_unified_dag(&config, &models_by_pipeline, &[])
            .expect("a same-pipeline collision is E036's to report, not this one's");
    }

    /// The other half of #1261: two transformation pipelines resolving to the
    /// SAME model (most easily by both taking the `models/**` default) is
    /// refused BY NAME rather than represented.
    ///
    /// The DAG keys a transformation node by model name, so it cannot build one
    /// model under two pipelines. `rocky run --pipeline <name>` does support
    /// that (verified: the same model materializes into two different
    /// warehouses), so the capability is not lost — it just is not expressible
    /// in one graph, and saying so beats
    /// `circular dependency detected involving: []`.
    #[test]
    fn a_model_claimed_by_two_transformation_pipelines_is_refused_by_name() {
        let config = config_with_pipelines(vec![
            ("silver", transform_pipeline(vec![])),
            ("gold", transform_pipeline(vec![])),
        ]);
        // Both pipelines resolved the same directory, so both claim `shared`.
        let models_by_pipeline = ModelsByPipeline::from([
            ("silver".to_string(), vec![model("shared", vec![], vec![])]),
            ("gold".to_string(), vec![model("shared", vec![], vec![])]),
        ]);

        let err = build_unified_dag(&config, &models_by_pipeline, &[])
            .expect_err("a doubly-claimed model must be refused, not silently duplicated");

        match &err {
            UnifiedDagError::ModelClaimedByMultiplePipelines { model, pipelines } => {
                assert_eq!(model, "shared");
                // Sorted, so the message is stable across runs.
                assert_eq!(pipelines, &vec!["gold".to_string(), "silver".to_string()]);
            }
            other => panic!("expected ModelClaimedByMultiplePipelines, got {other:?}"),
        }

        // The message has to name both pipelines and point at the way out,
        // because the whole complaint in #1261 was an error that named nothing.
        let msg = err.to_string();
        assert!(msg.contains("gold and silver"), "must name both: {msg}");
        assert!(msg.contains("--pipeline"), "must offer the way out: {msg}");
    }

    #[test]
    fn test_pipeline_chaining_with_replication_sugar() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![model("stg_orders", vec![], vec![])];
        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        // 2 replication nodes (source + load) + 1 transformation = 3 nodes
        assert_eq!(dag.node_count(), 3);

        // Edges: source -> load (internal), load -> stg_orders (inter-pipeline)
        let data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataDependency)
            .collect();
        assert_eq!(data_edges.len(), 2);

        // The load node of raw_ingest connects to stg_orders
        let inter_pipeline: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.to == NodeId::new("transformation", "stg_orders"))
            .collect();
        assert_eq!(inter_pipeline.len(), 1);
        assert_eq!(inter_pipeline[0].from, NodeId::new("load", "raw_ingest"));
    }

    // -----------------------------------------------------------------------
    // DAG construction tests — models
    // -----------------------------------------------------------------------

    #[test]
    fn test_model_dependencies() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("stg_customers", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "stg_customers"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        assert_eq!(dag.node_count(), 3);

        // Two intra-pipeline data edges
        let data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataDependency)
            .collect();
        assert_eq!(data_edges.len(), 2);

        // fct_orders depends on stg_orders and stg_customers
        let fct_incoming: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.to == NodeId::new("transformation", "fct_orders"))
            .collect();
        assert_eq!(fct_incoming.len(), 2);
    }

    #[test]
    fn test_model_with_tests() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let test_decls = vec![
            TestDecl {
                test_type: TestType::NotNull,
                column: Some("order_id".into()),
                severity: TestSeverity::Error,
                filter: None,
            },
            TestDecl {
                test_type: TestType::Unique,
                column: Some("order_id".into()),
                severity: TestSeverity::Error,
                filter: None,
            },
        ];

        let models = vec![model("stg_orders", vec![], test_decls)];
        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        // 1 model + 2 test nodes
        assert_eq!(dag.node_count(), 3);

        let test_nodes: Vec<_> = dag
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Test)
            .collect();
        assert_eq!(test_nodes.len(), 2);

        // Both tests have check edges from the model
        let check_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::CheckDependency)
            .collect();
        assert_eq!(check_edges.len(), 2);
        for edge in &check_edges {
            assert_eq!(edge.from, NodeId::new("transformation", "stg_orders"));
        }
    }

    #[test]
    fn test_unknown_pipeline_dependency() {
        let config =
            config_with_pipelines(vec![("silver", transform_pipeline(vec!["nonexistent"]))]);

        let models = vec![model("stg_orders", vec![], vec![])];
        let result = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        );
        assert!(matches!(
            result,
            Err(UnifiedDagError::UnknownDependency { .. })
        ));
    }

    #[test]
    fn test_empty_config() {
        let config = config_with_pipelines(vec![]);
        let dag = build_unified_dag(&config, &ModelsByPipeline::new(), &[]).unwrap();
        assert!(dag.nodes.is_empty());
        assert!(dag.edges.is_empty());
    }

    #[test]
    fn test_mixed_pipeline_types() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
            ("nightly_dq", quality_pipeline(vec!["silver"])),
        ]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("dim_customers", vec![], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        // 2 replication (source + load) + 2 transformation + 1 quality = 5 nodes
        assert_eq!(dag.node_count(), 5);

        // Inter-pipeline edges:
        //   source -> load (replication internal)
        //   load -> stg_orders, load -> dim_customers (repl -> transform)
        //   stg_orders -> nightly_dq, dim_customers -> nightly_dq (transform -> quality)
        let data_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataDependency)
            .collect();
        assert_eq!(data_edges.len(), 5);
    }

    // -----------------------------------------------------------------------
    // Seed node tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_seed_nodes_created() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);
        let seeds = vec![seed("dim_date"), seed("country_codes")];
        let dag = build_unified_dag(&config, &ModelsByPipeline::new(), &seeds).unwrap();

        // 2 seed nodes, no models
        assert_eq!(dag.node_count(), 2);

        let seed_nodes: Vec<_> = dag
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Seed)
            .collect();
        assert_eq!(seed_nodes.len(), 2);

        // Seeds have no pipeline association
        assert!(seed_nodes.iter().all(|n| n.pipeline.is_none()));
    }

    // -----------------------------------------------------------------------
    // Cross-step dependency tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_model_depends_on_seed() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);
        let seeds = vec![seed("dim_date")];
        let models = vec![model("fct_orders", vec!["dim_date"], vec![])];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();

        // 1 seed + 1 model = 2 nodes
        assert_eq!(dag.node_count(), 2);

        // Cross-step edge: seed:dim_date -> transformation:fct_orders
        assert_eq!(dag.edge_count(), 1);
        assert_eq!(dag.edges[0].from, NodeId::new("seed", "dim_date"));
        assert_eq!(dag.edges[0].to, NodeId::new("transformation", "fct_orders"));
        assert_eq!(dag.edges[0].edge_type, EdgeType::DataDependency);
    }

    #[test]
    fn test_model_depends_on_replication_load() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec![])),
        ]);

        // Model explicitly depends on raw_ingest (the replication pipeline name).
        let models = vec![model("stg_orders", vec!["raw_ingest"], vec![])];
        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        // Cross-step edge: load:raw_ingest -> transformation:stg_orders
        let cross_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.to == NodeId::new("transformation", "stg_orders"))
            .collect();
        assert_eq!(cross_edges.len(), 1);
        assert_eq!(cross_edges[0].from, NodeId::new("load", "raw_ingest"));
    }

    #[test]
    fn test_model_depends_on_seed_and_model() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);
        let seeds = vec![seed("dim_date")];
        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "dim_date"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();

        // fct_orders has two incoming edges: one from stg_orders, one from dim_date
        let fct_incoming: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.to == NodeId::new("transformation", "fct_orders"))
            .collect();
        assert_eq!(fct_incoming.len(), 2);

        let from_ids: HashSet<&NodeId> = fct_incoming.iter().map(|e| &e.from).collect();
        assert!(from_ids.contains(&NodeId::new("transformation", "stg_orders")));
        assert!(from_ids.contains(&NodeId::new("seed", "dim_date")));
    }

    #[test]
    fn test_full_elt_chain() {
        // Full chain: replication -> seed + transformation -> quality
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
            ("nightly_dq", quality_pipeline(vec!["silver"])),
        ]);

        let seeds = vec![seed("dim_date")];
        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "dim_date"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();

        // source + load + 2 models + 1 seed + 1 quality = 6 nodes
        assert_eq!(dag.node_count(), 6);

        // Verify the DAG is valid
        let errors = validate(&dag);
        assert!(
            errors.is_empty(),
            "unexpected validation errors: {errors:?}"
        );

        // Verify execution phases work
        let phases = execution_phases(&dag).unwrap();
        assert!(phases.len() >= 3); // at least: source, load+seed, models, quality
    }

    // -----------------------------------------------------------------------
    // Execution phase tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_phases_linear_chain_with_replication_sugar() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![model("stg_orders", vec![], vec![])];
        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        // Phase 0: source, Phase 1: load, Phase 2: stg_orders
        assert_eq!(phases.len(), 3);
        assert_eq!(phases[0][0].kind, NodeKind::Source);
        assert_eq!(phases[1][0].kind, NodeKind::Load);
        assert_eq!(phases[2][0].kind, NodeKind::Transformation);
    }

    #[test]
    fn test_phases_parallel_models() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("stg_customers", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "stg_customers"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        assert_eq!(phases.len(), 2);
        // stg_orders and stg_customers in parallel
        assert_eq!(phases[0].len(), 2);
        // fct_orders in the next phase
        assert_eq!(phases[1].len(), 1);
        assert_eq!(phases[1][0].label, "fct_orders");
    }

    #[test]
    fn test_phases_with_tests() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let test_decls = vec![TestDecl {
            test_type: TestType::NotNull,
            column: Some("id".into()),
            severity: TestSeverity::Error,
            filter: None,
        }];

        let models = vec![model("stg_orders", vec![], test_decls)];
        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        // Phase 0: stg_orders, Phase 1: test node
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0][0].kind, NodeKind::Transformation);
        assert_eq!(phases[1][0].kind, NodeKind::Test);
    }

    #[test]
    fn test_phases_all_independent() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("a", vec![], vec![]),
            model("b", vec![], vec![]),
            model("c", vec![], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        // All in one phase
        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].len(), 3);
    }

    #[test]
    fn test_phases_diamond() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("a", vec![], vec![]),
            model("b", vec!["a"], vec![]),
            model("c", vec!["a"], vec![]),
            model("d", vec!["b", "c"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        assert_eq!(phases.len(), 3);
        assert_eq!(phases[0].len(), 1); // a
        assert_eq!(phases[0][0].label, "a");
        assert_eq!(phases[1].len(), 2); // b, c parallel
        assert_eq!(phases[2].len(), 1); // d
        assert_eq!(phases[2][0].label, "d");
    }

    #[test]
    fn test_phases_with_seeds() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);
        let seeds = vec![seed("dim_date")];
        let models = vec![model("fct_orders", vec!["dim_date"], vec![])];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();
        let phases = execution_phases(&dag).unwrap();

        // Phase 0: seed, Phase 1: model
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0][0].kind, NodeKind::Seed);
        assert_eq!(phases[1][0].kind, NodeKind::Transformation);
    }

    // -----------------------------------------------------------------------
    // DAG query tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_dag_roots_and_leaves_with_sugar() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("fct_orders", vec!["stg_orders"], vec![]),
        ];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        let roots = dag.roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], &NodeId::new("source", "raw_ingest"));

        let leaves = dag.leaves();
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0], &NodeId::new("transformation", "fct_orders"));
    }

    #[test]
    fn test_node_id_format() {
        let id = NodeId::new("transformation", "stg_orders");
        assert_eq!(id.0, "transformation:stg_orders");
        assert_eq!(id.to_string(), "transformation:stg_orders");
    }

    #[test]
    fn test_test_label_with_column() {
        let test = TestDecl {
            test_type: TestType::NotNull,
            column: Some("order_id".into()),
            severity: TestSeverity::Error,
            filter: None,
        };
        let label = format_test_label("stg_orders", &test, 0);
        assert_eq!(label, "stg_orders::not_null_order_id");
    }

    #[test]
    fn test_test_label_without_column() {
        let test = TestDecl {
            test_type: TestType::RowCountRange {
                min: Some(1),
                max: None,
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let label = format_test_label("stg_orders", &test, 2);
        assert_eq!(label, "stg_orders::row_count_range_2");
    }

    #[test]
    fn test_outgoing_and_incoming_edges() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![model("a", vec![], vec![]), model("b", vec!["a"], vec![])];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &[],
        )
        .unwrap();

        let a_id = NodeId::new("transformation", "a");
        let b_id = NodeId::new("transformation", "b");

        let a_out = dag.outgoing_edges(&a_id);
        assert_eq!(a_out.len(), 1);
        assert_eq!(a_out[0].to, b_id);

        let b_in = dag.incoming_edges(&b_id);
        assert_eq!(b_in.len(), 1);
        assert_eq!(b_in[0].from, a_id);

        // a has no incoming, b has no outgoing
        assert!(dag.incoming_edges(&a_id).is_empty());
        assert!(dag.outgoing_edges(&b_id).is_empty());
    }

    #[test]
    fn test_dag_summary() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);
        let seeds = vec![seed("dim_date")];
        let models = vec![model("stg_orders", vec![], vec![])];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();
        let summary = dag.summary();

        assert_eq!(summary.total_nodes, 4); // source + load + seed + model
        assert_eq!(summary.counts_by_kind[&NodeKind::Source], 1);
        assert_eq!(summary.counts_by_kind[&NodeKind::Load], 1);
        assert_eq!(summary.counts_by_kind[&NodeKind::Seed], 1);
        assert_eq!(summary.counts_by_kind[&NodeKind::Transformation], 1);
    }

    // -----------------------------------------------------------------------
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_valid_dag() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);
        let seeds = vec![seed("dim_date")];
        let models = vec![model("stg_orders", vec!["dim_date"], vec![])];

        let dag = build_unified_dag(
            &config,
            &owned_by_sole_transformation(&config, models.clone()),
            &seeds,
        )
        .unwrap();
        let errors = validate(&dag);
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");
    }

    #[test]
    fn test_validate_duplicate_node_ids() {
        let dag = UnifiedDag {
            nodes: vec![
                UnifiedNode {
                    id: NodeId::new("transformation", "a"),
                    kind: NodeKind::Transformation,
                    label: "a".into(),
                    pipeline: None,
                },
                UnifiedNode {
                    id: NodeId::new("transformation", "a"),
                    kind: NodeKind::Transformation,
                    label: "a_dup".into(),
                    pipeline: None,
                },
            ],
            edges: vec![],
        };

        let errors = validate(&dag);
        assert!(!errors.is_empty());
        assert!(errors.iter().any(|e| matches!(
            e,
            UnifiedDagError::DuplicateNodeId { id } if id == "transformation:a"
        )));
    }

    #[test]
    fn test_validate_dangling_edge() {
        let dag = UnifiedDag {
            nodes: vec![UnifiedNode {
                id: NodeId::new("transformation", "a"),
                kind: NodeKind::Transformation,
                label: "a".into(),
                pipeline: None,
            }],
            edges: vec![UnifiedEdge {
                from: NodeId::new("transformation", "a"),
                to: NodeId::new("transformation", "nonexistent"),
                edge_type: EdgeType::DataDependency,
            }],
        };

        let errors = validate(&dag);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, UnifiedDagError::DanglingEdge { .. }))
        );
    }

    #[test]
    fn test_validate_self_loop() {
        let dag = UnifiedDag {
            nodes: vec![UnifiedNode {
                id: NodeId::new("transformation", "a"),
                kind: NodeKind::Transformation,
                label: "a".into(),
                pipeline: None,
            }],
            edges: vec![UnifiedEdge {
                from: NodeId::new("transformation", "a"),
                to: NodeId::new("transformation", "a"),
                edge_type: EdgeType::DataDependency,
            }],
        };

        let errors = validate(&dag);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, UnifiedDagError::SelfLoop { .. }))
        );
    }

    #[test]
    fn test_validate_test_data_dependency() {
        // A test node should not have an outgoing data dependency.
        let dag = UnifiedDag {
            nodes: vec![
                UnifiedNode {
                    id: NodeId::new("test", "t1"),
                    kind: NodeKind::Test,
                    label: "t1".into(),
                    pipeline: None,
                },
                UnifiedNode {
                    id: NodeId::new("transformation", "a"),
                    kind: NodeKind::Transformation,
                    label: "a".into(),
                    pipeline: None,
                },
            ],
            edges: vec![UnifiedEdge {
                from: NodeId::new("test", "t1"),
                to: NodeId::new("transformation", "a"),
                edge_type: EdgeType::DataDependency,
            }],
        };

        let errors = validate(&dag);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, UnifiedDagError::InvalidEdge { .. }))
        );
    }

    #[test]
    fn test_validate_cycle_detected() {
        let dag = UnifiedDag {
            nodes: vec![
                UnifiedNode {
                    id: NodeId::new("transformation", "a"),
                    kind: NodeKind::Transformation,
                    label: "a".into(),
                    pipeline: None,
                },
                UnifiedNode {
                    id: NodeId::new("transformation", "b"),
                    kind: NodeKind::Transformation,
                    label: "b".into(),
                    pipeline: None,
                },
            ],
            edges: vec![
                UnifiedEdge {
                    from: NodeId::new("transformation", "a"),
                    to: NodeId::new("transformation", "b"),
                    edge_type: EdgeType::DataDependency,
                },
                UnifiedEdge {
                    from: NodeId::new("transformation", "b"),
                    to: NodeId::new("transformation", "a"),
                    edge_type: EdgeType::DataDependency,
                },
            ],
        };

        let errors = validate(&dag);
        assert!(
            errors
                .iter()
                .any(|e| matches!(e, UnifiedDagError::CyclicDependency { .. }))
        );
    }

    // ---------- infer_runtime_dependencies ----------

    fn dag_with_models(models: &[(&str, NodeKind)]) -> UnifiedDag {
        let nodes = models
            .iter()
            .map(|(name, kind)| UnifiedNode {
                id: NodeId::new(&kind.to_string(), name),
                kind: *kind,
                label: (*name).to_string(),
                pipeline: None,
            })
            .collect();
        UnifiedDag {
            nodes,
            edges: Vec::new(),
        }
    }

    #[test]
    fn test_infer_adds_edge_from_sql_ref() {
        let mut dag = dag_with_models(&[
            ("orders", NodeKind::Transformation),
            ("stg_orders", NodeKind::Transformation),
        ]);

        let mut sql = HashMap::new();
        sql.insert("stg_orders".into(), "SELECT * FROM orders".into());

        infer_runtime_dependencies(&mut dag, &sql);

        assert_eq!(dag.edges.len(), 1);
        assert_eq!(dag.edges[0].from, NodeId::new("transformation", "orders"));
        assert_eq!(dag.edges[0].to, NodeId::new("transformation", "stg_orders"));
        assert_eq!(dag.edges[0].edge_type, EdgeType::DataDependency);
    }

    #[test]
    fn test_infer_handles_qualified_names() {
        let mut dag = dag_with_models(&[
            ("customers", NodeKind::Seed),
            ("dim_customer", NodeKind::Transformation),
        ]);

        let mut sql = HashMap::new();
        sql.insert(
            "dim_customer".into(),
            "SELECT * FROM main.raw.customers".into(),
        );

        infer_runtime_dependencies(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 1);
        assert_eq!(dag.edges[0].from, NodeId::new("seed", "customers"));
    }

    #[test]
    fn test_infer_skips_existing_edges() {
        let mut dag = dag_with_models(&[
            ("orders", NodeKind::Transformation),
            ("stg_orders", NodeKind::Transformation),
        ]);
        // Pre-existing edge.
        dag.edges.push(UnifiedEdge {
            from: NodeId::new("transformation", "orders"),
            to: NodeId::new("transformation", "stg_orders"),
            edge_type: EdgeType::DataDependency,
        });

        let mut sql = HashMap::new();
        sql.insert("stg_orders".into(), "SELECT * FROM orders".into());

        infer_runtime_dependencies(&mut dag, &sql);

        // No duplicate added.
        assert_eq!(dag.edges.len(), 1);
    }

    #[test]
    fn test_infer_ignores_self_reference() {
        let mut dag = dag_with_models(&[("loop_model", NodeKind::Transformation)]);
        let mut sql = HashMap::new();
        sql.insert("loop_model".into(), "SELECT * FROM loop_model".into());

        infer_runtime_dependencies(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 0);
    }

    #[test]
    fn test_infer_idempotent() {
        let mut dag = dag_with_models(&[
            ("orders", NodeKind::Load),
            ("stg_orders", NodeKind::Transformation),
        ]);

        let mut sql = HashMap::new();
        sql.insert("stg_orders".into(), "SELECT * FROM orders".into());

        infer_runtime_dependencies(&mut dag, &sql);
        infer_runtime_dependencies(&mut dag, &sql);

        assert_eq!(dag.edges.len(), 1);
    }

    #[test]
    fn test_infer_no_match_for_unknown_table() {
        let mut dag = dag_with_models(&[("model_a", NodeKind::Transformation)]);

        let mut sql = HashMap::new();
        sql.insert(
            "model_a".into(),
            "SELECT * FROM nonexistent_external_table".into(),
        );

        infer_runtime_dependencies(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 0);
    }

    #[test]
    fn test_infer_handles_invalid_sql_gracefully() {
        let mut dag = dag_with_models(&[("model_a", NodeKind::Transformation)]);
        let mut sql = HashMap::new();
        sql.insert("model_a".into(), "this is not sql".into());

        // Should not panic; invalid SQL just yields no inferred edges.
        infer_runtime_dependencies(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 0);
    }
}
