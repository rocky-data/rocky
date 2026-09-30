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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{AdapterConfig, PipelineConfig, RockyConfig};
use crate::models::Model;
use crate::physical_edges::{
    DerivedPhysicalEdges, PhysicalEdgeModel, derivation_warnings, fold_identifier,
};
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

    /// A model reads a name that more than one producer claims — a model and
    /// a seed or load pipeline share a label — and the read does not name
    /// exactly one of them by its physical target. See [`build_runtime_dag`].
    #[error(
        "model '{reader}' reads '{read}', but the label '{label}' belongs to {}. The read does \
         not name exactly one of them by its full `catalog.schema.table` target, or another \
         one could be the same table. Rocky cannot tell which one '{reader}' must run after, \
         and a wrong choice could let it read a table that is not built yet. Rename one of the \
         producers so each label is unique, or write the read as the intended producer's full \
         target. A `?` marks a part of a target Rocky does not know: declare it in the seed's \
         sidecar `[target]`, or set `table` on the load pipeline.",
        .producers.join(" and ")
    )]
    AmbiguousLabelProducer {
        /// The lowercased label the read matched.
        label: String,
        /// The model that issues the read.
        reader: String,
        /// The read as the SQL spells it (lowercased by the extractor).
        read: String,
        /// Every producer of the label, described with its target, sorted so
        /// the message is deterministic.
        producers: Vec<String>,
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
// Runtime dependency inference
// ---------------------------------------------------------------------------

/// The graph `rocky run --dag` executes: the unified DAG plus every ordering
/// edge Rocky can infer from what each model reads, and what that inference
/// could not settle.
#[derive(Debug)]
pub struct RuntimeDag {
    pub dag: UnifiedDag,
    /// Scheduling warnings, ready to show. `[run] strict_scheduling` turns a
    /// non-empty list into a refusal.
    pub warnings: Vec<String>,
    /// What the physical-read pass derived and what it declined to guess.
    pub physical: DerivedPhysicalEdges,
    /// What the label pass found: collisions, unparsed models, skipped edges.
    pub labels: LabelInferenceReport,
}

/// Build the unified DAG and infer every runtime ordering edge on it, in one
/// place and in one order.
///
/// `rocky run --dag` and a governed `rocky apply` of a `--dag` plan both build
/// their graph here, so they cannot see different edges.
///
/// `default_catalog_of` names the catalog a warehouse resolves a catalogless
/// `[target]` in, given the adapter's config — `None` when it cannot say,
/// which is the answer for every adapter but DuckDB. See
/// [`PhysicalEdgeModel::effective_catalog`] for why it is asked for at all.
///
/// `seed_default_catalog` is the catalog the seed loader gives a seed whose
/// sidecar names none, for the pipeline the seed nodes run under — `None` when
/// no single pipeline could be chosen, in which case those seeds fail when they
/// are dispatched and their catalog is unknown here.
///
/// # Precedence
///
/// Edges join the graph in passes, strongest evidence first, and a later pass
/// may only skip an edge that would contradict what an earlier one settled. A
/// guess therefore never displaces an exact edge, and that does not depend on
/// the order names sort in.
///
/// 1. **Declared** — `depends_on` and pipeline chaining ([`build_unified_dag`]).
/// 2. **Physical reads** — a model reads another model's `[target]` by name:
///    exact three-part and two-part reads, then the catalog fallback, then
///    bare-name guesses ([`crate::physical_edges::derive_physical_edges`]).
/// 3. **Label reads** — a model reads a name that is a model's, seed's or
///    load's label. This is the weakest evidence: it matches the last segment
///    of the read and ignores its schema and catalog. A label edge that would
///    close a cycle through a physical edge is skipped and reported. A cycle
///    made only of declared and label edges is not skipped — it is a genuine
///    cycle, and it keeps its loud refusal.
///
/// # Label collisions
///
/// When a model and a seed or load pipeline share a label, node build order
/// says nothing about which one a reader means, so it is not consulted. A read
/// is ordered after the producer whose full `catalog.schema.table` target it
/// names, provided no other producer of the label could be the same table. A
/// producer whose target is not fully known (a load with no explicit `table`,
/// or a seed whose catalog is unknown because no single pipeline could be
/// chosen for the seed nodes) is not ruled out by a read that does not
/// contradict it. A read that names none of them adds no label edge. A read
/// that could name several, or one while another could be the same table, is refused with
/// [`UnifiedDagError::AmbiguousLabelProducer`] before anything runs.
///
/// # Errors
///
/// Everything [`build_unified_dag`] refuses, plus
/// [`UnifiedDagError::AmbiguousLabelProducer`].
pub fn build_runtime_dag(
    config: &RockyConfig,
    models_by_pipeline: &ModelsByPipeline,
    seeds: &[SeedFile],
    seed_default_catalog: Option<&str>,
    default_catalog_of: &dyn Fn(&AdapterConfig) -> Option<String>,
) -> Result<RuntimeDag, UnifiedDagError> {
    let mut dag = build_unified_dag(config, models_by_pipeline, seeds)?;

    // The catalog a catalogless `[target]` resolves in is a property of the
    // adapter the target writes through, so it is asked per adapter name.
    let established = |adapter: &str| -> Option<String> {
        config.adapters.get(adapter).and_then(default_catalog_of)
    };
    let pipeline_catalog: HashMap<&str, Option<String>> = models_by_pipeline
        .keys()
        .map(|pipeline| {
            let catalog = config
                .pipelines
                .get(pipeline.as_str())
                .and_then(|p| established(p.target_adapter()));
            (pipeline.as_str(), catalog)
        })
        .collect();

    let inputs: Vec<PhysicalEdgeModel<'_>> = models_by_pipeline
        .iter()
        .flat_map(|(pipeline, models)| {
            let catalog = pipeline_catalog
                .get(pipeline.as_str())
                .and_then(Option::as_deref);
            let adapter = config
                .pipelines
                .get(pipeline.as_str())
                .map(PipelineConfig::target_adapter);
            models.iter().map(move |m| {
                let input = PhysicalEdgeModel::from_model(m).with_effective_catalog(catalog);
                match adapter {
                    Some(adapter) => input.with_adapter(adapter),
                    None => input,
                }
            })
        })
        .collect();

    // Pass 2: physical reads.
    let physical = infer_physical_dependencies(&mut dag, &inputs);
    let physical_edges: HashSet<(NodeId, NodeId)> = {
        let by_label: HashMap<&str, &NodeId> = dag
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Transformation)
            .map(|n| (n.label.as_str(), &n.id))
            .collect();
        physical
            .edges
            .iter()
            .filter_map(|(consumer, producer)| {
                let from = (*by_label.get(producer.as_str())?).clone();
                let to = (*by_label.get(consumer.as_str())?).clone();
                Some((from, to))
            })
            .collect()
    };

    // Pass 3: label reads.
    let sql_by_name: HashMap<String, String> = models_by_pipeline
        .values()
        .flatten()
        .map(|m| (m.config.name.clone(), m.sql.clone()))
        .collect();
    let targets = producer_targets(
        &dag,
        config,
        models_by_pipeline,
        seeds,
        seed_default_catalog,
        &pipeline_catalog,
        &established,
    );
    let labels = infer_label_dependencies(&mut dag, &sql_by_name, &targets, &physical_edges)?;

    let mut warnings = labels.warnings();
    warnings.extend(derivation_warnings(&physical));
    Ok(RuntimeDag {
        dag,
        warnings,
        physical,
        labels,
    })
}

/// Where a producing node writes, as far as that is declared or established.
///
/// A component that is not declared, or cannot be established, is `None`: it
/// is UNKNOWN, which is not the same as "different". A read can rule a producer
/// out only by a component that is known and does not match. See
/// [`ProducerTarget::match_read`].
#[derive(Debug, Clone, Default)]
struct ProducerTarget {
    catalog: Option<String>,
    schema: Option<String>,
    table: Option<String>,
}

/// How one read relates to one producer's target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadMatch {
    /// Every component the read names is known for the producer and equal: the
    /// read is that producer's table.
    Named,
    /// No known component contradicts the read, but some component the read
    /// names is unknown for the producer, so the read could still be its table.
    Possibly,
    /// A known component differs: the read is not that producer's table.
    Not,
}

impl ProducerTarget {
    /// Fold each component the way [`crate::physical_edges::derive_physical_edges`] folds both
    /// sides; an empty component is an unknown one.
    fn new(catalog: Option<&str>, schema: Option<&str>, table: Option<&str>) -> Self {
        let fold = |c: Option<&str>| c.map(fold_identifier).filter(|c| !c.is_empty());
        Self {
            catalog: fold(catalog),
            schema: fold(schema),
            table: fold(table),
        }
    }

    /// Whether a read spelled `parts` (folded, `catalog.schema.table`,
    /// `schema.table` or a bare `table`) is this producer's table.
    ///
    /// A two-part read resolves in the connection's current catalog, which
    /// Rocky cannot see, so it is compared on `(schema, table)` alone — the
    /// same comparison the physical pass makes. A bare read names no schema and
    /// cannot rule any producer out. A producer's unknown component never
    /// rules it out either: a seed's real target is chosen when it loads, after
    /// the graph is built, so "not known to match" must not be read as "does
    /// not match".
    fn match_read(&self, parts: &[String]) -> ReadMatch {
        let compare = |own: &Option<String>, read: &String| match own {
            Some(own) if own == read => ReadMatch::Named,
            Some(_) => ReadMatch::Not,
            None => ReadMatch::Possibly,
        };
        let components: Vec<ReadMatch> = match parts {
            [catalog, schema, table] => vec![
                compare(&self.catalog, catalog),
                compare(&self.schema, schema),
                compare(&self.table, table),
            ],
            [schema, table] => vec![compare(&self.schema, schema), compare(&self.table, table)],
            // A bare read, or one with more parts than a table has.
            _ => return ReadMatch::Possibly,
        };
        if components.contains(&ReadMatch::Not) {
            ReadMatch::Not
        } else if components.contains(&ReadMatch::Possibly) {
            ReadMatch::Possibly
        } else {
            ReadMatch::Named
        }
    }

    /// The target for a message, an unknown component shown as `?`.
    fn describe(&self) -> String {
        let part = |p: &Option<String>| p.as_deref().unwrap_or("?").to_string();
        format!(
            "{}.{}.{}",
            part(&self.catalog),
            part(&self.schema),
            part(&self.table)
        )
    }
}

/// The declared target of every node that can produce a table a model reads.
///
/// A model's own `[target]` (or, when it names no catalog, the catalog its
/// adapter established); a seed's sidecar `[target]`, with the seed loader's
/// defaults for what it leaves out; a load pipeline's `[target]`. What a target
/// does not fix stays unknown: a seed's catalog when no single pipeline could
/// be chosen for the seed nodes, and a load with no explicit table, which
/// writes tables named after its files. Such a producer is never named by a
/// read, and never ruled out by one — see [`ProducerTarget::match_read`]. A
/// replication pipeline's load writes templated targets and is entirely
/// unknown.
fn producer_targets(
    dag: &UnifiedDag,
    config: &RockyConfig,
    models_by_pipeline: &ModelsByPipeline,
    seeds: &[SeedFile],
    seed_default_catalog: Option<&str>,
    pipeline_catalog: &HashMap<&str, Option<String>>,
    established: &dyn Fn(&str) -> Option<String>,
) -> HashMap<NodeId, ProducerTarget> {
    let mut targets: HashMap<NodeId, ProducerTarget> = HashMap::new();

    for (pipeline, models) in models_by_pipeline {
        let effective = pipeline_catalog
            .get(pipeline.as_str())
            .and_then(Option::as_deref);
        for model in models {
            // An ephemeral model creates no table, so its configured target
            // is a name nothing can read.
            let target = if matches!(
                model.config.strategy,
                crate::models::StrategyConfig::Ephemeral
            ) {
                ProducerTarget::default()
            } else {
                let t = &model.config.target;
                let declared = Some(t.catalog.as_str()).filter(|c| !fold_identifier(c).is_empty());
                ProducerTarget::new(declared.or(effective), Some(&t.schema), Some(&t.table))
            };
            targets.insert(NodeId::new("transformation", &model.config.name), target);
        }
    }

    // The seed loader's own rule: a sidecar `[target]` names the schema, and the
    // catalog and table where it says so; whatever it leaves out defaults — the
    // catalog to the one chosen for the seed nodes' pipeline, the schema to the
    // default seed schema, the table to the seed's name. A sidecar catalog that
    // is spelled empty is taken as spelled, and stays unknown.
    for seed in seeds {
        let target = match &seed.config.target {
            Some(t) => ProducerTarget::new(
                t.catalog.as_deref().or(seed_default_catalog),
                Some(&t.schema),
                Some(t.table.as_deref().unwrap_or(&seed.name)),
            ),
            None => ProducerTarget::new(
                seed_default_catalog,
                Some(crate::seeds::DEFAULT_SEED_SCHEMA),
                Some(&seed.name),
            ),
        };
        targets.insert(NodeId::new("seed", &seed.name), target);
    }

    for node in &dag.nodes {
        if node.kind != NodeKind::Load {
            continue;
        }
        let Some(PipelineConfig::Load(load)) = node
            .pipeline
            .as_deref()
            .and_then(|p| config.pipelines.get(p))
        else {
            continue;
        };
        let t = &load.target;
        let declared = Some(t.catalog.as_str()).filter(|c| !fold_identifier(c).is_empty());
        let catalog = declared
            .map(str::to_owned)
            .or_else(|| established(&t.adapter));
        targets.insert(
            node.id.clone(),
            ProducerTarget::new(catalog.as_deref(), Some(&t.schema), t.table.as_deref()),
        );
    }

    targets
}

/// Add the edges implied by a model reading a name that is a node's label.
///
/// This pass parses each model's SQL, extracts the tables it reads, and
/// matches the LAST segment of each read against the labels of the nodes that
/// produce tables (transformations, seeds, loads). It is the weakest evidence
/// the runtime has — it ignores the read's schema and catalog — so it runs
/// after the physical pass and never displaces it: see [`build_runtime_dag`].
///
/// A label claimed by one node orders its readers after that node. A label
/// claimed by several is resolved per reader by the physical target the read
/// names — when exactly one claimant is definitely that table and no other could
/// be — or refused. Build order never decides: which node was built last says
/// nothing about which one a read means (#1629).
///
/// `physical` are the `(producer, consumer)` node pairs the physical pass
/// settled. `targets` holds each producer's declared target. Inferred edges
/// are de-duplicated against existing ones, so calling this repeatedly is
/// idempotent.
fn infer_label_dependencies(
    dag: &mut UnifiedDag,
    model_sql_by_name: &HashMap<String, String>,
    targets: &HashMap<NodeId, ProducerTarget>,
    physical: &HashSet<(NodeId, NodeId)>,
) -> Result<LabelInferenceReport, UnifiedDagError> {
    let mut report = LabelInferenceReport::default();
    // Every node that creates a table, by lowercase label so case-insensitive
    // SQL refs match. Several claimants of one label are a collision.
    let mut producers: HashMap<String, Vec<(NodeId, NodeKind)>> = HashMap::new();
    for node in &dag.nodes {
        match node.kind {
            NodeKind::Transformation | NodeKind::Seed | NodeKind::Load | NodeKind::Replication => {
                producers
                    .entry(node.label.to_lowercase())
                    .or_default()
                    .push((node.id.clone(), node.kind));
            }
            NodeKind::Source | NodeKind::Quality | NodeKind::Snapshot | NodeKind::Test => {}
        }
    }
    for (label, claimants) in &producers {
        if claimants.len() > 1 {
            report
                .label_collisions
                .push((label.clone(), claimants.len()));
        }
    }
    report.label_collisions.sort();

    let labels: HashMap<&NodeId, &str> = dag
        .nodes
        .iter()
        .map(|n| (&n.id, n.label.as_str()))
        .collect();

    // `(reader id, producer id)`, sorted so every decision below is made in
    // the same order every run.
    let mut candidates: BTreeSet<(String, String)> = BTreeSet::new();
    for node in &dag.nodes {
        if node.kind != NodeKind::Transformation {
            continue;
        }
        let Some(sql) = model_sql_by_name.get(&node.label) else {
            continue;
        };
        let refs = match rocky_sql::lineage::referenced_tables(sql) {
            Ok(refs) => refs,
            // A model whose reads cannot be extracted derives no edges here —
            // it may be co-scheduled with an unordered upstream. Surfaced,
            // never silent (#1351).
            Err(_) => {
                report.unparsed.push(node.label.clone());
                continue;
            }
        };
        for read in refs {
            let parts: Vec<String> = read.split('.').map(fold_identifier).collect();
            // Match by the bare table name (last segment of any qualified ref).
            let Some(label) = parts.last() else { continue };
            let Some(claimants) = producers.get(label) else {
                continue;
            };
            let producer_id = match claimants.as_slice() {
                [(only, _)] => only,
                // Several nodes claim the label: the read must name exactly
                // one of them by its physical target, and no other claimant
                // may be able to be the same table. A claimant whose target is
                // not fully known is not ruled out by a read that does not
                // contradict it.
                _ => {
                    let possible: Vec<(&NodeId, ReadMatch)> = claimants
                        .iter()
                        .map(|(id, _)| {
                            let found = targets
                                .get(id)
                                .map_or(ReadMatch::Possibly, |t| t.match_read(&parts));
                            (id, found)
                        })
                        .filter(|(_, found)| *found != ReadMatch::Not)
                        .collect();
                    if possible.is_empty() {
                        // The qualified read is of another table entirely.
                        // None of these label claimants can supply it.
                        continue;
                    }
                    let [(named, ReadMatch::Named)] = possible.as_slice() else {
                        let mut described: Vec<String> = claimants
                            .iter()
                            .map(|(id, kind)| {
                                let name = labels.get(id).copied().unwrap_or_default();
                                let target = targets
                                    .get(id)
                                    .map(ProducerTarget::describe)
                                    .unwrap_or_else(|| ProducerTarget::default().describe());
                                format!("{} '{name}' (target {target})", producer_kind(*kind))
                            })
                            .collect();
                        described.sort();
                        return Err(UnifiedDagError::AmbiguousLabelProducer {
                            label: label.clone(),
                            reader: node.label.clone(),
                            read,
                            producers: described,
                        });
                    };
                    *named
                }
            };
            // A model never depends on itself.
            if *producer_id != node.id {
                candidates.insert((node.id.0.clone(), producer_id.0.clone()));
            }
        }
    }

    // Existing edges as a set so we don't double-add; adjacency carries whether
    // each edge is one the physical pass settled.
    let mut existing: HashSet<(NodeId, NodeId)> = dag
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();
    let mut adjacency: HashMap<NodeId, Vec<(NodeId, bool)>> = HashMap::new();
    for e in &dag.edges {
        let is_physical = physical.contains(&(e.from.clone(), e.to.clone()));
        adjacency
            .entry(e.from.clone())
            .or_default()
            .push((e.to.clone(), is_physical));
    }
    let label_of = |id: &NodeId| labels.get(id).copied().unwrap_or_default().to_string();

    let mut new_edges = Vec::new();
    for (reader, producer) in candidates {
        let (reader_id, producer_id) = (NodeId(reader), NodeId(producer));
        let key = (producer_id.clone(), reader_id.clone());
        if existing.contains(&key) {
            continue;
        }
        // Ordering the producer first closes a cycle iff the producer already
        // runs after the reader. When that path runs through a physical edge,
        // the physical evidence is the stronger and this guess is the one to
        // drop. A cycle of declared and label edges alone stays: genuine
        // reciprocal reads keep their loud refusal, and a guard here must not
        // downgrade a real SQL cycle into a silent stale-read success.
        if closes_a_cycle_through_a_physical_edge(&adjacency, &reader_id, &producer_id) {
            report
                .skipped_cycle_edges
                .push((label_of(&reader_id), label_of(&producer_id)));
            continue;
        }
        existing.insert(key);
        adjacency
            .entry(producer_id.clone())
            .or_default()
            .push((reader_id.clone(), false));
        new_edges.push(UnifiedEdge {
            from: producer_id,
            to: reader_id,
            edge_type: EdgeType::DataDependency,
        });
    }

    dag.edges.extend(new_edges);
    report.unparsed.sort();
    Ok(report)
}

/// Whether ordering `producer` before `reader` would close a cycle that runs
/// through at least one physical edge: is there a path `reader ⇝ producer`
/// that uses one?
///
/// `adjacency` maps a node to the nodes that run after it, each flagged when
/// the edge is one the physical pass settled.
fn closes_a_cycle_through_a_physical_edge(
    adjacency: &HashMap<NodeId, Vec<(NodeId, bool)>>,
    reader: &NodeId,
    producer: &NodeId,
) -> bool {
    let mut seen: HashSet<(&NodeId, bool)> = HashSet::new();
    let mut stack: Vec<(&NodeId, bool)> = vec![(reader, false)];
    while let Some((current, through_physical)) = stack.pop() {
        if current == producer {
            // The path ends here: walking on would revisit the producer.
            if through_physical {
                return true;
            }
            continue;
        }
        if !seen.insert((current, through_physical)) {
            continue;
        }
        if let Some(next) = adjacency.get(current) {
            stack.extend(
                next.iter()
                    .map(|(to, is_physical)| (to, through_physical || *is_physical)),
            );
        }
    }
    false
}

/// A producer's kind, for a message.
fn producer_kind(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Transformation => "model",
        NodeKind::Seed => "seed",
        NodeKind::Load => "load pipeline",
        NodeKind::Replication => "replication pipeline",
        NodeKind::Source | NodeKind::Quality | NodeKind::Snapshot | NodeKind::Test => "node",
    }
}

/// What the label pass (`infer_label_dependencies`) could not settle — surfaced by the
/// `run --dag` caller as scheduling warnings instead of silently dropping
/// edges (#1351).
#[derive(Debug, Default)]
pub struct LabelInferenceReport {
    /// Lowercased labels claimed by more than one producing node, with the
    /// claimant count. A read of such a label is ordered after the producer
    /// whose full target it names, or refused — never by build order (#1629).
    pub label_collisions: Vec<(String, usize)>,
    /// Transformation labels whose SQL failed table-reference extraction.
    pub unparsed: Vec<String>,
    /// `(reader, producer)` labels of by-name edges skipped because ordering
    /// the producer first would close a cycle through a physical-read edge.
    pub skipped_cycle_edges: Vec<(String, String)>,
}

impl LabelInferenceReport {
    /// Render operator-facing warnings; empty when nothing was unresolved.
    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        let mut w = Vec::new();
        for (label, n) in &self.label_collisions {
            w.push(format!(
                "label '{label}' is produced by {n} nodes — a read of it is ordered after the \
                 producer whose full catalog.schema.table target it names, and refused when it \
                 names none or several. Rename the colliding producers so each label is unique \
                 (depends_on cannot order a model against a seed or load)"
            ));
        }
        for (reader, producer) in &self.skipped_cycle_edges {
            w.push(format!(
                "label-based ordering: '{reader}' reads the name '{producer}', but ordering \
                 '{producer}' first would close a dependency cycle through an exact \
                 physical-read edge, so that edge was skipped and the pair executes in the \
                 physical-read order. Declare depends_on to choose the order explicitly"
            ));
        }
        for m in &self.unparsed {
            w.push(format!(
                "model '{m}': SQL could not be parsed for table references — label-based \
                 ordering could not be derived for it"
            ));
        }
        w
    }
}

/// Target-aware physical-read edges for transformation nodes (#1275).
///
/// [`infer_label_dependencies`] matches reads against node LABELS — it orders
/// a model after a seed/load it reads by name, but is blind to configured
/// `[target]`s: a model reading another model's physical `schema.table`
/// derives nothing there unless the table happens to equal the model name.
/// This pass derives those edges from rendered target components via
/// [`crate::physical_edges::derive_physical_edges`] — the same derivation the
/// plain-run layer computation uses, so both schedulers order the same pairs.
///
/// It runs BEFORE the label pass ([`build_runtime_dag`]): exact physical
/// evidence outranks a by-name guess, so the guess must never occupy the graph
/// first.
///
/// Cycle-closing candidates are skipped deterministically inside the
/// derivation (the executor's `execution_phases` hard-errors on cycles, and
/// a derived edge must never turn a runnable project into a refused one).
/// Returns the derivation so the caller can surface its warnings.
fn infer_physical_dependencies(
    dag: &mut UnifiedDag,
    models: &[PhysicalEdgeModel<'_>],
) -> DerivedPhysicalEdges {
    use std::collections::HashMap as Map;
    // Transformation-node index by label (label == model name for
    // transformation nodes — the same contract infer_label_dependencies
    // relies on for SQL lookup).
    let by_label: Map<&str, NodeId> = dag
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Transformation)
        .map(|n| (n.label.as_str(), n.id.clone()))
        .collect();
    let id_to_label: Map<&NodeId, &str> = dag
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Transformation)
        .map(|n| (&n.id, n.label.as_str()))
        .collect();

    // Existing name-level relation among transformation nodes: consumer
    // (edge.to) depends on producer (edge.from).
    let existing: Vec<(String, String)> = dag
        .edges
        .iter()
        .filter_map(|e| {
            let from = id_to_label.get(&e.from)?;
            let to = id_to_label.get(&e.to)?;
            Some(((*to).to_string(), (*from).to_string()))
        })
        .collect();

    let mut derived = crate::physical_edges::derive_physical_edges(models, &existing);

    let edge_set: std::collections::HashSet<(NodeId, NodeId)> = dag
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();
    // Full-graph reachability guard. The derivation's own cycle guard sees
    // only the transformation-projected relation — a real path between two
    // transformations THROUGH a non-transformation node (a check, a seed) is
    // invisible to it, and `execution_phases` hard-errors on cycles, so every
    // insertion is re-checked against the whole graph: a derived edge must
    // never make a runnable project refuse.
    // O(V+E) per query over an adjacency map rebuilt only when edges were
    // added since the last build — not per candidate (the review measured
    // the rebuild-per-candidate shape at hundreds of millions of visits on
    // pathological projects).
    fn reaches_node(
        adj: &std::collections::HashMap<NodeId, Vec<NodeId>>,
        from: &NodeId,
        to: &NodeId,
    ) -> bool {
        let mut seen: std::collections::HashSet<&NodeId> = std::collections::HashSet::new();
        let mut stack = vec![from];
        while let Some(cur) = stack.pop() {
            if cur == to {
                return true;
            }
            if !seen.insert(cur) {
                continue;
            }
            if let Some(next) = adj.get(cur) {
                stack.extend(next.iter());
            }
        }
        false
    }
    fn build_adj(dag: &UnifiedDag) -> std::collections::HashMap<NodeId, Vec<NodeId>> {
        let mut adj: std::collections::HashMap<NodeId, Vec<NodeId>> =
            std::collections::HashMap::new();
        for e in &dag.edges {
            adj.entry(e.from.clone()).or_default().push(e.to.clone());
        }
        adj
    }
    let mut adj = build_adj(dag);
    for (consumer, producer) in derived.edges.clone() {
        let (Some(cid), Some(pid)) = (
            by_label.get(consumer.as_str()),
            by_label.get(producer.as_str()),
        ) else {
            continue;
        };
        if edge_set.contains(&(pid.clone(), cid.clone())) {
            continue;
        }
        // Inserting producer→consumer closes a cycle iff the consumer's node
        // already reaches the producer's node through the FULL graph.
        if reaches_node(&adj, cid, pid) {
            derived
                .edges
                .retain(|(c, p)| !(c == &consumer && p == &producer));
            derived.skipped_cycle_edges.push((consumer, producer));
            continue;
        }
        dag.edges.push(UnifiedEdge {
            from: pid.clone(),
            to: cid.clone(),
            edge_type: EdgeType::DataDependency,
        });
        adj.entry(pid.clone()).or_default().push(cid.clone());
    }
    // Second pass: a name-level skip's premise is "the opposite direction
    // was accepted". If insertion REJECTED that opposite edge (a real cycle
    // through an intermediate), the skipped direction may now be safe — and
    // without it the pair would end up with NO edge at all, silently
    // co-scheduled. Reconsider every name-level skip against the live graph.
    let skipped_snapshot = derived.skipped_cycle_edges.clone();
    for (consumer, producer) in skipped_snapshot {
        let (Some(cid), Some(pid)) = (
            by_label.get(consumer.as_str()),
            by_label.get(producer.as_str()),
        ) else {
            continue;
        };
        if edge_set.contains(&(pid.clone(), cid.clone())) {
            continue;
        }
        if reaches_node(&adj, cid, pid) {
            continue;
        }
        derived
            .skipped_cycle_edges
            .retain(|(c, p)| !(c == &consumer && p == &producer));
        derived.edges.push((consumer, producer));
        dag.edges.push(UnifiedEdge {
            from: pid.clone(),
            to: cid.clone(),
            edge_type: EdgeType::DataDependency,
        });
        adj.entry(pid.clone()).or_default().push(cid.clone());
    }
    derived
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
            fulfill: Default::default(),
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
            drop_existing_kind: None,
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
            file_path: format!("models/{name}.sql").into(),
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

    // ---------- the label pass ----------

    /// The label pass alone over a hand-built DAG: no producer targets, no
    /// physical edges. Fixtures here have no label collision, so it cannot
    /// refuse; the collision cases go through [`build_runtime_dag`].
    fn infer_labels(dag: &mut UnifiedDag, sql: &HashMap<String, String>) -> LabelInferenceReport {
        infer_label_dependencies(dag, sql, &HashMap::new(), &HashSet::new())
            .expect("a fixture without a label collision cannot be refused")
    }

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

        infer_labels(&mut dag, &sql);

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

        infer_labels(&mut dag, &sql);
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

        infer_labels(&mut dag, &sql);

        // No duplicate added.
        assert_eq!(dag.edges.len(), 1);
    }

    #[test]
    fn test_infer_ignores_self_reference() {
        let mut dag = dag_with_models(&[("loop_model", NodeKind::Transformation)]);
        let mut sql = HashMap::new();
        sql.insert("loop_model".into(), "SELECT * FROM loop_model".into());

        infer_labels(&mut dag, &sql);
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

        infer_labels(&mut dag, &sql);
        infer_labels(&mut dag, &sql);

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

        infer_labels(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 0);
    }

    #[test]
    fn test_infer_handles_invalid_sql_gracefully() {
        let mut dag = dag_with_models(&[("model_a", NodeKind::Transformation)]);
        let mut sql = HashMap::new();
        sql.insert("model_a".into(), "this is not sql".into());

        // Should not panic; invalid SQL just yields no inferred edges.
        infer_labels(&mut dag, &sql);
        assert_eq!(dag.edges.len(), 0);
    }

    /// #1275: a transformation reading another transformation's PHYSICAL
    /// target — with a table name ≠ model name, so the label heuristic
    /// cannot see it — derives an edge and the executor phases order them.
    #[test]
    fn physical_reads_order_transformation_phases() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut producer = model("orders_model", vec![], vec![]);
        producer.config.target.table = "orders_v2".into();
        producer.sql = "SELECT 1 AS id".into();
        let mut consumer = model("mart", vec![], vec![]);
        consumer.sql = "SELECT id FROM silver.orders_v2".into();

        let models = vec![producer, consumer];
        let by_pipeline = owned_by_sole_transformation(&config, models.clone());
        let mut dag = build_unified_dag(&config, &by_pipeline, &[]).expect("build dag");

        // Precondition: label inference alone leaves them co-phased.
        let sql_by_name: std::collections::HashMap<String, String> = models
            .iter()
            .map(|m| (m.config.name.clone(), m.sql.clone()))
            .collect();
        infer_labels(&mut dag, &sql_by_name);
        let phases = execution_phases(&dag).expect("phases");
        let phase_of = |label: &str, phases: &Vec<Vec<&UnifiedNode>>| -> usize {
            phases
                .iter()
                .position(|l| l.iter().any(|n| n.label == label))
                .unwrap()
        };
        assert_eq!(
            phase_of("orders_model", &phases),
            phase_of("mart", &phases),
            "precondition: the label heuristic is blind to the renamed target"
        );

        // The graph `run --dag` actually schedules from.
        let runtime =
            build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog).expect("runtime dag");
        assert_eq!(runtime.physical.edges.len(), 1, "{:?}", runtime.physical);
        let phases = execution_phases(&runtime.dag).expect("phases after augmentation");
        assert!(
            phase_of("orders_model", &phases) < phase_of("mart", &phases),
            "producer must phase strictly before its physical reader"
        );
    }

    // ---------- build_runtime_dag: the entry point `run --dag` uses ----------

    /// A catalog resolver that establishes nothing — every adapter but DuckDB.
    fn no_catalog(_: &AdapterConfig) -> Option<String> {
        None
    }

    /// What a DuckDB adapter's own catalog is: its database file's stem.
    fn duckdb_stem_catalog(adapter: &AdapterConfig) -> Option<String> {
        adapter
            .path
            .as_deref()
            .and_then(|p| std::path::Path::new(p).file_stem())
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_owned)
    }

    /// `[pipeline.X] type = "load"` writing `catalog.schema[.table]`.
    fn load_pipeline(catalog: &str, schema: &str, table: Option<&str>) -> PipelineConfig {
        let table = table.map_or(String::new(), |t| format!("table = \"{t}\"\n"));
        toml::from_str(&format!(
            "type = \"load\"\nsource_dir = \"data/\"\n\n[target]\ncatalog = \"{catalog}\"\n\
             schema = \"{schema}\"\n{table}"
        ))
        .expect("load pipeline fixture must deserialize")
    }

    /// A config whose `default` adapter is a DuckDB file `db.duckdb`.
    fn duckdb_config(pipelines: Vec<(&str, PipelineConfig)>) -> RockyConfig {
        let mut config = config_with_pipelines(pipelines);
        config
            .adapters
            .insert("default".into(), duckdb_adapter("db.duckdb"));
        config
    }

    /// A model writing `catalog.schema.table` and reading `sql`.
    fn model_reading(name: &str, target: (&str, &str, &str), sql: &str) -> Model {
        let mut m = model_targeting(name, target.0, target.1, target.2);
        m.sql = sql.to_string();
        m
    }

    /// The execution phase of the node with this id (`kind:name`).
    fn phase_index(dag: &UnifiedDag, id: &str) -> usize {
        execution_phases(dag)
            .expect("phases")
            .iter()
            .position(|phase| phase.iter().any(|n| n.id.0 == id))
            .unwrap_or_else(|| panic!("no node {id}"))
    }

    /// Whether `from` must complete before `to` (node ids, `kind:name`).
    fn has_edge(dag: &UnifiedDag, from: &str, to: &str) -> bool {
        dag.edges.iter().any(|e| e.from.0 == from && e.to.0 == to)
    }

    /// #1629 P1: a producer whose `[target]` omits its catalog (`catalog =
    /// ""`, the DuckDB single-catalog shape) is indexed under an empty
    /// catalog, so a three-part read of it misses the exact index. Through
    /// the graph `run --dag` builds: once the adapter establishes the
    /// producer's catalog, the read that names it still orders the pair.
    #[test]
    fn a_catalogless_producer_is_ordered_before_a_read_that_names_its_catalog() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "orders_model",
                    ("", "silver", "orders_v2"),
                    "SELECT 1 AS id",
                ),
                model_reading(
                    "mart",
                    ("db", "silver", "mart"),
                    "SELECT id FROM db.silver.orders_v2",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(
            has_edge(
                &runtime.dag,
                "transformation:orders_model",
                "transformation:mart"
            ),
            "{:?}",
            runtime.physical
        );
        assert!(runtime.warnings.is_empty(), "{:?}", runtime.warnings);
        assert!(
            phase_index(&runtime.dag, "transformation:orders_model")
                < phase_index(&runtime.dag, "transformation:mart")
        );
    }

    /// The same project on an adapter that cannot say which catalog its
    /// catalogless targets live in. The read might name another catalog's
    /// table, so no edge is guessed — and the read is surfaced, not dropped.
    #[test]
    fn a_catalogless_producer_on_an_adapter_that_cannot_say_is_not_guessed_at() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "orders_model",
                    ("", "silver", "orders_v2"),
                    "SELECT 1 AS id",
                ),
                model_reading(
                    "mart",
                    ("db", "silver", "mart"),
                    "SELECT id FROM db.silver.orders_v2",
                ),
            ],
        );
        let runtime =
            build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog).expect("runtime dag");
        assert!(
            !has_edge(
                &runtime.dag,
                "transformation:orders_model",
                "transformation:mart"
            ),
            "no edge without an established catalog"
        );
        assert!(
            runtime
                .warnings
                .iter()
                .any(|w| w.contains("'mart'") && w.contains("db.silver.orders_v2")),
            "the unbound read is named: {:?}",
            runtime.warnings
        );
    }

    /// Never guess across catalogs: the producer is established to live in
    /// `db`, so a read of `other.silver.orders_v2` is another catalog's table.
    #[test]
    fn a_read_naming_another_catalog_gets_no_edge_in_the_runtime_dag() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "orders_model",
                    ("", "silver", "orders_v2"),
                    "SELECT 1 AS id",
                ),
                model_reading(
                    "mart",
                    ("db", "silver", "mart"),
                    "SELECT id FROM other.silver.orders_v2",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(!has_edge(
            &runtime.dag,
            "transformation:orders_model",
            "transformation:mart"
        ));
        assert!(runtime.warnings.is_empty(), "{:?}", runtime.warnings);
    }

    /// Several catalogless producers could be the read's table: no edge, and
    /// the warning names every candidate.
    #[test]
    fn several_catalogless_candidates_add_no_edge_and_are_named_in_the_runtime_dag() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading("first", ("", "silver", "shared"), "SELECT 1 AS x"),
                model_reading("second", ("", "silver", "shared"), "SELECT 2 AS x"),
                model_reading(
                    "reader",
                    ("db", "silver", "reader"),
                    "SELECT x FROM db.silver.shared",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(!has_edge(
            &runtime.dag,
            "transformation:first",
            "transformation:reader"
        ));
        assert!(!has_edge(
            &runtime.dag,
            "transformation:second",
            "transformation:reader"
        ));
        assert!(
            runtime
                .warnings
                .iter()
                .any(|w| w.contains("'first'") && w.contains("'second'")),
            "{:?}",
            runtime.warnings
        );
        assert_eq!(
            runtime.physical.target_collisions,
            vec![("first".into(), "second".into())],
            "two writers on one adapter still collide"
        );
    }

    /// Two adapter names with the same DuckDB file stem have the same
    /// established catalog. Neither name can exclude the other producer.
    #[test]
    fn equal_stem_adapters_leave_the_read_ambiguous_and_report_a_collision() {
        let mut config = config_with_pipelines(vec![
            ("p_one", transform_pipeline_on("wh_one")),
            ("p_two", transform_pipeline_on("wh_two")),
            ("p_read", transform_pipeline_on("wh_one")),
        ]);
        config
            .adapters
            .insert("wh_one".into(), duckdb_adapter("one/db.duckdb"));
        config
            .adapters
            .insert("wh_two".into(), duckdb_adapter("two/db.duckdb"));
        let by_pipeline = ModelsByPipeline::from([
            (
                "p_one".into(),
                vec![model_reading("first", ("", "main", "shared"), "SELECT 1")],
            ),
            (
                "p_two".into(),
                vec![model_reading("second", ("", "main", "shared"), "SELECT 2")],
            ),
            (
                "p_read".into(),
                vec![model_reading(
                    "reader",
                    ("db", "main", "out"),
                    "SELECT x FROM db.main.shared",
                )],
            ),
        ]);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(runtime.physical.edges.is_empty(), "{:?}", runtime.physical);
        assert_eq!(runtime.physical.unbound_reads.len(), 1);
        assert_eq!(
            runtime.physical.unbound_reads[0].candidates,
            vec!["first", "second"]
        );
        assert_eq!(
            runtime.physical.target_collisions,
            vec![("first".into(), "second".into())]
        );
        assert!(
            runtime
                .warnings
                .iter()
                .any(|w| w.contains("'first'") && w.contains("'second'")),
            "{:?}",
            runtime.warnings
        );
    }

    /// A declared target on another adapter must not consume the exact hit
    /// before the reader's catalogless producer gets the fallback edge.
    #[test]
    fn another_adapters_exact_target_does_not_hide_the_local_fallback() {
        let mut config = config_with_pipelines(vec![
            ("p_local", transform_pipeline_on("wh_local")),
            ("p_other", transform_pipeline_on("wh_other")),
            ("p_read", transform_pipeline_on("wh_local")),
        ]);
        config
            .adapters
            .insert("wh_local".into(), duckdb_adapter("one/db.duckdb"));
        config
            .adapters
            .insert("wh_other".into(), duckdb_adapter("two/other.duckdb"));
        let by_pipeline = ModelsByPipeline::from([
            (
                "p_local".into(),
                vec![model_reading("local", ("", "main", "shared"), "SELECT 1")],
            ),
            (
                "p_other".into(),
                vec![model_reading("other", ("db", "main", "shared"), "SELECT 2")],
            ),
            (
                "p_read".into(),
                vec![model_reading(
                    "reader",
                    ("db", "main", "out"),
                    "SELECT x FROM db.main.shared",
                )],
            ),
        ]);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(
            has_edge(
                &runtime.dag,
                "transformation:local",
                "transformation:reader"
            ),
            "{:?}",
            runtime.physical
        );
        assert!(!has_edge(
            &runtime.dag,
            "transformation:other",
            "transformation:reader"
        ));
    }

    /// Another adapter name alone does not disprove an exact physical read.
    /// Without a local fallback producer, keep that exact ordering edge.
    #[test]
    fn cross_adapter_exact_read_stays_ordered_without_a_local_fallback() {
        let mut config = config_with_pipelines(vec![
            ("p_other", transform_pipeline_on("wh_other")),
            ("p_read", transform_pipeline_on("wh_local")),
        ]);
        config
            .adapters
            .insert("wh_local".into(), duckdb_adapter("one/db.duckdb"));
        config
            .adapters
            .insert("wh_other".into(), duckdb_adapter("two/other.duckdb"));
        let by_pipeline = ModelsByPipeline::from([
            (
                "p_other".into(),
                vec![model_reading("other", ("db", "main", "shared"), "SELECT 1")],
            ),
            (
                "p_read".into(),
                vec![model_reading(
                    "reader",
                    ("db", "main", "out"),
                    "SELECT x FROM db.main.shared",
                )],
            ),
        ]);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(has_edge(
            &runtime.dag,
            "transformation:other",
            "transformation:reader"
        ));
    }

    /// Two pipelines, two DuckDB files, one `schema.table` written through
    /// both. Each catalogless target lives in ITS adapter's catalog and a read
    /// naming `alpha` can only mean the `alpha` file's. The producer on the
    /// other adapter does not make that proven binding ambiguous.
    #[test]
    fn a_schema_table_two_adapters_bind_the_reader_to_its_adapter() {
        let mut config = config_with_pipelines(vec![
            ("p_alpha", transform_pipeline_on("wh_alpha")),
            ("p_beta", transform_pipeline_on("wh_beta")),
            ("p_read", transform_pipeline_on("wh_alpha")),
        ]);
        config
            .adapters
            .insert("wh_alpha".into(), duckdb_adapter("one/alpha.duckdb"));
        config
            .adapters
            .insert("wh_beta".into(), duckdb_adapter("two/beta.duckdb"));
        let by_pipeline = ModelsByPipeline::from([
            (
                "p_alpha".to_string(),
                vec![model_reading(
                    "shared_alpha",
                    ("", "main", "shared"),
                    "SELECT 1 AS x",
                )],
            ),
            (
                "p_beta".to_string(),
                vec![model_reading(
                    "shared_beta",
                    ("", "main", "shared"),
                    "SELECT 2 AS x",
                )],
            ),
            (
                "p_read".to_string(),
                vec![model_reading(
                    "reader",
                    ("alpha", "main", "reader"),
                    "SELECT x FROM alpha.main.shared",
                )],
            ),
        ]);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(has_edge(
            &runtime.dag,
            "transformation:shared_alpha",
            "transformation:reader"
        ));
        assert!(!has_edge(
            &runtime.dag,
            "transformation:shared_beta",
            "transformation:reader"
        ));
        assert!(
            runtime.warnings.is_empty(),
            "the binding is settled: {:?}",
            runtime.warnings
        );
    }

    /// The review's P1 construction, through the graph `run --dag` builds.
    /// `alpha` reads ANOTHER catalog's `beta_table` and `beta` (catalog
    /// `prod`) reads alpha's table by a two-part name. The real edge is
    /// beta-after-alpha; guessing on `(schema, table)` would fabricate
    /// alpha-after-beta and let the cycle guard discard the real one.
    #[test]
    fn a_read_of_another_catalogs_table_never_reverses_a_real_edge_in_the_runtime_dag() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "alpha",
                    ("prod", "main", "alpha_table"),
                    "SELECT x FROM external.main.beta_table",
                ),
                model_reading(
                    "beta",
                    ("prod", "main", "beta_table"),
                    "SELECT y FROM main.alpha_table",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(
            has_edge(&runtime.dag, "transformation:alpha", "transformation:beta"),
            "beta reads alpha's table: {:?}",
            runtime.physical
        );
        assert!(
            !has_edge(&runtime.dag, "transformation:beta", "transformation:alpha"),
            "alpha reads a table in ANOTHER catalog, not beta's"
        );
        assert!(
            phase_index(&runtime.dag, "transformation:alpha")
                < phase_index(&runtime.dag, "transformation:beta")
        );
    }

    /// An exact physical edge is decided before any inference, whatever the
    /// names sort like: `a_reader`'s catalog fallback and `z_writer`'s exact
    /// read contradict, and the guess must be the one skipped.
    #[test]
    fn an_exact_read_outranks_a_catalog_fallback_in_the_runtime_dag() {
        let config = duckdb_config(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "a_reader",
                    ("db", "main", "a_out"),
                    "SELECT x FROM db.main.z_out",
                ),
                model_reading(
                    "z_writer",
                    ("", "main", "z_out"),
                    "SELECT x FROM main.a_out",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("runtime dag");
        assert!(
            has_edge(
                &runtime.dag,
                "transformation:a_reader",
                "transformation:z_writer"
            ),
            "z_writer reads a_reader's table exactly: {:?}",
            runtime.physical
        );
        assert!(!has_edge(
            &runtime.dag,
            "transformation:z_writer",
            "transformation:a_reader"
        ));
        assert!(
            phase_index(&runtime.dag, "transformation:a_reader")
                < phase_index(&runtime.dag, "transformation:z_writer")
        );
    }

    /// The repro shape from #1629's body. `customers` reads
    /// `warehouse.silver.rollup` — exactly `rollup`'s target — and `rollup`
    /// bare-reads `customers`, a name that matches only by label (the model
    /// writes `customers_v2`). The label match is the weakest evidence in the
    /// graph: it must not displace the exact read, so `rollup` runs first.
    /// Before, the label edge was laid down first, the exact edge was skipped
    /// as its cycle-closer, and `run --dag` refused the project.
    #[test]
    fn an_exact_physical_read_outranks_a_contradicting_label_read() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "customers",
                    ("warehouse", "prod", "customers_v2"),
                    "SELECT y FROM warehouse.silver.rollup",
                ),
                model_reading(
                    "rollup",
                    ("warehouse", "silver", "rollup"),
                    "SELECT x FROM customers",
                ),
            ],
        );
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
            .expect("the exact edge stands and the guess is dropped, not refused");
        assert!(has_edge(
            &runtime.dag,
            "transformation:rollup",
            "transformation:customers"
        ));
        assert!(!has_edge(
            &runtime.dag,
            "transformation:customers",
            "transformation:rollup"
        ));
        assert!(
            phase_index(&runtime.dag, "transformation:rollup")
                < phase_index(&runtime.dag, "transformation:customers")
        );
        assert_eq!(
            runtime.labels.skipped_cycle_edges,
            vec![("rollup".to_string(), "customers".to_string())]
        );
        assert!(
            runtime
                .warnings
                .iter()
                .any(|w| w.contains("label-based ordering") && w.contains("'customers'")),
            "the dropped guess is reported: {:?}",
            runtime.warnings
        );
    }

    /// A cycle made only of a declared edge and a label edge is a genuine
    /// cycle — no physical evidence says which side is wrong — so it keeps its
    /// loud refusal instead of being downgraded to a silent stale read.
    #[test]
    fn a_label_edge_closing_a_cycle_with_only_a_declared_edge_is_still_refused() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut a = model("a", vec!["b"], vec![]);
        a.sql = "SELECT 1 AS x".into();
        let mut b = model("b", vec![], vec![]);
        b.sql = "SELECT y FROM a".into();
        let by_pipeline = owned_by_sole_transformation(&config, vec![a, b]);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
            .expect("the DAG builds; the cycle is the executor's to refuse");
        assert!(
            execution_phases(&runtime.dag).is_err(),
            "a genuine cycle keeps its loud refusal"
        );
        assert!(runtime.labels.skipped_cycle_edges.is_empty());
    }

    // ---------- P2: a label two producers share ----------

    /// The review's P2 construction. A transformation pipeline owns model
    /// `shared` (writes `prod.silver.shared_output`, reads the reader's
    /// output) and model `reader` (reads `prod.bronze.shared`). A LOAD
    /// pipeline named `shared` writes `prod.bronze.shared`. The reader means
    /// the load: the order is load, reader, model `shared` — whichever
    /// pipeline the config lists first.
    fn shared_label_project(load_first: bool) -> (RockyConfig, ModelsByPipeline) {
        let transform = ("t", transform_pipeline(vec![]));
        let load = ("shared", load_pipeline("prod", "bronze", Some("shared")));
        let config = config_with_pipelines(if load_first {
            vec![load, transform]
        } else {
            vec![transform, load]
        });
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "shared",
                    ("prod", "silver", "shared_output"),
                    "SELECT y FROM prod.silver.reader_output",
                ),
                model_reading(
                    "reader",
                    ("prod", "silver", "reader_output"),
                    "SELECT x FROM prod.bronze.shared",
                ),
            ],
        );
        (config, by_pipeline)
    }

    #[test]
    fn a_reader_of_a_colliding_label_is_ordered_after_the_load_it_names() {
        for load_first in [true, false] {
            let (config, by_pipeline) = shared_label_project(load_first);
            let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
                .expect("the read names the load's exact target");
            let dag = &runtime.dag;
            assert!(
                has_edge(dag, "load:shared", "transformation:reader"),
                "load_first={load_first}: the reader reads the load's table"
            );
            assert!(
                has_edge(dag, "transformation:reader", "transformation:shared"),
                "load_first={load_first}: model `shared` reads the reader's exact target"
            );
            assert!(
                !has_edge(dag, "transformation:shared", "transformation:reader"),
                "load_first={load_first}: the reader must never wait for model `shared`"
            );
            assert!(
                !has_edge(dag, "transformation:shared", "load:shared")
                    && !has_edge(dag, "load:shared", "transformation:shared"),
                "load_first={load_first}: the two producers are not ordered against each other"
            );
            // The order that matters: no model runs before its input exists.
            let load = phase_index(dag, "load:shared");
            let reader = phase_index(dag, "transformation:reader");
            let model = phase_index(dag, "transformation:shared");
            assert!(
                load < reader && reader < model,
                "load_first={load_first}: load={load} reader={reader} model={model}"
            );
        }
    }

    /// The same construction with a SEED that has a sidecar target: it too
    /// is named by the read's full target, and node build order (seeds are
    /// always built first) decides nothing.
    #[test]
    fn a_reader_of_a_colliding_label_is_ordered_after_the_seed_it_names() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading(
                    "shared",
                    ("prod", "silver", "shared_output"),
                    "SELECT y FROM prod.silver.reader_output",
                ),
                model_reading(
                    "reader",
                    ("prod", "silver", "reader_output"),
                    "SELECT x FROM prod.bronze.shared",
                ),
            ],
        );
        let mut colliding_seed = seed("shared");
        colliding_seed.config.target = Some(crate::seeds::SeedTarget {
            catalog: Some("prod".into()),
            schema: "bronze".into(),
            table: Some("shared".into()),
        });
        let runtime =
            build_runtime_dag(&config, &by_pipeline, &[colliding_seed], None, &no_catalog)
                .expect("the read names the seed's exact target");
        let dag = &runtime.dag;
        assert!(has_edge(dag, "seed:shared", "transformation:reader"));
        assert!(!has_edge(
            dag,
            "transformation:shared",
            "transformation:reader"
        ));
        assert!(
            phase_index(dag, "seed:shared") < phase_index(dag, "transformation:reader")
                && phase_index(dag, "transformation:reader")
                    < phase_index(dag, "transformation:shared")
        );
    }

    /// And the inverse read: the reader names the MODEL's target
    /// (`prod.silver.shared`, whose table is the label), so the load that shares
    /// its label is NOT ordered before the reader. A rule that answers the case
    /// above by "the load wins" cannot pass both; resolving by target gets both
    /// right, whichever pipeline is built last.
    ///
    /// The model's own edge is asserted as a sanity check only: the physical
    /// pass derives it before the label pass runs, so it cannot tell a label
    /// pass that adds it from one that does not. What this test pins is the
    /// edge that must be ABSENT, and that the label really did collide.
    #[test]
    fn a_reader_of_a_colliding_label_is_ordered_after_the_model_it_names() {
        for load_first in [true, false] {
            let (config, mut by_pipeline) = shared_label_project(load_first);
            let models = by_pipeline.get_mut("t").expect("pipeline t");
            // The model `shared` writes a table called `shared`, so a read of
            // it matches the label by its last segment and the collision is
            // live; it reads nothing.
            let model = models
                .iter_mut()
                .find(|m| m.config.name == "shared")
                .expect("shared");
            model.config.target.table = "shared".into();
            model.sql = "SELECT 1 AS y".to_string();
            models
                .iter_mut()
                .find(|m| m.config.name == "reader")
                .expect("reader")
                .sql = "SELECT x FROM prod.silver.shared".to_string();
            let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
                .expect("the read names the model's exact target");
            assert!(
                has_edge(
                    &runtime.dag,
                    "transformation:shared",
                    "transformation:reader"
                ),
                "load_first={load_first}"
            );
            assert!(
                !has_edge(&runtime.dag, "load:shared", "transformation:reader"),
                "load_first={load_first}: the reader does not read the load's table"
            );
            assert_eq!(
                runtime.labels.label_collisions,
                vec![("shared".to_string(), 2)],
                "the label really collides, so the label pass had to choose"
            );
        }
    }

    /// A bare read of a label two producers share names neither by target,
    /// so it cannot be resolved: the DAG is refused, naming both.
    #[test]
    fn a_bare_read_of_a_colliding_label_is_refused_naming_both_producers() {
        for load_first in [true, false] {
            let (config, mut by_pipeline) = shared_label_project(load_first);
            by_pipeline
                .get_mut("t")
                .expect("pipeline t")
                .iter_mut()
                .find(|m| m.config.name == "reader")
                .expect("reader")
                .sql = "SELECT x FROM shared".to_string();
            let err = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
                .expect_err("an unresolvable collision must refuse");
            let UnifiedDagError::AmbiguousLabelProducer {
                label,
                reader,
                read,
                producers,
            } = &err
            else {
                panic!("wrong refusal: {err}");
            };
            assert_eq!(
                (label.as_str(), reader.as_str(), read.as_str()),
                ("shared", "reader", "shared")
            );
            assert_eq!(producers.len(), 2, "{producers:?}");
            let message = err.to_string();
            assert!(
                message.contains("model 'shared' (target prod.silver.shared_output)")
                    && message.contains("load pipeline 'shared' (target prod.bronze.shared)"),
                "the refusal names both producers with their targets: {message}"
            );
        }
    }

    /// A qualified read that names neither claimant is a different table.
    /// Another producer's exact physical edge must survive label inference.
    #[test]
    fn a_read_naming_neither_colliding_producer_keeps_its_exact_edge() {
        let (config, mut by_pipeline) = shared_label_project(true);
        by_pipeline
            .get_mut("t")
            .expect("pipeline t")
            .iter_mut()
            .find(|m| m.config.name == "reader")
            .expect("reader")
            .sql = "SELECT x FROM prod.elsewhere.shared".to_string();
        by_pipeline
            .get_mut("t")
            .expect("pipeline t")
            .push(model_reading(
                "external_source",
                ("prod", "elsewhere", "shared"),
                "SELECT 1 AS x",
            ));
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
            .expect("neither claimant can be this qualified read");
        assert!(has_edge(
            &runtime.dag,
            "transformation:external_source",
            "transformation:reader"
        ));
        assert!(!has_edge(
            &runtime.dag,
            "transformation:shared",
            "transformation:reader"
        ));
        assert!(!has_edge(
            &runtime.dag,
            "load:shared",
            "transformation:reader"
        ));
    }

    /// A producer whose target is not fully known is not ruled out by a read
    /// that does not contradict it. A seed with no sidecar `[target]` loads into
    /// the default seed schema, in a catalog its loader picks later, so the read
    /// `main.seeds.orders` may be the seed's table just as it is the model's:
    /// two writers of one table, whose order nothing decides. The DAG is
    /// refused rather than resolved to the one producer whose target is known.
    #[test]
    fn a_seed_whose_unknown_target_could_be_the_read_makes_the_read_ambiguous() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading("orders", ("main", "seeds", "orders"), "SELECT 1 AS id"),
                model_reading(
                    "mart",
                    ("main", "marts", "mart"),
                    "SELECT id FROM main.seeds.orders",
                ),
            ],
        );
        let err = build_runtime_dag(&config, &by_pipeline, &[seed("orders")], None, &no_catalog)
            .expect_err("two possible writers of the read table must refuse");
        let message = err.to_string();
        assert!(
            matches!(err, UnifiedDagError::AmbiguousLabelProducer { .. })
                && message.contains("model 'orders' (target main.seeds.orders)")
                && message.contains("seed 'orders' (target ?.seeds.orders)"),
            "the refusal names both writers and marks what is unknown: {message}"
        );
    }

    /// The default seed schema rules a sidecar-free seed OUT of a read that
    /// names another schema, so the read resolves to the one producer that is
    /// definitely its table. Without that knowledge the seed's unknown schema
    /// would keep every read of a shared label ambiguous.
    #[test]
    fn a_seed_with_no_sidecar_is_ruled_out_by_its_default_schema() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading("orders", ("prod", "silver", "orders"), "SELECT 1 AS id"),
                model_reading(
                    "mart",
                    ("prod", "marts", "mart"),
                    "SELECT id FROM prod.silver.orders",
                ),
            ],
        );
        let runtime =
            build_runtime_dag(&config, &by_pipeline, &[seed("orders")], None, &no_catalog)
                .expect("the read names silver, which the seed cannot be in");
        assert!(
            !has_edge(&runtime.dag, "seed:orders", "transformation:mart"),
            "the seed is not what `prod.silver.orders` names"
        );
        assert!(has_edge(
            &runtime.dag,
            "transformation:orders",
            "transformation:mart"
        ));
    }

    /// When the seed nodes' pipeline is known, so is the catalog a sidecar-free
    /// seed loads into (`main` here, the seed loader's fallback). A model writing
    /// `prod.seeds.orders` is then the only producer a read of that table can
    /// mean — the seed is in `main.seeds.orders` — and the read resolves. With
    /// the seed's catalog unknown the same read is refused: the seed could be in
    /// `prod` too.
    #[test]
    fn a_sidecar_free_seed_in_another_catalog_is_ruled_out_by_its_default_catalog() {
        let make = || {
            let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
            let by_pipeline = owned_by_sole_transformation(
                &config,
                vec![
                    model_reading("orders", ("prod", "seeds", "orders"), "SELECT 1 AS id"),
                    model_reading(
                        "mart",
                        ("prod", "marts", "mart"),
                        "SELECT id FROM prod.seeds.orders",
                    ),
                ],
            );
            (config, by_pipeline)
        };

        let (config, by_pipeline) = make();
        let runtime = build_runtime_dag(
            &config,
            &by_pipeline,
            &[seed("orders")],
            Some("main"),
            &no_catalog,
        )
        .expect("the seed is in `main`, so `prod.seeds.orders` cannot be its table");
        assert!(
            !has_edge(&runtime.dag, "seed:orders", "transformation:mart"),
            "the read names the model's table, not the seed's"
        );

        let (config, by_pipeline) = make();
        assert!(
            matches!(
                build_runtime_dag(&config, &by_pipeline, &[seed("orders")], None, &no_catalog),
                Err(UnifiedDagError::AmbiguousLabelProducer { .. })
            ),
            "with the seed's catalog unknown it could be `prod`"
        );
    }

    /// The same catalog on both sides is two writers of one table, and it is
    /// still refused now that the seed's catalog is known: both are definitely
    /// the table the read names.
    #[test]
    fn a_sidecar_free_seed_and_a_model_in_one_catalog_still_make_the_read_ambiguous() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let by_pipeline = owned_by_sole_transformation(
            &config,
            vec![
                model_reading("orders", ("main", "seeds", "orders"), "SELECT 1 AS id"),
                model_reading(
                    "mart",
                    ("main", "marts", "mart"),
                    "SELECT id FROM main.seeds.orders",
                ),
            ],
        );
        let err = build_runtime_dag(
            &config,
            &by_pipeline,
            &[seed("orders")],
            Some("main"),
            &no_catalog,
        )
        .expect_err("two definite writers of the read table must refuse");
        let message = err.to_string();
        assert!(
            message.contains("model 'orders' (target main.seeds.orders)")
                && message.contains("seed 'orders' (target main.seeds.orders)"),
            "both writers are fully known: {message}"
        );
    }

    /// A seed's sidecar `[target]` that names a schema but no catalog takes the
    /// same default catalog as one with no sidecar, as the seed loader gives it.
    /// The model is in another catalog, so the read of `prod.bronze.shared` is
    /// the seed's alone — an edge only the label pass can add.
    #[test]
    fn a_sidecar_target_with_no_catalog_takes_the_seed_default_catalog() {
        let make = || {
            let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
            let by_pipeline = owned_by_sole_transformation(
                &config,
                vec![
                    model_reading("shared", ("other", "bronze", "shared"), "SELECT 1 AS y"),
                    model_reading(
                        "reader",
                        ("prod", "silver", "reader_output"),
                        "SELECT y FROM prod.bronze.shared",
                    ),
                ],
            );
            let mut sidecar_seed = seed("shared");
            sidecar_seed.config.target = Some(crate::seeds::SeedTarget {
                catalog: None,
                schema: "bronze".into(),
                table: None,
            });
            (config, by_pipeline, sidecar_seed)
        };

        let (config, by_pipeline, sidecar_seed) = make();
        let runtime = build_runtime_dag(
            &config,
            &by_pipeline,
            &[sidecar_seed],
            Some("prod"),
            &no_catalog,
        )
        .expect("the seed is in `prod`, the model is not");
        assert!(has_edge(
            &runtime.dag,
            "seed:shared",
            "transformation:reader"
        ));

        let (config, by_pipeline, sidecar_seed) = make();
        assert!(
            matches!(
                build_runtime_dag(&config, &by_pipeline, &[sidecar_seed], None, &no_catalog),
                Err(UnifiedDagError::AmbiguousLabelProducer { .. })
            ),
            "with no default catalog the seed could be anywhere, so it is not named"
        );
    }

    /// A catalogless model whose catalog Rocky could not establish could be in
    /// the catalog a read names, so it is not ruled out — and the read, which
    /// also names the load's target exactly, is refused. Once the adapter
    /// establishes the model's catalog as a different one, the model is ruled
    /// out and the read resolves to the load.
    #[test]
    fn a_catalogless_claimant_is_ruled_out_only_by_an_established_catalog() {
        let make = || {
            let config = duckdb_config(vec![
                ("shared", load_pipeline("prod", "bronze", Some("shared"))),
                ("t", transform_pipeline(vec![])),
            ]);
            let by_pipeline = owned_by_sole_transformation(
                &config,
                vec![
                    model_reading("shared", ("", "bronze", "shared"), "SELECT 1 AS y"),
                    model_reading(
                        "reader",
                        ("prod", "silver", "reader_output"),
                        "SELECT y FROM prod.bronze.shared",
                    ),
                ],
            );
            (config, by_pipeline)
        };

        let (config, by_pipeline) = make();
        assert!(
            matches!(
                build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog),
                Err(UnifiedDagError::AmbiguousLabelProducer { .. })
            ),
            "the model's catalog is unknown, so it could be `prod`"
        );

        let (config, by_pipeline) = make();
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &duckdb_stem_catalog)
            .expect("the model lives in `db`, so the read cannot be its table");
        assert!(has_edge(
            &runtime.dag,
            "load:shared",
            "transformation:reader"
        ));
    }

    /// A collision nobody reads orders nothing, so there is nothing to
    /// refuse: it is reported and the run goes on.
    #[test]
    fn a_colliding_label_nobody_reads_is_reported_not_refused() {
        let (config, mut by_pipeline) = shared_label_project(true);
        for m in by_pipeline.get_mut("t").expect("pipeline t") {
            m.sql = "SELECT 1 AS x".to_string();
        }
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
            .expect("no reader, no ambiguity");
        assert_eq!(
            runtime.labels.label_collisions,
            vec![("shared".to_string(), 2)]
        );
        assert!(
            runtime
                .warnings
                .iter()
                .any(|w| w.contains("label 'shared'")),
            "{:?}",
            runtime.warnings
        );
    }

    /// #1275 cycle policy inside the unified DAG: mutual physical reads must
    /// not make `execution_phases` refuse — one direction is applied, the
    /// closer is skipped and reported.
    #[test]
    fn mutual_physical_reads_do_not_make_the_executor_refuse() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut a = model("a", vec![], vec![]);
        a.sql = "SELECT x FROM silver.b".into();
        let mut b = model("b", vec![], vec![]);
        b.sql = "SELECT y FROM silver.a".into();
        let models = vec![a, b];
        let by_pipeline = owned_by_sole_transformation(&config, models.clone());
        let mut dag = build_unified_dag(&config, &by_pipeline, &[]).expect("build dag");

        let inputs: Vec<crate::physical_edges::PhysicalEdgeModel<'_>> = models
            .iter()
            .map(crate::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
        let derived = infer_physical_dependencies(&mut dag, &inputs);
        assert_eq!(derived.edges.len(), 1);
        assert_eq!(derived.skipped_cycle_edges.len(), 1);
        execution_phases(&dag).expect("phases must still compute — no cycle may be introduced");
    }

    /// #1275 guard depth: a REAL dependency path between two transformations
    /// that runs THROUGH a non-transformation node (here: a quality node) is
    /// invisible to the derivation's transformation-projected cycle guard —
    /// the insertion-time full-graph guard must catch it, or the derived
    /// edge would make `execution_phases` refuse a project that runs today.
    #[test]
    fn a_cycle_through_a_non_transformation_node_is_caught_at_insertion() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut a = model("a", vec![], vec![]);
        a.sql = "SELECT 1 AS x".into();
        let mut b = model("b", vec![], vec![]);
        b.sql = "SELECT x FROM silver.a".into();
        let models = vec![a, b];
        let by_pipeline = owned_by_sole_transformation(&config, models.clone());
        let mut dag = build_unified_dag(&config, &by_pipeline, &[]).expect("build dag");

        let a_id = dag
            .nodes
            .iter()
            .find(|n| n.label == "a" && n.kind == NodeKind::Transformation)
            .unwrap()
            .id
            .clone();
        let b_id = dag
            .nodes
            .iter()
            .find(|n| n.label == "b" && n.kind == NodeKind::Transformation)
            .unwrap()
            .id
            .clone();
        let check_id = NodeId("check:a_gate".to_string());
        dag.nodes.push(UnifiedNode {
            id: check_id.clone(),
            kind: NodeKind::Quality,
            label: "a_gate".into(),
            pipeline: Some("t".into()),
        });
        dag.edges.push(UnifiedEdge {
            from: b_id.clone(),
            to: check_id.clone(),
            edge_type: EdgeType::CheckDependency,
        });
        dag.edges.push(UnifiedEdge {
            from: check_id,
            to: a_id,
            edge_type: EdgeType::CheckDependency,
        });

        let inputs: Vec<crate::physical_edges::PhysicalEdgeModel<'_>> = models
            .iter()
            .map(crate::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
        let derived = infer_physical_dependencies(&mut dag, &inputs);
        assert!(
            derived.edges.is_empty(),
            "the closing edge must be skipped, not inserted: {derived:?}"
        );
        assert_eq!(derived.skipped_cycle_edges.len(), 1);
        execution_phases(&dag).expect("a derived edge must never make a runnable project refuse");
    }

    /// #1275 second-order guard: when insertion rejects the name-accepted
    /// direction of a mutual pair (a real cycle through an intermediate),
    /// the name-level-skipped direction must be reconsidered — otherwise the
    /// pair ends up with NO edge and silently co-schedules.
    #[test]
    fn a_rejected_direction_reinstates_the_skipped_one() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        // Mutual physical reads: candidates ("a","b") then ("b","a") in
        // deterministic order; the derivation accepts ("a","b") and skips
        // ("b","a").
        let mut a = model("a", vec![], vec![]);
        a.sql = "SELECT x FROM silver.b".into();
        let mut b = model("b", vec![], vec![]);
        b.sql = "SELECT y FROM silver.a".into();
        let models = vec![a, b];
        let by_pipeline = owned_by_sole_transformation(&config, models.clone());
        let mut dag = build_unified_dag(&config, &by_pipeline, &[]).expect("build dag");

        // Pre-existing intermediate path a → gate → b, so inserting the
        // accepted ("a","b") edge (b before a) would close a cycle at the
        // NodeId level and gets rejected there.
        let a_id = dag
            .nodes
            .iter()
            .find(|n| n.label == "a" && n.kind == NodeKind::Transformation)
            .unwrap()
            .id
            .clone();
        let b_id = dag
            .nodes
            .iter()
            .find(|n| n.label == "b" && n.kind == NodeKind::Transformation)
            .unwrap()
            .id
            .clone();
        let gate_id = NodeId("check:gate".to_string());
        dag.nodes.push(UnifiedNode {
            id: gate_id.clone(),
            kind: NodeKind::Quality,
            label: "gate".into(),
            pipeline: Some("t".into()),
        });
        dag.edges.push(UnifiedEdge {
            from: a_id.clone(),
            to: gate_id.clone(),
            edge_type: EdgeType::CheckDependency,
        });
        dag.edges.push(UnifiedEdge {
            from: gate_id,
            to: b_id.clone(),
            edge_type: EdgeType::CheckDependency,
        });

        let inputs: Vec<crate::physical_edges::PhysicalEdgeModel<'_>> = models
            .iter()
            .map(crate::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
        let derived = infer_physical_dependencies(&mut dag, &inputs);
        assert_eq!(
            derived.edges,
            vec![("b".to_string(), "a".to_string())],
            "the skipped direction must be reinstated: {derived:?}"
        );
        assert_eq!(
            derived.skipped_cycle_edges,
            vec![("a".to_string(), "b".to_string())]
        );
        let phases = execution_phases(&dag).expect("no cycle");
        let pos = |label: &str| {
            phases
                .iter()
                .position(|l| l.iter().any(|n| n.label == label))
                .unwrap()
        };
        assert!(pos("a") < pos("b"), "consistent with the intermediate path");
    }

    /// #1351: a transformation whose SQL cannot be parsed is REPORTED, not
    /// silently skipped.
    #[test]
    fn unparseable_sql_is_reported_by_label_inference() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut broken = model("broken", vec![], vec![]);
        broken.sql = "SELEC x FRM (".into();
        let by_pipeline = owned_by_sole_transformation(&config, vec![broken.clone()]);
        let mut dag = build_unified_dag(&config, &by_pipeline, &[]).expect("build dag");
        let sql: HashMap<String, String> =
            HashMap::from([("broken".to_string(), broken.sql.clone())]);
        let report = infer_labels(&mut dag, &sql);
        assert_eq!(report.unparsed, vec!["broken".to_string()]);
        assert!(!report.warnings().is_empty());
    }

    /// Status quo pinned: genuinely reciprocal label reads REFUSE loudly
    /// (as the single-slot heuristic always did) — the guard must not
    /// downgrade a real SQL cycle into a silent stale-read success.
    #[test]
    fn mutual_label_reads_still_refuse_loudly() {
        let config = config_with_pipelines(vec![("t", transform_pipeline(vec![]))]);
        let mut a = model("a", vec![], vec![]);
        a.sql = "SELECT x FROM b".into();
        let mut b = model("b", vec![], vec![]);
        b.sql = "SELECT y FROM a".into();
        let models = vec![a.clone(), b.clone()];
        let by_pipeline = owned_by_sole_transformation(&config, models);
        let runtime = build_runtime_dag(&config, &by_pipeline, &[], None, &no_catalog)
            .expect("the DAG builds; the cycle is the executor's to refuse");
        assert!(
            runtime.labels.label_collisions.is_empty()
                && runtime.labels.unparsed.is_empty()
                && runtime.labels.skipped_cycle_edges.is_empty(),
            "nothing to report for a clean mutual pair: {:?}",
            runtime.labels
        );
        assert!(
            execution_phases(&runtime.dag).is_err(),
            "a genuine SQL cycle keeps its loud refusal"
        );
    }
}
