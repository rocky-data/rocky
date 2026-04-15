//! Unified ELT DAG — read-only view of all pipeline stages as a single graph.
//!
//! Rocky currently models pipelines as separate [`PipelineConfig`] variants
//! (replication, transformation, quality, snapshot, load). This module
//! introduces foundation types that represent every stage as a node in one
//! directed acyclic graph, with typed edges expressing data-flow and
//! governance dependencies.
//!
//! **This is a read-only projection** of the existing config — it does not
//! replace [`PipelineConfig`] or change how pipelines execute. Future work
//! (Plan 50 phases 2+) will wire the unified DAG into the execution engine.
//!
//! [`PipelineConfig`]: crate::config::PipelineConfig

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{PipelineConfig, RockyConfig};
use crate::models::Model;

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
}

// ---------------------------------------------------------------------------
// DAG construction
// ---------------------------------------------------------------------------

/// Builds a unified DAG from the current configuration and loaded models.
///
/// Each pipeline in `config.pipelines` becomes one or more nodes:
/// - **Replication** pipelines become a single `Replication` node.
/// - **Transformation** pipelines expand into per-model `Transformation`
///   nodes (and `Seed` / `Test` nodes when applicable).
/// - **Quality** pipelines become a single `Quality` node.
/// - **Snapshot** pipelines become a single `Snapshot` node.
/// - **Load** pipelines become a single `Load` node.
///
/// Edges are derived from:
/// - `depends_on` at the pipeline level (inter-pipeline chaining).
/// - `depends_on` at the model level (intra-pipeline model ordering).
/// - Implicit test-after-model relationships.
pub fn build_unified_dag(
    config: &RockyConfig,
    models: &[Model],
) -> Result<UnifiedDag, UnifiedDagError> {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    // Track pipeline-name -> list of node IDs, so inter-pipeline depends_on
    // can wire edges from the *last* node of the upstream pipeline.
    let mut pipeline_node_ids: HashMap<String, Vec<NodeId>> = HashMap::new();

    for (pipeline_name, pipeline_cfg) in &config.pipelines {
        match pipeline_cfg {
            PipelineConfig::Replication(_) => {
                let node_id = NodeId::new("replication", pipeline_name);
                nodes.push(UnifiedNode {
                    id: node_id.clone(),
                    kind: NodeKind::Replication,
                    label: pipeline_name.clone(),
                    pipeline: Some(pipeline_name.clone()),
                });
                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .push(node_id);
            }
            PipelineConfig::Transformation(_) => {
                // Filter models belonging to this pipeline. In practice the
                // caller passes models loaded from the glob of the relevant
                // transformation pipeline. We add all provided models.
                add_transformation_nodes(
                    pipeline_name,
                    models,
                    &mut nodes,
                    &mut edges,
                    &mut pipeline_node_ids,
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
                pipeline_node_ids
                    .entry(pipeline_name.clone())
                    .or_default()
                    .push(node_id);
            }
        }
    }

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
                    .map(|n| n.kind != NodeKind::Test)
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
                // Exclude test nodes from exit — tests don't gate downstream
                // pipelines.
                .filter(|id| {
                    nodes
                        .iter()
                        .find(|n| n.id == **id)
                        .map(|n| n.kind != NodeKind::Test)
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

/// Expands a transformation pipeline into per-model nodes, seed nodes,
/// and test nodes, wiring intra-pipeline edges.
fn add_transformation_nodes(
    pipeline_name: &str,
    models: &[Model],
    nodes: &mut Vec<UnifiedNode>,
    edges: &mut Vec<UnifiedEdge>,
    pipeline_node_ids: &mut HashMap<String, Vec<NodeId>>,
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
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Relationships { .. } => "relationships",
        TestType::Expression { .. } => "expression",
        TestType::RowCountRange { .. } => "row_count_range",
    };

    match &test.column {
        Some(col) => format!("{model_name}::{type_str}_{col}"),
        None => format!("{model_name}::{type_str}_{index}"),
    }
}

// ---------------------------------------------------------------------------
// Execution phases (parallel layers)
// ---------------------------------------------------------------------------

/// Computes parallel execution phases from the unified DAG.
///
/// Returns groups of node references that can execute concurrently. Within
/// each phase, all upstream dependencies (across all edge types) have been
/// satisfied by previous phases. This is the unified-DAG equivalent of
/// [`crate::dag::execution_layers`].
///
/// Returns an error if the DAG contains a cycle.
pub fn execution_phases(dag: &UnifiedDag) -> Result<Vec<Vec<&UnifiedNode>>, UnifiedDagError> {
    // Build adjacency structures keyed by NodeId.
    let node_map: HashMap<&NodeId, &UnifiedNode> = dag.nodes.iter().map(|n| (&n.id, n)).collect();

    let mut in_degree: HashMap<&NodeId, usize> = HashMap::new();
    let mut dependents: HashMap<&NodeId, Vec<&NodeId>> = HashMap::new();

    for node in &dag.nodes {
        in_degree.entry(&node.id).or_insert(0);
    }

    for edge in &dag.edges {
        *in_degree.entry(&edge.to).or_insert(0) += 1;
        dependents.entry(&edge.from).or_default().push(&edge.to);
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
            let layer = dag
                .edges
                .iter()
                .filter(|e| e.to == **id)
                .filter_map(|e| node_layer.get(&e.from))
                .max()
                .map(|&max_dep| max_dep + 1)
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use crate::models::{ModelConfig, StrategyConfig, TargetConfig};
    use crate::tests::{TestDecl, TestSeverity, TestType};
    use indexmap::IndexMap;

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
            schema_evolution: Default::default(),
        }
    }

    /// Helper: build a minimal replication pipeline config.
    fn repl_pipeline(depends_on: Vec<&str>) -> PipelineConfig {
        PipelineConfig::Replication(Box::new(ReplicationPipelineConfig {
            strategy: "incremental".into(),
            timestamp_column: "_fivetran_synced".into(),
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
                intent: None,
                freshness: None,
                tests,
                format: None,
                format_options: None,
            },
            sql: format!("SELECT * FROM upstream_{name}"),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    // -----------------------------------------------------------------------
    // DAG construction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_single_replication_pipeline() {
        let config = config_with_pipelines(vec![("raw_ingest", repl_pipeline(vec![]))]);
        let dag = build_unified_dag(&config, &[]).unwrap();

        assert_eq!(dag.nodes.len(), 1);
        assert_eq!(dag.nodes[0].kind, NodeKind::Replication);
        assert_eq!(dag.nodes[0].label, "raw_ingest");
        assert!(dag.edges.is_empty());
    }

    #[test]
    fn test_pipeline_chaining() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![model("stg_orders", vec![], vec![])];
        let dag = build_unified_dag(&config, &models).unwrap();

        // 1 replication + 1 transformation model = 2 nodes
        assert_eq!(dag.nodes.len(), 2);

        // Should have one inter-pipeline edge: raw_ingest -> stg_orders
        assert_eq!(dag.edges.len(), 1);
        assert_eq!(dag.edges[0].from, NodeId::new("replication", "raw_ingest"));
        assert_eq!(dag.edges[0].to, NodeId::new("transformation", "stg_orders"));
        assert_eq!(dag.edges[0].edge_type, EdgeType::DataDependency);
    }

    #[test]
    fn test_model_dependencies() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("stg_customers", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "stg_customers"], vec![]),
        ];

        let dag = build_unified_dag(&config, &models).unwrap();

        assert_eq!(dag.nodes.len(), 3);

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
            },
            TestDecl {
                test_type: TestType::Unique,
                column: Some("order_id".into()),
                severity: TestSeverity::Error,
            },
        ];

        let models = vec![model("stg_orders", vec![], test_decls)];
        let dag = build_unified_dag(&config, &models).unwrap();

        // 1 model + 2 test nodes
        assert_eq!(dag.nodes.len(), 3);

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
        let result = build_unified_dag(&config, &models);
        assert!(matches!(
            result,
            Err(UnifiedDagError::UnknownDependency { .. })
        ));
    }

    #[test]
    fn test_empty_config() {
        let config = config_with_pipelines(vec![]);
        let dag = build_unified_dag(&config, &[]).unwrap();
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

        let dag = build_unified_dag(&config, &models).unwrap();

        // 1 replication + 2 transformation + 1 quality = 4 nodes
        assert_eq!(dag.nodes.len(), 4);

        // Inter-pipeline edges: raw_ingest -> stg_orders, raw_ingest -> dim_customers,
        // stg_orders -> nightly_dq, dim_customers -> nightly_dq
        let inter_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataDependency)
            .collect();
        assert_eq!(inter_edges.len(), 4);
    }

    // -----------------------------------------------------------------------
    // Execution phase tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_phases_linear_chain() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![model("stg_orders", vec![], vec![])];
        let dag = build_unified_dag(&config, &models).unwrap();
        let phases = execution_phases(&dag).unwrap();

        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].len(), 1);
        assert_eq!(phases[0][0].kind, NodeKind::Replication);
        assert_eq!(phases[1].len(), 1);
        assert_eq!(phases[1][0].kind, NodeKind::Transformation);
    }

    #[test]
    fn test_phases_parallel_models() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("stg_customers", vec![], vec![]),
            model("fct_orders", vec!["stg_orders", "stg_customers"], vec![]),
        ];

        let dag = build_unified_dag(&config, &models).unwrap();
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
        }];

        let models = vec![model("stg_orders", vec![], test_decls)];
        let dag = build_unified_dag(&config, &models).unwrap();
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

        let dag = build_unified_dag(&config, &models).unwrap();
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

        let dag = build_unified_dag(&config, &models).unwrap();
        let phases = execution_phases(&dag).unwrap();

        assert_eq!(phases.len(), 3);
        assert_eq!(phases[0].len(), 1); // a
        assert_eq!(phases[0][0].label, "a");
        assert_eq!(phases[1].len(), 2); // b, c parallel
        assert_eq!(phases[2].len(), 1); // d
        assert_eq!(phases[2][0].label, "d");
    }

    #[test]
    fn test_dag_roots_and_leaves() {
        let config = config_with_pipelines(vec![
            ("raw_ingest", repl_pipeline(vec![])),
            ("silver", transform_pipeline(vec!["raw_ingest"])),
        ]);

        let models = vec![
            model("stg_orders", vec![], vec![]),
            model("fct_orders", vec!["stg_orders"], vec![]),
        ];

        let dag = build_unified_dag(&config, &models).unwrap();

        let roots = dag.roots();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], &NodeId::new("replication", "raw_ingest"));

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
        };
        let label = format_test_label("stg_orders", &test, 2);
        assert_eq!(label, "stg_orders::row_count_range_2");
    }

    #[test]
    fn test_outgoing_and_incoming_edges() {
        let config = config_with_pipelines(vec![("silver", transform_pipeline(vec![]))]);

        let models = vec![model("a", vec![], vec![]), model("b", vec!["a"], vec![])];

        let dag = build_unified_dag(&config, &models).unwrap();

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
}
