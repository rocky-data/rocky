//! Cross-engine multi-warehouse project types.
//!
//! Foundation for Plan 39: projects that span multiple warehouses within a
//! single `rocky.toml` — e.g., replicate from Fivetran to Databricks AND
//! transform in Snowflake.
//!
//! This module provides:
//! - [`CrossEngineProject`] — pipelines with their resolved adapters and
//!   cross-pipeline dependency edges.
//! - [`PipelineResolution`] — a single pipeline with its source/target
//!   adapter types and execution order.
//! - [`validate_cross_engine_deps`] — checks for latency and consistency
//!   risks when pipeline A's target feeds pipeline B's source across
//!   different warehouses.
//! - [`resolve_execution_graph`] — topological sort of pipelines respecting
//!   `depends_on`, producing an ordered list of [`PipelineResolution`]s.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{PipelineConfig, RockyConfig};
use crate::dag::{self, DagNode};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A resolved cross-engine project: all pipelines with their adapter
/// assignments and the dependency edges between them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossEngineProject {
    /// Pipelines in topologically sorted execution order.
    pub pipelines: Vec<PipelineResolution>,

    /// Cross-pipeline dependency edges where the source and target adapters
    /// differ (i.e., data crosses warehouse boundaries).
    pub cross_warehouse_edges: Vec<CrossWarehouseEdge>,
}

/// A single pipeline resolved with its adapter types and execution order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineResolution {
    /// Pipeline name (key in `[pipeline.*]`).
    pub pipeline_name: String,

    /// Adapter type for the pipeline's source side (e.g., `"fivetran"`,
    /// `"databricks"`). `None` for pipeline types that have no distinct
    /// source adapter (transformation, quality, load).
    pub source_adapter: Option<String>,

    /// Adapter type for the pipeline's target side (e.g., `"databricks"`,
    /// `"snowflake"`).
    pub target_adapter: String,

    /// Zero-based position in the topological execution order.
    pub execution_order: usize,
}

/// An edge between two pipelines where data crosses a warehouse boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossWarehouseEdge {
    /// The upstream pipeline whose target feeds the downstream pipeline.
    pub source_pipeline: String,

    /// The downstream pipeline that consumes from the upstream target.
    pub target_pipeline: String,

    /// Adapter type on the upstream side (the target adapter of
    /// `source_pipeline`).
    pub upstream_adapter_type: String,

    /// Adapter type on the downstream side (the source adapter of
    /// `target_pipeline`).
    pub downstream_adapter_type: String,
}

/// Warning emitted by cross-engine dependency validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossEngineWarning {
    /// The upstream pipeline name.
    pub source_pipeline: String,

    /// The downstream pipeline name.
    pub target_pipeline: String,

    /// Classification of the warning.
    pub warning_type: CrossEngineWarningType,

    /// Human-readable explanation.
    pub message: String,
}

/// Classification of cross-engine warnings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossEngineWarningType {
    /// The downstream pipeline reads from a different warehouse than the
    /// upstream pipeline writes to, introducing network latency.
    LatencyRisk,

    /// Cross-warehouse data flow has no transactional guarantee — the
    /// downstream pipeline may see partially written data.
    ConsistencyRisk,

    /// One of the adapters in the cross-warehouse edge is not a recognized
    /// warehouse type, so Rocky cannot assess the risk.
    UnsupportedAdapter,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from cross-engine graph resolution.
#[derive(Debug, Error)]
pub enum CrossEngineError {
    #[error("pipeline '{pipeline}' references unknown adapter '{adapter}'")]
    UnknownAdapter { pipeline: String, adapter: String },

    #[error(transparent)]
    DagError(#[from] dag::DagError),
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Checks cross-pipeline dependencies for latency and consistency risks.
///
/// When pipeline A's target adapter differs from pipeline B's source adapter
/// (and B depends on A), the data flow crosses warehouse boundaries. This
/// function emits warnings for each such edge.
///
/// Returns an empty `Vec` when all pipelines target the same warehouse or
/// when there are no inter-pipeline dependencies.
pub fn validate_cross_engine_deps(config: &RockyConfig) -> Vec<CrossEngineWarning> {
    let mut warnings = Vec::new();

    for (name, pipeline) in &config.pipelines {
        for dep_name in pipeline.depends_on() {
            let Some(dep_pipeline) = config.pipelines.get(dep_name) else {
                // Unknown dependency — the DAG resolver will catch this.
                continue;
            };

            let upstream_target_adapter_name = dep_pipeline.target_adapter();
            let downstream_source_adapter_name = source_adapter_name(pipeline);

            // Look up the adapter types from the config.
            let upstream_type = config
                .adapters
                .get(upstream_target_adapter_name)
                .map(|a| a.adapter_type.as_str());
            let downstream_type = config
                .adapters
                .get(downstream_source_adapter_name)
                .map(|a| a.adapter_type.as_str());

            match (upstream_type, downstream_type) {
                (Some(up), Some(down)) if up != down => {
                    warnings.push(CrossEngineWarning {
                        source_pipeline: dep_name.clone(),
                        target_pipeline: name.clone(),
                        warning_type: CrossEngineWarningType::LatencyRisk,
                        message: format!(
                            "pipeline '{name}' reads from '{down}' but depends on \
                             '{dep_name}' which writes to '{up}' — cross-warehouse \
                             data transfer adds network latency"
                        ),
                    });
                    warnings.push(CrossEngineWarning {
                        source_pipeline: dep_name.clone(),
                        target_pipeline: name.clone(),
                        warning_type: CrossEngineWarningType::ConsistencyRisk,
                        message: format!(
                            "cross-warehouse dependency {dep_name} ({up}) -> \
                             {name} ({down}) has no transactional guarantee — \
                             downstream may observe partially written data"
                        ),
                    });
                }
                (None, _) => {
                    warnings.push(CrossEngineWarning {
                        source_pipeline: dep_name.clone(),
                        target_pipeline: name.clone(),
                        warning_type: CrossEngineWarningType::UnsupportedAdapter,
                        message: format!(
                            "pipeline '{dep_name}' references adapter \
                             '{upstream_target_adapter_name}' which is not defined \
                             in the config"
                        ),
                    });
                }
                (_, None) => {
                    warnings.push(CrossEngineWarning {
                        source_pipeline: dep_name.clone(),
                        target_pipeline: name.clone(),
                        warning_type: CrossEngineWarningType::UnsupportedAdapter,
                        message: format!(
                            "pipeline '{name}' references adapter \
                             '{downstream_source_adapter_name}' which is not defined \
                             in the config"
                        ),
                    });
                }
                _ => {
                    // Same adapter type — no cross-warehouse risk.
                }
            }
        }
    }

    warnings
}

// ---------------------------------------------------------------------------
// Execution graph resolution
// ---------------------------------------------------------------------------

/// Resolves the execution graph for all pipelines in the config.
///
/// Performs a topological sort of pipelines respecting their `depends_on`
/// fields and resolves each pipeline's source and target adapter types.
///
/// Returns [`PipelineResolution`]s in execution order (dependencies before
/// dependents).
pub fn resolve_execution_graph(
    config: &RockyConfig,
) -> Result<Vec<PipelineResolution>, CrossEngineError> {
    // Build DAG nodes from pipeline configs.
    let dag_nodes: Vec<DagNode> = config
        .pipelines
        .iter()
        .map(|(name, pipeline)| DagNode {
            name: name.clone(),
            depends_on: pipeline.depends_on().to_vec(),
        })
        .collect();

    let sorted_names = dag::topological_sort(&dag_nodes)?;

    let mut resolutions = Vec::with_capacity(sorted_names.len());
    for (order, name) in sorted_names.into_iter().enumerate() {
        let pipeline = config.pipelines.get(&name).expect(
            "topological_sort only returns names from the input nodes; \
             pipeline must exist in config",
        );

        let target_adapter_name = pipeline.target_adapter();
        let target_type = config
            .adapters
            .get(target_adapter_name)
            .map(|a| a.adapter_type.clone())
            .ok_or_else(|| CrossEngineError::UnknownAdapter {
                pipeline: name.clone(),
                adapter: target_adapter_name.to_owned(),
            })?;

        let source_adapter_type = resolve_source_adapter_type(config, &name, pipeline)?;

        resolutions.push(PipelineResolution {
            pipeline_name: name,
            source_adapter: source_adapter_type,
            target_adapter: target_type,
            execution_order: order,
        });
    }

    Ok(resolutions)
}

/// Builds a [`CrossEngineProject`] from a resolved execution graph and its
/// warnings.
///
/// Convenience function that combines [`resolve_execution_graph`] with
/// [`validate_cross_engine_deps`].
pub fn build_cross_engine_project(
    config: &RockyConfig,
) -> Result<CrossEngineProject, CrossEngineError> {
    let pipelines = resolve_execution_graph(config)?;

    // Identify cross-warehouse edges.
    let cross_warehouse_edges = find_cross_warehouse_edges(config);

    Ok(CrossEngineProject {
        pipelines,
        cross_warehouse_edges,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the source adapter name for a pipeline.
///
/// For pipeline types with a distinct source (replication, snapshot), returns
/// the source adapter name. For others (transformation, quality, load),
/// returns the target adapter name since they operate on a single warehouse.
fn source_adapter_name(pipeline: &PipelineConfig) -> &str {
    match pipeline {
        PipelineConfig::Replication(r) => &r.source.adapter,
        PipelineConfig::Snapshot(s) => &s.source.adapter,
        PipelineConfig::Transformation(t) => &t.target.adapter,
        PipelineConfig::Quality(q) => &q.target.adapter,
        PipelineConfig::Load(l) => &l.target.adapter,
    }
}

/// Resolves the source adapter type string for a pipeline, returning `None`
/// for pipeline types that have no distinct source adapter.
fn resolve_source_adapter_type(
    config: &RockyConfig,
    pipeline_name: &str,
    pipeline: &PipelineConfig,
) -> Result<Option<String>, CrossEngineError> {
    match pipeline {
        PipelineConfig::Replication(r) => {
            let adapter = config.adapters.get(&r.source.adapter).ok_or_else(|| {
                CrossEngineError::UnknownAdapter {
                    pipeline: pipeline_name.to_owned(),
                    adapter: r.source.adapter.clone(),
                }
            })?;
            Ok(Some(adapter.adapter_type.clone()))
        }
        PipelineConfig::Snapshot(s) => {
            let adapter = config.adapters.get(&s.source.adapter).ok_or_else(|| {
                CrossEngineError::UnknownAdapter {
                    pipeline: pipeline_name.to_owned(),
                    adapter: s.source.adapter.clone(),
                }
            })?;
            Ok(Some(adapter.adapter_type.clone()))
        }
        // Transformation, quality, and load pipelines have no distinct source adapter.
        PipelineConfig::Transformation(_)
        | PipelineConfig::Quality(_)
        | PipelineConfig::Load(_) => Ok(None),
    }
}

/// Finds all dependency edges that cross warehouse boundaries.
fn find_cross_warehouse_edges(config: &RockyConfig) -> Vec<CrossWarehouseEdge> {
    let mut edges = Vec::new();

    for (name, pipeline) in &config.pipelines {
        for dep_name in pipeline.depends_on() {
            let Some(dep_pipeline) = config.pipelines.get(dep_name) else {
                continue;
            };

            let upstream_target_name = dep_pipeline.target_adapter();
            let downstream_source_name = source_adapter_name(pipeline);

            let upstream_type = config
                .adapters
                .get(upstream_target_name)
                .map(|a| a.adapter_type.as_str());
            let downstream_type = config
                .adapters
                .get(downstream_source_name)
                .map(|a| a.adapter_type.as_str());

            if let (Some(up), Some(down)) = (upstream_type, downstream_type) {
                if up != down {
                    edges.push(CrossWarehouseEdge {
                        source_pipeline: dep_name.clone(),
                        target_pipeline: name.clone(),
                        upstream_adapter_type: up.to_owned(),
                        downstream_adapter_type: down.to_owned(),
                    });
                }
            }
        }
    }

    edges
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AdapterConfig, ChecksConfig, ExecutionConfig, GovernanceConfig, PipelineConfig,
        PipelineSourceConfig, PipelineTargetConfig, QualityPipelineConfig, QualityTargetConfig,
        ReplicationPipelineConfig, RetryConfig, SchemaPatternConfig, SnapshotPipelineConfig,
        SnapshotSourceConfig, SnapshotTargetConfig, TransformationPipelineConfig,
        TransformationTargetConfig,
    };
    use indexmap::IndexMap;

    /// Helper: builds a minimal `AdapterConfig` with the given type.
    fn adapter(adapter_type: &str) -> AdapterConfig {
        AdapterConfig {
            adapter_type: adapter_type.to_owned(),
            host: None,
            http_path: None,
            token: None,
            client_id: None,
            client_secret: None,
            timeout_secs: None,
            destination_id: None,
            api_key: None,
            api_secret: None,
            account: None,
            warehouse: None,
            username: None,
            password: None,
            oauth_token: None,
            private_key_path: None,
            role: None,
            database: None,
            project_id: None,
            location: None,
            path: None,
            retry: RetryConfig::default(),
        }
    }

    /// Helper: builds a minimal replication pipeline config.
    fn replication_pipeline(
        source_adapter: &str,
        target_adapter: &str,
        depends_on: Vec<String>,
    ) -> PipelineConfig {
        PipelineConfig::Replication(Box::new(ReplicationPipelineConfig {
            strategy: "incremental".into(),
            timestamp_column: "_fivetran_synced".into(),
            metadata_columns: vec![],
            source: PipelineSourceConfig {
                adapter: source_adapter.into(),
                catalog: Some("raw_catalog".into()),
                schema_pattern: SchemaPatternConfig {
                    prefix: "src__".into(),
                    separator: "__".into(),
                    components: vec!["client".into(), "source".into()],
                },
                discovery: None,
            },
            target: PipelineTargetConfig {
                adapter: target_adapter.into(),
                catalog_template: "{client}_warehouse".into(),
                schema_template: "raw__{source}".into(),
                separator: None,
                governance: GovernanceConfig::default(),
            },
            checks: ChecksConfig::default(),
            execution: ExecutionConfig::default(),
            depends_on,
        }))
    }

    /// Helper: builds a minimal transformation pipeline config.
    fn transformation_pipeline(target_adapter: &str, depends_on: Vec<String>) -> PipelineConfig {
        PipelineConfig::Transformation(Box::new(TransformationPipelineConfig {
            models: "models/**".into(),
            target: TransformationTargetConfig {
                adapter: target_adapter.into(),
                governance: GovernanceConfig::default(),
            },
            checks: ChecksConfig::default(),
            execution: ExecutionConfig::default(),
            depends_on,
        }))
    }

    /// Helper: builds a minimal config with given adapters and pipelines.
    fn config(adapters: Vec<(&str, &str)>, pipelines: Vec<(&str, PipelineConfig)>) -> RockyConfig {
        let mut adapter_map = IndexMap::new();
        for (name, adapter_type) in adapters {
            adapter_map.insert(name.to_owned(), adapter(adapter_type));
        }
        let mut pipeline_map = IndexMap::new();
        for (name, pipeline) in pipelines {
            pipeline_map.insert(name.to_owned(), pipeline);
        }
        RockyConfig {
            state: Default::default(),
            adapters: adapter_map,
            pipelines: pipeline_map,
            hooks: Default::default(),
            cost: Default::default(),
            schema_evolution: Default::default(),
        }
    }

    // -- validate_cross_engine_deps tests --

    #[test]
    fn no_warnings_for_single_warehouse() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("db", vec!["bronze".into()]),
                ),
            ],
        );

        let warnings = validate_cross_engine_deps(&cfg);
        assert!(
            warnings.is_empty(),
            "expected no warnings, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warnings_when_no_dependencies() {
        let cfg = config(
            vec![("db", "databricks"), ("sf", "snowflake")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                ("silver", transformation_pipeline("sf", vec![])),
            ],
        );

        let warnings = validate_cross_engine_deps(&cfg);
        assert!(warnings.is_empty());
    }

    #[test]
    fn latency_and_consistency_warnings_for_cross_warehouse_dep() {
        let cfg = config(
            vec![("db", "databricks"), ("sf", "snowflake")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("sf", vec!["bronze".into()]),
                ),
            ],
        );

        let warnings = validate_cross_engine_deps(&cfg);
        assert_eq!(warnings.len(), 2, "expected latency + consistency warnings");

        let types: Vec<_> = warnings.iter().map(|w| &w.warning_type).collect();
        assert!(types.contains(&&CrossEngineWarningType::LatencyRisk));
        assert!(types.contains(&&CrossEngineWarningType::ConsistencyRisk));

        // Verify pipeline names are correct.
        for w in &warnings {
            assert_eq!(w.source_pipeline, "bronze");
            assert_eq!(w.target_pipeline, "silver");
        }
    }

    #[test]
    fn unsupported_adapter_warning_for_missing_adapter() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("missing_adapter", vec!["bronze".into()]),
                ),
            ],
        );

        let warnings = validate_cross_engine_deps(&cfg);
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].warning_type,
            CrossEngineWarningType::UnsupportedAdapter
        );
        assert!(warnings[0].message.contains("missing_adapter"));
    }

    #[test]
    fn multiple_cross_warehouse_edges() {
        let cfg = config(
            vec![
                ("fivetran", "fivetran"),
                ("db", "databricks"),
                ("sf", "snowflake"),
            ],
            vec![
                ("ingest", replication_pipeline("fivetran", "db", vec![])),
                (
                    "transform",
                    transformation_pipeline("sf", vec!["ingest".into()]),
                ),
                (
                    "quality",
                    PipelineConfig::Quality(Box::new(QualityPipelineConfig {
                        target: QualityTargetConfig {
                            adapter: "db".into(),
                        },
                        tables: vec![],
                        checks: ChecksConfig::default(),
                        execution: ExecutionConfig::default(),
                        depends_on: vec!["transform".into()],
                    })),
                ),
            ],
        );

        let warnings = validate_cross_engine_deps(&cfg);
        // ingest(db) -> transform(sf): latency + consistency = 2
        // transform(sf) -> quality(db): latency + consistency = 2
        assert_eq!(warnings.len(), 4);
    }

    // -- resolve_execution_graph tests --

    #[test]
    fn single_pipeline_resolves() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![("bronze", replication_pipeline("db", "db", vec![]))],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        assert_eq!(graph.len(), 1);
        assert_eq!(graph[0].pipeline_name, "bronze");
        assert_eq!(graph[0].source_adapter.as_deref(), Some("databricks"));
        assert_eq!(graph[0].target_adapter, "databricks");
        assert_eq!(graph[0].execution_order, 0);
    }

    #[test]
    fn linear_chain_respects_depends_on() {
        let cfg = config(
            vec![("db", "databricks"), ("sf", "snowflake")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("sf", vec!["bronze".into()]),
                ),
                ("gold", transformation_pipeline("sf", vec!["silver".into()])),
            ],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        assert_eq!(graph.len(), 3);

        let names: Vec<&str> = graph.iter().map(|r| r.pipeline_name.as_str()).collect();
        // bronze (no deps) -> silver (dep: bronze) -> gold (dep: silver)
        assert_eq!(names, vec!["bronze", "silver", "gold"]);

        // Verify execution_order is sequential.
        for (i, resolution) in graph.iter().enumerate() {
            assert_eq!(resolution.execution_order, i);
        }
    }

    #[test]
    fn parallel_pipelines_sorted_alphabetically() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("charlie", replication_pipeline("db", "db", vec![])),
                ("alpha", replication_pipeline("db", "db", vec![])),
                ("bravo", replication_pipeline("db", "db", vec![])),
            ],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        let names: Vec<&str> = graph.iter().map(|r| r.pipeline_name.as_str()).collect();
        // Kahn's algorithm with min-heap gives alphabetical order for peers.
        assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
    }

    #[test]
    fn cycle_detected() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("a", transformation_pipeline("db", vec!["b".into()])),
                ("b", transformation_pipeline("db", vec!["a".into()])),
            ],
        );

        let result = resolve_execution_graph(&cfg);
        assert!(
            matches!(
                result,
                Err(CrossEngineError::DagError(
                    dag::DagError::CyclicDependency { .. }
                ))
            ),
            "expected cycle error, got: {result:?}"
        );
    }

    #[test]
    fn unknown_dependency_detected() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![(
                "lonely",
                transformation_pipeline("db", vec!["nonexistent".into()]),
            )],
        );

        let result = resolve_execution_graph(&cfg);
        assert!(
            matches!(
                result,
                Err(CrossEngineError::DagError(
                    dag::DagError::UnknownDependency { .. }
                ))
            ),
            "expected unknown dependency error, got: {result:?}"
        );
    }

    #[test]
    fn unknown_target_adapter_errors() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![("broken", transformation_pipeline("missing", vec![]))],
        );

        let result = resolve_execution_graph(&cfg);
        assert!(
            matches!(result, Err(CrossEngineError::UnknownAdapter { .. })),
            "expected unknown adapter error, got: {result:?}"
        );
    }

    #[test]
    fn transformation_has_no_source_adapter() {
        let cfg = config(
            vec![("sf", "snowflake")],
            vec![("transform", transformation_pipeline("sf", vec![]))],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        assert_eq!(graph[0].source_adapter, None);
        assert_eq!(graph[0].target_adapter, "snowflake");
    }

    #[test]
    fn snapshot_pipeline_has_source_adapter() {
        let cfg = config(
            vec![("db", "databricks"), ("sf", "snowflake")],
            vec![(
                "customer_history",
                PipelineConfig::Snapshot(Box::new(SnapshotPipelineConfig {
                    unique_key: vec!["customer_id".into()],
                    updated_at: "updated_at".into(),
                    invalidate_hard_deletes: false,
                    source: SnapshotSourceConfig {
                        adapter: "db".into(),
                        catalog: "raw".into(),
                        schema: "shopify".into(),
                        table: "customers".into(),
                    },
                    target: SnapshotTargetConfig {
                        adapter: "sf".into(),
                        catalog: "warehouse".into(),
                        schema: "scd".into(),
                        table: "customers_history".into(),
                        governance: GovernanceConfig::default(),
                    },
                    checks: ChecksConfig::default(),
                    execution: ExecutionConfig::default(),
                    depends_on: vec![],
                })),
            )],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        assert_eq!(graph[0].source_adapter.as_deref(), Some("databricks"));
        assert_eq!(graph[0].target_adapter, "snowflake");
    }

    // -- build_cross_engine_project tests --

    #[test]
    fn build_project_with_cross_warehouse_edges() {
        let cfg = config(
            vec![("db", "databricks"), ("sf", "snowflake")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("sf", vec!["bronze".into()]),
                ),
            ],
        );

        let project = build_cross_engine_project(&cfg).unwrap();
        assert_eq!(project.pipelines.len(), 2);
        assert_eq!(project.cross_warehouse_edges.len(), 1);

        let edge = &project.cross_warehouse_edges[0];
        assert_eq!(edge.source_pipeline, "bronze");
        assert_eq!(edge.target_pipeline, "silver");
        assert_eq!(edge.upstream_adapter_type, "databricks");
        assert_eq!(edge.downstream_adapter_type, "snowflake");
    }

    #[test]
    fn build_project_no_cross_warehouse_edges() {
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("bronze", replication_pipeline("db", "db", vec![])),
                (
                    "silver",
                    transformation_pipeline("db", vec!["bronze".into()]),
                ),
            ],
        );

        let project = build_cross_engine_project(&cfg).unwrap();
        assert_eq!(project.pipelines.len(), 2);
        assert!(project.cross_warehouse_edges.is_empty());
    }

    #[test]
    fn empty_config_produces_empty_project() {
        let cfg = config(vec![], vec![]);
        let project = build_cross_engine_project(&cfg).unwrap();
        assert!(project.pipelines.is_empty());
        assert!(project.cross_warehouse_edges.is_empty());
    }

    #[test]
    fn diamond_dependency_graph() {
        // a -> b, a -> c, b -> d, c -> d
        let cfg = config(
            vec![("db", "databricks")],
            vec![
                ("a", replication_pipeline("db", "db", vec![])),
                ("b", transformation_pipeline("db", vec!["a".into()])),
                ("c", transformation_pipeline("db", vec!["a".into()])),
                (
                    "d",
                    transformation_pipeline("db", vec!["b".into(), "c".into()]),
                ),
            ],
        );

        let graph = resolve_execution_graph(&cfg).unwrap();
        let names: Vec<&str> = graph.iter().map(|r| r.pipeline_name.as_str()).collect();

        // a first, d last, b and c in between (alphabetically: b before c)
        assert_eq!(names[0], "a");
        assert_eq!(names[3], "d");
        assert!(
            names.iter().position(|&n| n == "b").unwrap()
                < names.iter().position(|&n| n == "d").unwrap()
        );
        assert!(
            names.iter().position(|&n| n == "c").unwrap()
                < names.iter().position(|&n| n == "d").unwrap()
        );
    }
}
