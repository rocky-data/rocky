//! Cross-warehouse bridge step planning.
//!
//! When a DAG edge crosses warehouse boundaries (e.g., pipeline A writes to
//! Databricks and pipeline B reads from Snowflake), a **bridge step** is needed
//! to move data between the two warehouses.
//!
//! This module provides:
//! - [`BridgeStep`] — a planned data movement between two warehouses.
//! - [`BridgeStrategy`] — how the data transfer will be performed.
//! - [`plan_bridge_steps`] — analyses a [`CrossEngineProject`] and produces
//!   the bridge steps needed for cross-warehouse edges.
//! - [`BridgeCapability`] — adapter-level capability flags for data export
//!   and import.
//!
//! Bridge steps are **explicit** — they appear in `rocky plan` output so
//! users see exactly which data movements will occur. No hidden data transfer.
//!
//! [`CrossEngineProject`]: crate::cross_engine::CrossEngineProject

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::RockyConfig;
use crate::cross_engine::CrossEngineProject;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from bridge step planning.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// The source adapter does not support data export.
    #[error(
        "adapter '{adapter}' (type '{adapter_type}') does not support data export — \
         cannot bridge from this warehouse"
    )]
    AdapterDoesNotSupportExport {
        adapter: String,
        adapter_type: String,
    },

    /// The target adapter does not support data import.
    #[error(
        "adapter '{adapter}' (type '{adapter_type}') does not support data import — \
         cannot bridge to this warehouse"
    )]
    AdapterDoesNotSupportImport {
        adapter: String,
        adapter_type: String,
    },

    /// No supported bridge strategy exists between the two adapter types.
    #[error(
        "unsupported bridge: no data transfer strategy from '{from_adapter}' to \
         '{to_adapter}' — {reason}"
    )]
    UnsupportedBridge {
        from_adapter: String,
        to_adapter: String,
        reason: String,
    },

    /// A referenced adapter is not defined in the config.
    #[error("bridge planning references unknown adapter '{adapter}' in pipeline '{pipeline}'")]
    UnknownAdapter { pipeline: String, adapter: String },
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A planned data movement between two warehouses.
///
/// Represents one cross-warehouse edge that requires explicit data transfer.
/// Bridge steps appear in `rocky plan` output with clear annotations showing
/// the source, target, and transfer strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeStep {
    /// Pipeline that produces the data (upstream).
    pub source_pipeline: String,

    /// Pipeline that consumes the data (downstream).
    pub target_pipeline: String,

    /// Adapter type on the source side (e.g., `"databricks"`).
    pub source_adapter_type: String,

    /// Adapter type on the target side (e.g., `"snowflake"`).
    pub target_adapter_type: String,

    /// How the data transfer will be performed.
    pub strategy: BridgeStrategy,

    /// Human-readable description of the bridge step, suitable for display
    /// in `rocky plan` output.
    pub description: String,
}

/// How data is transferred across warehouse boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeStrategy {
    /// Extract data via SQL query, write to intermediate files (CSV/Parquet),
    /// then load into the target warehouse via its loader adapter.
    ExtractLoad,

    /// Use the generic Arrow/CSV batch loader — read via `SELECT *` on the
    /// source, generate batched `INSERT INTO` on the target.
    ArrowBatchInsert,

    /// Both warehouses support the same cloud storage (S3, GCS, ADLS) and
    /// can use `COPY INTO` / `UNLOAD` for efficient bulk transfer.
    CloudStorageTransfer,
}

impl std::fmt::Display for BridgeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExtractLoad => write!(f, "extract-load"),
            Self::ArrowBatchInsert => write!(f, "arrow-batch-insert"),
            Self::CloudStorageTransfer => write!(f, "cloud-storage-transfer"),
        }
    }
}

/// Capability flags for an adapter type, determining which bridge strategies
/// it can participate in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeCapability {
    /// Can export data (via SQL query execution and result streaming).
    pub can_export: bool,
    /// Can import data (via loader adapter or INSERT statements).
    pub can_import: bool,
    /// Supports cloud storage operations (COPY INTO / UNLOAD).
    pub cloud_storage: bool,
}

// ---------------------------------------------------------------------------
// Capability resolution
// ---------------------------------------------------------------------------

/// Returns the bridge capabilities for a known adapter type.
///
/// Data-plane capabilities (`can_export` / `can_import`) are derived from
/// the canonical [`adapter_capability::capability_for`] table — any
/// adapter without a `supports_data` role cannot participate in bridges.
/// `cloud_storage` is orthogonal (it's about COPY INTO / UNLOAD support,
/// not trait impls) and kept as a small local enumeration.
pub fn adapter_capabilities(adapter_type: &str) -> BridgeCapability {
    let supports_data = crate::adapter_capability::capability_for(adapter_type)
        .is_some_and(|cap| cap.supports_data);
    let cloud_storage = matches!(adapter_type, "databricks" | "snowflake" | "bigquery");
    BridgeCapability {
        can_export: supports_data,
        can_import: supports_data,
        cloud_storage,
    }
}

// ---------------------------------------------------------------------------
// Bridge step planning
// ---------------------------------------------------------------------------

/// Selects the best bridge strategy for a pair of adapter types.
///
/// Strategy preference order:
/// 1. Cloud storage transfer (fastest, if both sides support it).
/// 2. Extract-load via intermediate files (medium speed, wide compatibility).
/// 3. Arrow batch insert (slowest, always works if both can query/insert).
fn select_bridge_strategy(
    source_type: &str,
    target_type: &str,
) -> Result<BridgeStrategy, BridgeError> {
    let source_caps = adapter_capabilities(source_type);
    let target_caps = adapter_capabilities(target_type);

    if !source_caps.can_export {
        return Err(BridgeError::AdapterDoesNotSupportExport {
            adapter: source_type.to_owned(),
            adapter_type: source_type.to_owned(),
        });
    }

    if !target_caps.can_import {
        return Err(BridgeError::AdapterDoesNotSupportImport {
            adapter: target_type.to_owned(),
            adapter_type: target_type.to_owned(),
        });
    }

    // Prefer cloud storage when both sides support it.
    if source_caps.cloud_storage && target_caps.cloud_storage {
        return Ok(BridgeStrategy::CloudStorageTransfer);
    }

    // Fall back to extract-load (intermediate files) when at least one
    // side supports cloud storage — the non-cloud side uses local files.
    if source_caps.cloud_storage || target_caps.cloud_storage {
        return Ok(BridgeStrategy::ExtractLoad);
    }

    // Last resort: Arrow batch insert (e.g., DuckDB → DuckDB).
    Ok(BridgeStrategy::ArrowBatchInsert)
}

/// Plans bridge steps for all cross-warehouse edges in a project.
///
/// For each [`CrossWarehouseEdge`] in the project, determines the best
/// transfer strategy and produces a [`BridgeStep`]. Returns an empty `Vec`
/// when all pipelines target the same warehouse (no bridges needed).
///
/// # Errors
///
/// Returns [`BridgeError`] when an adapter type doesn't support the required
/// export/import operations.
pub fn plan_bridge_steps(
    project: &CrossEngineProject,
    _config: &RockyConfig,
) -> Result<Vec<BridgeStep>, BridgeError> {
    let mut steps = Vec::with_capacity(project.cross_warehouse_edges.len());

    for edge in &project.cross_warehouse_edges {
        let strategy =
            select_bridge_strategy(&edge.upstream_adapter_type, &edge.downstream_adapter_type)?;

        let description = format!(
            "move data from {} ({}) to {} ({}) via {}",
            edge.source_pipeline,
            edge.upstream_adapter_type,
            edge.target_pipeline,
            edge.downstream_adapter_type,
            strategy,
        );

        steps.push(BridgeStep {
            source_pipeline: edge.source_pipeline.clone(),
            target_pipeline: edge.target_pipeline.clone(),
            source_adapter_type: edge.upstream_adapter_type.clone(),
            target_adapter_type: edge.downstream_adapter_type.clone(),
            strategy,
            description,
        });
    }

    Ok(steps)
}

/// Validates that all cross-warehouse edges in a project can be bridged.
///
/// Unlike [`plan_bridge_steps`] (which plans the actual transfers), this
/// performs a quick validation pass that surfaces all errors at once.
/// Returns a list of errors — empty means all bridges are feasible.
pub fn validate_bridge_feasibility(project: &CrossEngineProject) -> Vec<BridgeError> {
    let mut errors = Vec::new();

    for edge in &project.cross_warehouse_edges {
        if let Err(e) =
            select_bridge_strategy(&edge.upstream_adapter_type, &edge.downstream_adapter_type)
        {
            errors.push(e);
        }
    }

    errors
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_engine::CrossWarehouseEdge;

    fn edge(
        source: &str,
        target: &str,
        upstream_type: &str,
        downstream_type: &str,
    ) -> CrossWarehouseEdge {
        CrossWarehouseEdge {
            source_pipeline: source.to_owned(),
            target_pipeline: target.to_owned(),
            upstream_adapter_type: upstream_type.to_owned(),
            downstream_adapter_type: downstream_type.to_owned(),
        }
    }

    fn project(edges: Vec<CrossWarehouseEdge>) -> CrossEngineProject {
        CrossEngineProject {
            pipelines: vec![],
            cross_warehouse_edges: edges,
        }
    }

    fn empty_config() -> RockyConfig {
        RockyConfig {
            state: Default::default(),
            adapters: Default::default(),
            pipelines: Default::default(),
            hooks: Default::default(),
            cost: Default::default(),
            budget: Default::default(),
            schema_evolution: Default::default(),
            retry: None,
            portability: Default::default(),
            cache: Default::default(),
            mask: Default::default(),
            classifications: Default::default(),
        }
    }

    // -- adapter_capabilities tests --

    #[test]
    fn databricks_has_full_capabilities() {
        let caps = adapter_capabilities("databricks");
        assert!(caps.can_export);
        assert!(caps.can_import);
        assert!(caps.cloud_storage);
    }

    #[test]
    fn snowflake_has_full_capabilities() {
        let caps = adapter_capabilities("snowflake");
        assert!(caps.can_export);
        assert!(caps.can_import);
        assert!(caps.cloud_storage);
    }

    #[test]
    fn bigquery_has_full_capabilities() {
        let caps = adapter_capabilities("bigquery");
        assert!(caps.can_export);
        assert!(caps.can_import);
        assert!(caps.cloud_storage);
    }

    #[test]
    fn duckdb_has_no_cloud_storage() {
        let caps = adapter_capabilities("duckdb");
        assert!(caps.can_export);
        assert!(caps.can_import);
        assert!(!caps.cloud_storage);
    }

    #[test]
    fn fivetran_is_discovery_only() {
        let caps = adapter_capabilities("fivetran");
        assert!(!caps.can_export);
        assert!(!caps.can_import);
        assert!(!caps.cloud_storage);
    }

    #[test]
    fn unknown_adapter_has_no_capabilities() {
        let caps = adapter_capabilities("postgres");
        assert!(!caps.can_export);
        assert!(!caps.can_import);
        assert!(!caps.cloud_storage);
    }

    // -- select_bridge_strategy tests --

    #[test]
    fn cloud_warehouses_prefer_cloud_storage_transfer() {
        let strategy = select_bridge_strategy("databricks", "snowflake").unwrap();
        assert_eq!(strategy, BridgeStrategy::CloudStorageTransfer);
    }

    #[test]
    fn cloud_to_duckdb_uses_extract_load() {
        let strategy = select_bridge_strategy("databricks", "duckdb").unwrap();
        assert_eq!(strategy, BridgeStrategy::ExtractLoad);
    }

    #[test]
    fn duckdb_to_cloud_uses_extract_load() {
        let strategy = select_bridge_strategy("duckdb", "snowflake").unwrap();
        assert_eq!(strategy, BridgeStrategy::ExtractLoad);
    }

    #[test]
    fn duckdb_to_duckdb_uses_arrow_batch() {
        let strategy = select_bridge_strategy("duckdb", "duckdb").unwrap();
        assert_eq!(strategy, BridgeStrategy::ArrowBatchInsert);
    }

    #[test]
    fn fivetran_source_cannot_export() {
        let result = select_bridge_strategy("fivetran", "databricks");
        assert!(matches!(
            result,
            Err(BridgeError::AdapterDoesNotSupportExport { .. })
        ));
    }

    #[test]
    fn fivetran_target_cannot_import() {
        let result = select_bridge_strategy("databricks", "fivetran");
        assert!(matches!(
            result,
            Err(BridgeError::AdapterDoesNotSupportImport { .. })
        ));
    }

    #[test]
    fn unknown_adapter_cannot_export() {
        let result = select_bridge_strategy("unknown", "databricks");
        assert!(matches!(
            result,
            Err(BridgeError::AdapterDoesNotSupportExport { .. })
        ));
    }

    // -- plan_bridge_steps tests --

    #[test]
    fn no_edges_produces_no_steps() {
        let proj = project(vec![]);
        let steps = plan_bridge_steps(&proj, &empty_config()).unwrap();
        assert!(steps.is_empty());
    }

    #[test]
    fn single_cross_warehouse_edge_produces_one_step() {
        let proj = project(vec![edge("bronze", "silver", "databricks", "snowflake")]);
        let steps = plan_bridge_steps(&proj, &empty_config()).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].source_pipeline, "bronze");
        assert_eq!(steps[0].target_pipeline, "silver");
        assert_eq!(steps[0].strategy, BridgeStrategy::CloudStorageTransfer);
        assert!(steps[0].description.contains("bronze"));
        assert!(steps[0].description.contains("silver"));
    }

    #[test]
    fn multiple_edges_produce_multiple_steps() {
        let proj = project(vec![
            edge("ingest", "transform", "databricks", "snowflake"),
            edge("transform", "quality", "snowflake", "duckdb"),
        ]);
        let steps = plan_bridge_steps(&proj, &empty_config()).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].strategy, BridgeStrategy::CloudStorageTransfer);
        assert_eq!(steps[1].strategy, BridgeStrategy::ExtractLoad);
    }

    #[test]
    fn unsupported_bridge_returns_error() {
        let proj = project(vec![edge("ingest", "transform", "fivetran", "databricks")]);
        let result = plan_bridge_steps(&proj, &empty_config());
        assert!(result.is_err());
    }

    // -- validate_bridge_feasibility tests --

    #[test]
    fn all_valid_bridges_returns_empty() {
        let proj = project(vec![
            edge("a", "b", "databricks", "snowflake"),
            edge("b", "c", "snowflake", "duckdb"),
        ]);
        let errors = validate_bridge_feasibility(&proj);
        assert!(errors.is_empty());
    }

    #[test]
    fn invalid_bridge_collected_in_errors() {
        let proj = project(vec![
            edge("a", "b", "databricks", "snowflake"),
            edge("b", "c", "fivetran", "duckdb"),
        ]);
        let errors = validate_bridge_feasibility(&proj);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            BridgeError::AdapterDoesNotSupportExport { .. }
        ));
    }

    #[test]
    fn bridge_step_description_is_human_readable() {
        let proj = project(vec![edge(
            "raw_ingest",
            "silver_transform",
            "databricks",
            "bigquery",
        )]);
        let steps = plan_bridge_steps(&proj, &empty_config()).unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].description.contains("raw_ingest"));
        assert!(steps[0].description.contains("silver_transform"));
        assert!(steps[0].description.contains("databricks"));
        assert!(steps[0].description.contains("bigquery"));
        assert!(steps[0].description.contains("cloud-storage-transfer"));
    }

    #[test]
    fn bigquery_to_snowflake_uses_cloud_storage() {
        let strategy = select_bridge_strategy("bigquery", "snowflake").unwrap();
        assert_eq!(strategy, BridgeStrategy::CloudStorageTransfer);
    }

    #[test]
    fn duckdb_to_bigquery_uses_extract_load() {
        let strategy = select_bridge_strategy("duckdb", "bigquery").unwrap();
        assert_eq!(strategy, BridgeStrategy::ExtractLoad);
    }
}
