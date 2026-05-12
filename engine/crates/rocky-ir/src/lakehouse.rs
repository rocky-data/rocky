//! Lakehouse-native materialization types.
//!
//! Pure data: format enum, options struct, and the error type the DDL
//! generators raise. The DDL generators themselves live in
//! `rocky_core::lakehouse` because they consume `SqlDialect`, which is
//! the dialect-trait surface in `rocky-core::traits`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use rocky_sql::validation;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during lakehouse DDL generation.
#[derive(Debug, Error)]
pub enum LakehouseError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("unsupported format for dialect: {0}")]
    UnsupportedFormat(String),

    #[error("invalid option: {0}")]
    InvalidOption(String),
}

// ---------------------------------------------------------------------------
// LakehouseFormat
// ---------------------------------------------------------------------------

/// The physical format used to materialize a model in the lakehouse.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LakehouseFormat {
    /// Delta Lake table (`CREATE TABLE ... USING DELTA`).
    DeltaTable,
    /// Apache Iceberg table (`CREATE TABLE ... USING ICEBERG`).
    IcebergTable,
    /// Warehouse-managed materialized view (`CREATE MATERIALIZED VIEW`).
    MaterializedView,
    /// Databricks-specific streaming table (`CREATE STREAMING TABLE`).
    StreamingTable,
    /// Plain managed table (default warehouse format).
    #[default]
    Table,
    /// SQL view (no physical storage).
    View,
}

impl std::fmt::Display for LakehouseFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeltaTable => write!(f, "delta_table"),
            Self::IcebergTable => write!(f, "iceberg_table"),
            Self::MaterializedView => write!(f, "materialized_view"),
            Self::StreamingTable => write!(f, "streaming_table"),
            Self::Table => write!(f, "table"),
            Self::View => write!(f, "view"),
        }
    }
}

// ---------------------------------------------------------------------------
// LakehouseOptions
// ---------------------------------------------------------------------------

/// Format-specific DDL options for lakehouse materializations.
///
/// Not all options apply to every format — the `rocky_core::lakehouse`
/// DDL generator silently ignores options that are irrelevant for the
/// chosen format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LakehouseOptions {
    /// `PARTITIONED BY (col1, col2)` — Delta and Iceberg tables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,

    /// `CLUSTER BY (col1, col2)` — Databricks liquid clustering (Delta)
    /// or Iceberg sorted clustering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_by: Vec<String>,

    /// Arbitrary key-value table properties.
    /// Rendered as `TBLPROPERTIES ('key' = 'value', ...)` for Delta/Iceberg.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_properties: Vec<(String, String)>,

    /// Optional comment on the table/view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}
