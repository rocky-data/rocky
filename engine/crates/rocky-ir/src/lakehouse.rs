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

    /// The target dialect cannot render the lakehouse `format = X` DDL.
    /// Raised by `generate_lakehouse_ddl` when the dialect does not
    /// advertise `supports_lakehouse_format_ddl()` — today every dialect
    /// except Databricks. This is a known limitation of the generator,
    /// not a property of the format itself (Trino does Iceberg
    /// natively); the message names both the format and the dialect.
    #[error("lakehouse format '{format}' DDL is not implemented for adapter/dialect '{dialect}'")]
    DialectUnsupported { format: String, dialect: String },

    #[error("invalid option: {0}")]
    InvalidOption(String),

    /// Managed-Iceberg `format_options` declared a combination the Databricks
    /// warehouse rejects at execution.
    ///
    /// Raised by `generate_lakehouse_ddl` (and surfaced earlier as a
    /// `rocky compile` diagnostic, before any warehouse call) when an Iceberg
    /// model sets mutually-exclusive `partition_by` + `cluster_by`, or an
    /// engine-managed `write.format.*` table property. Without this guard
    /// Databricks rejects the generated DDL at the warehouse on the first run
    /// (`SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED` /
    /// `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`). The message names the
    /// offending option.
    #[error("invalid managed-Iceberg format_options: {0}")]
    ManagedIcebergUnsupported(String),
}

// ---------------------------------------------------------------------------
// Managed-Iceberg format_options validation
// ---------------------------------------------------------------------------

/// A managed-Iceberg `format_options` constraint that Databricks rejects at
/// the warehouse.
///
/// Each carries a ready-to-render `message` (which names the offending
/// option) and an actionable `suggestion`. The compiler maps these onto
/// `E035` diagnostics; the DDL generator turns the first one into a
/// [`LakehouseError::ManagedIcebergUnsupported`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedIcebergViolation {
    /// Human-readable description that names the offending option(s).
    pub message: String,
    /// Actionable fix.
    pub suggestion: String,
}

/// Does `key` name an engine-managed Iceberg table property?
///
/// Databricks **managed Iceberg** owns the on-disk write format, so it rejects
/// any `write.format.*` table property (e.g. `write.format.default`) with
/// `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`. The reserved set is deliberately
/// narrow — only the `write.format.` family that the warehouse controls — so
/// benign properties (`delta.*`, user-defined keys, `comment`) keep passing.
fn is_engine_managed_iceberg_property(key: &str) -> bool {
    key.starts_with("write.format.")
}

/// Validate managed-Iceberg `format_options` against the constraints
/// Databricks enforces at the warehouse, returning one
/// [`ManagedIcebergViolation`] per problem (an empty vec means valid).
///
/// Only [`LakehouseFormat::IcebergTable`] is checked; every other format
/// returns no violations. The two rules:
///
/// 1. `partition_by` and `cluster_by` are **mutually exclusive** on Iceberg —
///    Databricks rejects them together with
///    `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`.
/// 2. Engine-managed `write.format.*` table properties are rejected with
///    `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED` (see
///    [`is_engine_managed_iceberg_property`]).
///
/// This is the single source of truth shared by the `rocky compile`
/// diagnostic path and the `generate_lakehouse_ddl` run-path guard, so the
/// two checks can never drift.
#[must_use]
pub fn validate_managed_iceberg_options(
    format: &LakehouseFormat,
    options: &LakehouseOptions,
) -> Vec<ManagedIcebergViolation> {
    if !matches!(format, LakehouseFormat::IcebergTable) {
        return Vec::new();
    }

    let mut violations = Vec::new();

    if !options.partition_by.is_empty() && !options.cluster_by.is_empty() {
        violations.push(ManagedIcebergViolation {
            message: format!(
                "managed-Iceberg format sets both partition_by ({}) and cluster_by ({}); \
                 Databricks Iceberg rejects PARTITIONED BY together with CLUSTER BY \
                 (they are mutually exclusive)",
                options.partition_by.join(", "),
                options.cluster_by.join(", "),
            ),
            suggestion:
                "keep either partition_by or cluster_by on an iceberg_table model, not both"
                    .to_string(),
        });
    }

    for (key, _value) in &options.table_properties {
        if is_engine_managed_iceberg_property(key) {
            violations.push(ManagedIcebergViolation {
                message: format!(
                    "managed-Iceberg format sets the engine-managed table property '{key}'; \
                     Databricks manages the Iceberg write format itself and rejects it"
                ),
                suggestion: format!(
                    "remove the '{key}' table property — Databricks managed Iceberg controls \
                     the write.format.* family"
                ),
            });
        }
    }

    violations
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_partition_and_cluster_together_is_rejected() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            cluster_by: vec!["user_id".into()],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 1, "exactly one violation expected");
        // The message must name both offending options.
        let msg = &violations[0].message;
        assert!(msg.contains("partition_by"), "names partition_by: {msg}");
        assert!(msg.contains("cluster_by"), "names cluster_by: {msg}");
        assert!(msg.contains("event_date"), "names the column: {msg}");
        assert!(msg.contains("user_id"), "names the column: {msg}");
    }

    #[test]
    fn iceberg_write_format_default_property_is_rejected() {
        let opts = LakehouseOptions {
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 1);
        assert!(
            violations[0].message.contains("write.format.default"),
            "names the offending key: {}",
            violations[0].message
        );
    }

    #[test]
    fn iceberg_partition_only_is_accepted() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            ..LakehouseOptions::default()
        };
        assert!(validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty());
    }

    #[test]
    fn iceberg_cluster_only_is_accepted() {
        let opts = LakehouseOptions {
            cluster_by: vec!["user_id".into()],
            ..LakehouseOptions::default()
        };
        assert!(validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty());
    }

    #[test]
    fn iceberg_benign_properties_are_accepted() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            table_properties: vec![
                ("delta.enableChangeDataFeed".into(), "true".into()),
                ("team".into(), "growth".into()),
            ],
            comment: Some("benign".into()),
            ..LakehouseOptions::default()
        };
        assert!(
            validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty(),
            "benign delta.*/user properties must not be flagged"
        );
    }

    #[test]
    fn non_iceberg_formats_are_never_flagged() {
        // The same partition+cluster+write.format combo on Delta is fine —
        // the managed-Iceberg constraints apply only to Iceberg.
        let opts = LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..LakehouseOptions::default()
        };
        for format in [
            LakehouseFormat::DeltaTable,
            LakehouseFormat::Table,
            LakehouseFormat::StreamingTable,
            LakehouseFormat::MaterializedView,
            LakehouseFormat::View,
        ] {
            assert!(
                validate_managed_iceberg_options(&format, &opts).is_empty(),
                "format {format} must not be subject to managed-Iceberg rules"
            );
        }
    }

    #[test]
    fn iceberg_both_violations_reported() {
        let opts = LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            table_properties: vec![("write.format.version".into(), "2".into())],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 2, "both rules fire independently");
    }
}
