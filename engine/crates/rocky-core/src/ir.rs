//! §P4.2: identifier-list fields on IR structs — `unique_key`,
//! `partition_by`, `ColumnSelection::Explicit`, `columns_to_drop` — use
//! `Vec<Arc<str>>` so cloning a plan / drift result is refcount-cheap.
//! JSON wire format is preserved by serde's `rc` feature.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rocky_sql::validation::{self, ValidationError};
use serde::{Deserialize, Serialize};

use crate::lakehouse::{LakehouseFormat, LakehouseOptions};
use crate::models::TimeGrain;

/// A transformation plan that compiles to warehouse-specific SQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Plan {
    Replication(ReplicationPlan),
    Transformation(TransformationPlan),
    Snapshot(SnapshotPlan),
}

/// Silver layer: custom SQL transformations with multiple sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationPlan {
    /// Multiple input tables (for joins).
    pub sources: Vec<SourceRef>,
    pub target: TargetRef,
    pub strategy: MaterializationStrategy,
    /// User-written SQL (loaded from file or inline).
    pub sql: String,
    pub governance: GovernanceConfig,
    /// Optional lakehouse table format (Delta, Iceberg, etc.).
    /// When set, SQL generation uses format-specific DDL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<LakehouseFormat>,
    /// Format-specific options (partitioning, clustering, properties).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_options: Option<LakehouseOptions>,
}

/// Raw layer: 1:1 copy with incremental watermark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPlan {
    pub source: SourceRef,
    pub target: TargetRef,
    pub strategy: MaterializationStrategy,
    pub columns: ColumnSelection,
    pub metadata_columns: Vec<MetadataColumn>,
    pub governance: GovernanceConfig,
}

/// SCD Type 2 snapshot: tracks historical changes via valid_from / valid_to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPlan {
    pub source: SourceRef,
    pub target: TargetRef,
    /// Columns that uniquely identify a row.
    pub unique_key: Vec<Arc<str>>,
    /// Column used to detect changes.
    pub updated_at: String,
    /// When true, invalidate rows deleted from source.
    pub invalidate_hard_deletes: bool,
    pub governance: GovernanceConfig,
}

/// How to materialize the target table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy")]
pub enum MaterializationStrategy {
    /// Drop and recreate the entire table.
    FullRefresh,
    /// Append rows newer than the watermark.
    Incremental {
        timestamp_column: String,
        watermark: Option<WatermarkState>,
    },
    /// Upsert based on unique key columns.
    Merge {
        unique_key: Vec<Arc<str>>,
        update_columns: ColumnSelection,
    },
    /// Databricks Materialized View — warehouse manages refresh.
    MaterializedView,
    /// Snowflake Dynamic Table — warehouse manages lag-based refresh.
    DynamicTable {
        /// Target lag specification (e.g., "1 hour", "downstream").
        target_lag: String,
    },
    /// Partition-keyed materialization (resolved from `StrategyConfig::TimeInterval`).
    ///
    /// Each plan instance targets exactly one partition. The runtime populates
    /// `window` per partition before SQL generation; static planning leaves it `None`.
    TimeInterval {
        /// Output column that holds the partition value.
        time_column: String,
        /// Partition granularity.
        granularity: TimeGrain,
        /// The specific partition window being computed by this plan instance.
        /// `Some(...)` when invoked by the runtime; `None` during static planning.
        window: Option<PartitionWindow>,
    },
    /// Ephemeral model — not materialized to any table. Inlined as a CTE
    /// in downstream consumers. Useful for lightweight transformations
    /// that don't need persistence.
    Ephemeral,
    /// Delete matching rows by partition key, then insert fresh data.
    /// Common dbt pattern for partition-based incremental loads where
    /// MERGE overhead is unnecessary.
    DeleteInsert {
        /// Column(s) used to identify the partition to delete before inserting.
        partition_by: Vec<Arc<str>>,
    },
    /// Alias for `TimeInterval` with sensible defaults. Processes data
    /// in micro-batches based on a timestamp column. dbt-compatible naming.
    Microbatch {
        /// Timestamp column that defines micro-batch boundaries.
        timestamp_column: String,
        /// Batch granularity (default: Hour).
        granularity: TimeGrain,
    },
}

/// A single partition's time window, used to substitute `@start_date` /
/// `@end_date` placeholders in the model SQL and to build the WHERE filter
/// for `INSERT OVERWRITE PARTITION`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionWindow {
    /// Canonical partition key (see `TimeGrain::format_str`), e.g. `"2026-04-07"`.
    pub key: String,
    /// `@start_date` placeholder value (inclusive).
    pub start: DateTime<Utc>,
    /// `@end_date` placeholder value (exclusive).
    pub end: DateTime<Utc>,
}

/// Reference to a source table (three-part name).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl SourceRef {
    /// Returns the fully-qualified three-part name (`catalog.schema.table`).
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }

    /// Returns the fully-qualified name after validating each component is a safe SQL identifier.
    ///
    /// Use this instead of `full_name()` when the result will be interpolated into SQL.
    pub fn validated_full_name(&self) -> Result<String, ValidationError> {
        validation::format_table_ref(&self.catalog, &self.schema, &self.table)
    }
}

/// Reference to a target table (templates resolved at plan time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetRef {
    /// Template like `{client}_warehouse`, resolved with parsed schema components.
    pub catalog: String,
    /// Template like `raw__{regions}__{connector}`, resolved with parsed schema components.
    pub schema: String,
    pub table: String,
}

impl TargetRef {
    /// Returns the fully-qualified three-part name (`catalog.schema.table`).
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }

    /// Returns the fully-qualified name after validating each component is a safe SQL identifier.
    ///
    /// Use this instead of `full_name()` when the result will be interpolated into SQL.
    pub fn validated_full_name(&self) -> Result<String, ValidationError> {
        validation::format_table_ref(&self.catalog, &self.schema, &self.table)
    }
}

/// Which columns to select.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnSelection {
    /// `SELECT *`
    All,
    /// `SELECT col1, col2, ...`
    Explicit(Vec<Arc<str>>),
}

/// Extra columns to add during replication (e.g., `CAST(NULL AS STRING) AS _loaded_by`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: String,
    pub value: String,
}

/// Governance configuration for catalog/schema lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernanceConfig {
    pub permissions_file: Option<String>,
    pub auto_create_catalogs: bool,
    pub auto_create_schemas: bool,
}

/// Tracks the last-seen watermark for incremental loads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    /// The MAX(timestamp_column) from the last successful run.
    pub last_value: DateTime<Utc>,
    /// When this watermark was recorded.
    pub updated_at: DateTime<Utc>,
}

/// Column metadata from DESCRIBE TABLE.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Three-part table reference.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TableRef {
    /// Returns the fully-qualified three-part name (`catalog.schema.table`).
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }

    /// Returns the fully-qualified name after validating each component is a safe SQL identifier.
    ///
    /// Use this instead of `full_name()` when the result will be interpolated into SQL.
    pub fn validated_full_name(&self) -> Result<String, ValidationError> {
        validation::format_table_ref(&self.catalog, &self.schema, &self.table)
    }

    /// Returns a key suitable for state store lookups.
    pub fn state_key(&self) -> String {
        self.full_name()
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Result of drift detection between source and target columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftResult {
    pub table: TableRef,
    pub drifted_columns: Vec<DriftedColumn>,
    pub action: DriftAction,
    /// Columns in grace period (present in target, absent from source, not yet expired).
    #[serde(default)]
    pub grace_period_columns: Vec<GracePeriodColumn>,
    /// Columns whose grace period has expired and should be dropped from the target.
    #[serde(default)]
    pub columns_to_drop: Vec<Arc<str>>,
}

/// A single column that has drifted between source and target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftedColumn {
    pub name: String,
    pub source_type: String,
    pub target_type: String,
}

/// A column that exists in the target but has been dropped from the source
/// and is being kept during a grace period. After the grace period expires
/// the column is removed from the target table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GracePeriodColumn {
    /// Column name.
    pub name: String,
    /// Column data type in the target.
    pub data_type: String,
    /// When the column was first detected as missing from the source.
    pub first_seen_at: DateTime<Utc>,
    /// When the grace period expires.
    pub expires_at: DateTime<Utc>,
    /// Days remaining until the column is dropped.
    pub days_remaining: u32,
}

/// What to do when drift is detected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DriftAction {
    /// Drop target table and do a full refresh.
    DropAndRecreate,
    /// ALTER TABLE to change column types (future).
    AlterColumnTypes,
    /// Log a warning but take no action.
    Ignore,
}

/// A permission grant on a catalog or schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub principal: String,
    pub permission: Permission,
    pub target: GrantTarget,
}

/// Databricks permissions that Rocky manages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Permission {
    Browse,
    UseCatalog,
    UseSchema,
    Select,
    Modify,
    Manage,
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::Browse => write!(f, "BROWSE"),
            Permission::UseCatalog => write!(f, "USE CATALOG"),
            Permission::UseSchema => write!(f, "USE SCHEMA"),
            Permission::Select => write!(f, "SELECT"),
            Permission::Modify => write!(f, "MODIFY"),
            Permission::Manage => write!(f, "MANAGE"),
        }
    }
}

/// Target of a GRANT/REVOKE statement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GrantTarget {
    Catalog(String),
    Schema { catalog: String, schema: String },
}

/// The diff between desired and current permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionDiff {
    pub grants_to_add: Vec<Grant>,
    pub grants_to_revoke: Vec<Grant>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_plan_serialization() {
        let plan = ReplicationPlan {
            source: SourceRef {
                catalog: "source_catalog".into(),
                schema: "src__acme__us_west__shopify".into(),
                table: "orders".into(),
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "staging__us_west__shopify".into(),
                table: "orders".into(),
            },
            strategy: MaterializationStrategy::Incremental {
                timestamp_column: "_fivetran_synced".into(),
                watermark: None,
            },
            columns: ColumnSelection::All,
            metadata_columns: vec![MetadataColumn {
                name: "_loaded_by".into(),
                data_type: "STRING".into(),
                value: "NULL".into(),
            }],
            governance: GovernanceConfig {
                permissions_file: Some("config/catalog_permissions.yaml".into()),
                auto_create_catalogs: true,
                auto_create_schemas: true,
            },
        };

        let json = serde_json::to_string_pretty(&plan).unwrap();
        let deserialized: ReplicationPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.source.catalog, "source_catalog");
        assert_eq!(deserialized.target.table, "orders");
    }

    #[test]
    fn test_plan_enum_serialization() {
        let plan = Plan::Replication(ReplicationPlan {
            source: SourceRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl".into(),
            },
            target: TargetRef {
                catalog: "tcat".into(),
                schema: "tsch".into(),
                table: "tbl".into(),
            },
            strategy: MaterializationStrategy::FullRefresh,
            columns: ColumnSelection::All,
            metadata_columns: vec![],
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        });

        let json = serde_json::to_string(&plan).unwrap();
        let deserialized: Plan = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, Plan::Replication(_)));
    }

    #[test]
    fn test_table_ref_display() {
        let r = TableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        };
        assert_eq!(r.to_string(), "cat.sch.tbl");
        assert_eq!(r.state_key(), "cat.sch.tbl");
    }

    #[test]
    fn test_validated_full_name_accepts_valid() {
        let t = TargetRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west".into(),
            table: "orders".into(),
        };
        assert_eq!(
            t.validated_full_name().unwrap(),
            "acme_warehouse.staging__us_west.orders"
        );

        let s = SourceRef {
            catalog: "src_catalog".into(),
            schema: "raw_schema".into(),
            table: "users".into(),
        };
        assert_eq!(
            s.validated_full_name().unwrap(),
            "src_catalog.raw_schema.users"
        );

        let r = TableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        };
        assert_eq!(r.validated_full_name().unwrap(), "cat.sch.tbl");
    }

    #[test]
    fn test_validated_full_name_rejects_injection() {
        let bad = TargetRef {
            catalog: "good".into(),
            schema: "also_good".into(),
            table: "bad; DROP TABLE".into(),
        };
        assert!(bad.validated_full_name().is_err());

        let bad_table = TableRef {
            catalog: "cat".into(),
            schema: "sch WITH SPACES".into(),
            table: "tbl".into(),
        };
        assert!(bad_table.validated_full_name().is_err());
    }

    #[test]
    fn test_permission_display() {
        assert_eq!(Permission::Browse.to_string(), "BROWSE");
        assert_eq!(Permission::UseCatalog.to_string(), "USE CATALOG");
        assert_eq!(Permission::UseSchema.to_string(), "USE SCHEMA");
        assert_eq!(Permission::Select.to_string(), "SELECT");
        assert_eq!(Permission::Modify.to_string(), "MODIFY");
        assert_eq!(Permission::Manage.to_string(), "MANAGE");
    }

    #[test]
    fn test_watermark_serialization() {
        let wm = WatermarkState {
            last_value: Utc::now(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&wm).unwrap();
        let deserialized: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(wm.last_value, deserialized.last_value);
    }

    #[test]
    fn test_materialization_strategies() {
        let strategies = vec![
            MaterializationStrategy::FullRefresh,
            MaterializationStrategy::Incremental {
                timestamp_column: "_fivetran_synced".into(),
                watermark: None,
            },
            MaterializationStrategy::Merge {
                unique_key: vec!["id".into()],
                update_columns: ColumnSelection::All,
            },
            MaterializationStrategy::MaterializedView,
            MaterializationStrategy::DynamicTable {
                target_lag: "1 hour".into(),
            },
            MaterializationStrategy::TimeInterval {
                time_column: "order_date".into(),
                granularity: TimeGrain::Day,
                window: None,
            },
            MaterializationStrategy::Ephemeral,
            MaterializationStrategy::DeleteInsert {
                partition_by: vec!["date_key".into()],
            },
            MaterializationStrategy::Microbatch {
                timestamp_column: "event_time".into(),
                granularity: TimeGrain::Hour,
            },
        ];

        for strategy in &strategies {
            let json = serde_json::to_string(strategy).unwrap();
            let _: MaterializationStrategy = serde_json::from_str(&json).unwrap();
        }
    }
}
