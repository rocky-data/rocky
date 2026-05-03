//! §P4.2: identifier-list fields on IR structs — `unique_key`,
//! `partition_by`, `ColumnSelection::Explicit`, `columns_to_drop` — use
//! `Vec<Arc<str>>` so cloning a plan / drift result is refcount-cheap.
//! JSON wire format is preserved by serde's `rc` feature.
//!
//! ## Canonical-JSON convention for [`ModelIr`] / [`ProjectIr`]
//!
//! Recipe-hash determinism requires a single, predictable serialization
//! shape. The rule has three legs:
//!
//! - Every `Option<T>` field carries
//!   `#[serde(default, skip_serializing_if = "Option::is_none")]`. `None`
//!   values are absent from the JSON; the recipe-hash never sees an explicit
//!   `null`.
//! - Every `Vec<T>` field that is "conceptually optional" (empty == absent —
//!   e.g. `metadata_columns`, `unique_key`, `sources`, `column_masks`)
//!   carries `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.
//! - Every `bool` field whose semantic default is `false` (e.g.
//!   `invalidate_hard_deletes`) carries
//!   `#[serde(default, skip_serializing_if = "std::ops::Not::not")]` so the
//!   key is omitted when the value is the default.
//!
//! Add new fields with the matching attribute pair so the rule stays uniform.

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
    ///
    /// The watermark value itself is not carried on the strategy: it is
    /// runtime state read from the embedded state store (see
    /// [`crate::state::StateStore::get_watermark`]) and the SQL generator
    /// emits a `WHERE ts > (SELECT MAX(ts) FROM target)` subquery that
    /// resolves the bound at execution time. Keeping the field off the
    /// strategy means recipe-hash inputs are runtime-state-free.
    Incremental { timestamp_column: String },
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
    /// Columns present in the source but absent from the target — the
    /// target needs an `ALTER TABLE ADD COLUMN` before the next
    /// incremental INSERT can succeed (otherwise BigQuery rejects with
    /// `Inserted row has wrong column count`, and Snowflake/Databricks
    /// have analogous failures). Populated by [`detect_drift`].
    #[serde(default)]
    pub added_columns: Vec<ColumnInfo>,
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
///
/// `Ord` / `PartialOrd` are derived so that callers which need a
/// deterministic enumeration (e.g. [`crate::role_graph::flatten_role_graph`]
/// collecting permissions into a `BTreeSet`) get a stable ordering keyed by
/// the declaration order below, independent of input traversal order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

impl std::str::FromStr for Permission {
    type Err = UnknownPermission;

    /// Parse one of the managed Rocky permissions from its canonical
    /// uppercase spelling (`"SELECT"`, `"USE CATALOG"`, ...).
    ///
    /// Unknown strings return [`UnknownPermission`] so callers can surface
    /// a typed error (e.g. [`crate::role_graph::RoleGraphError::UnknownPermission`])
    /// rather than silently skipping the entry.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BROWSE" => Ok(Permission::Browse),
            "USE CATALOG" => Ok(Permission::UseCatalog),
            "USE SCHEMA" => Ok(Permission::UseSchema),
            "SELECT" => Ok(Permission::Select),
            "MODIFY" => Ok(Permission::Modify),
            "MANAGE" => Ok(Permission::Manage),
            other => Err(UnknownPermission(other.to_string())),
        }
    }
}

/// Error returned by [`Permission`]'s `FromStr` impl when the input string
/// isn't one of the managed permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownPermission(pub String);

impl std::fmt::Display for UnknownPermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown permission: {}", self.0)
    }
}

impl std::error::Error for UnknownPermission {}

/// Target of a GRANT/REVOKE statement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GrantTarget {
    Catalog(String),
    Schema { catalog: String, schema: String },
}

/// The diff between desired and current permissions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionDiff {
    pub grants_to_add: Vec<Grant>,
    pub grants_to_revoke: Vec<Grant>,
}

/// A role with its fully flattened permission set, after walking the
/// `inherits` DAG and deduplicating.
///
/// Produced by [`crate::role_graph::flatten_role_graph`] and consumed by
/// [`crate::traits::GovernanceAdapter::reconcile_role_graph`]. The
/// `flattened_permissions` list is deterministically sorted via
/// [`Permission`]'s `Ord` impl so adapter-level SQL generation is stable
/// across runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedRole {
    /// Role name, matching the key in the owning `BTreeMap`.
    pub name: String,
    /// Union of the role's own permissions and every ancestor's
    /// permissions, deduplicated and sorted.
    pub flattened_permissions: Vec<Permission>,
    /// Immediate parents declared via the role's `inherits` list,
    /// preserved verbatim for audit/debug reporting — **not** the full
    /// transitive closure.
    pub inherits_from: Vec<String>,
}

/// A single resolved column-masking instruction.
///
/// Policy lives in [`crate::config::RockyConfig`] (workspace-default
/// `[mask]` block plus optional per-env `[mask.<env>]` overrides). The
/// resolution-as-applied — column name plus the strategy chosen for the
/// active environment — is captured here so the recipe-hash reflects what
/// the warehouse actually sees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMask {
    /// Column name, interned for cheap cloning.
    pub column: Arc<str>,
    /// Resolved strategy for the active environment.
    pub strategy: crate::traits::MaskStrategy,
}

/// Per-model intermediate representation.
///
/// Carries everything Rocky needs to know about a single model to generate
/// SQL, run governance, and compute a recipe-hash: the SQL itself, the
/// typed output columns, the lineage edges that target this model, the
/// materialization strategy, governance metadata, the resolved
/// column-masking plan for the active environment, and the source / target
/// table refs plus variant-specific metadata (column selection, metadata
/// columns, snapshot key/timestamp, lakehouse format) needed to losslessly
/// represent any [`Plan`] variant.
///
/// `ModelIr` is an internal contract. It does not derive `JsonSchema` and
/// is not part of the public CLI output schema; consumers outside the
/// engine should depend on the typed `*Output` structs in
/// `rocky-cli::output` instead.
///
/// ## Flat-fields design
///
/// Variant-specific fields ([`Self::source`], [`Self::columns`],
/// [`Self::unique_key`], ...) live as flat optional / empty-default fields
/// on [`ModelIr`] rather than inside a nested `kind: ModelKind` enum. The
/// canonical-JSON convention (skip `None`, skip empty `Vec`, skip default
/// `bool`) keeps the per-variant JSON shape compact, and a single struct
/// makes the recipe-hash easier to reason about — there is no enum
/// discriminant to canonicalise. Compile-time variant exhaustiveness is
/// retained via the still-present [`Plan`] enum, which converts to /
/// from [`ModelIr`] losslessly.
///
/// All optional fields follow the canonical-JSON rule documented at the
/// top of this module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelIr {
    /// Stable identifier for this model (project-unique).
    pub name: Arc<str>,
    /// User-written SQL for this model. The SQL string is the load-bearing
    /// recipe-hash input: text changes (even comments) hash to a different
    /// value.
    pub sql: String,
    /// Inferred output columns. May be empty when typecheck partial-failed
    /// or the model uses `SELECT *` against an upstream that hasn't been
    /// typechecked yet.
    pub typed_columns: Vec<crate::types::TypedColumn>,
    /// Column-level lineage edges whose target is this model. Cross-model
    /// edges that originate elsewhere appear in the consumer's slice.
    pub lineage_edges: Vec<crate::lineage::LineageEdge>,
    /// How the warehouse should materialize this model.
    ///
    /// Recipe-hash invariant: when this strategy is
    /// [`MaterializationStrategy::TimeInterval`], the `window` field MUST
    /// be `None` at hash time. The static plan emits `None`; the runtime
    /// fills `Some(...)` per partition. Hashing the resolved partition
    /// would yield a different recipe-hash for every partition of the same
    /// model, defeating content-addressed write semantics.
    pub materialization: MaterializationStrategy,
    /// Governance metadata (catalog/schema lifecycle policy).
    pub governance: GovernanceConfig,
    /// Target table reference. Required on every [`Plan`] variant.
    pub target: TargetRef,
    /// Resolved column masks for the active environment. Populated at IR
    /// construction by reading
    /// [`crate::config::RockyConfig::resolve_mask_for_env`]. Empty when no
    /// classifications matched or every resolved strategy was
    /// [`crate::traits::MaskStrategy::None`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_masks: Vec<ColumnMask>,
    /// Single-source ref. `Some` for [`Plan::Replication`] and
    /// [`Plan::Snapshot`]; `None` for [`Plan::Transformation`] (which uses
    /// [`Self::sources`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
    /// Multi-source refs (joins). Populated for [`Plan::Transformation`];
    /// empty for [`Plan::Replication`] and [`Plan::Snapshot`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceRef>,
    /// Column selection. `Some` for [`Plan::Replication`]; `None` for
    /// other variants (which select via the `sql` field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<ColumnSelection>,
    /// Extra columns added during replication (e.g.
    /// `CAST(NULL AS STRING) AS _loaded_by`). Populated for
    /// [`Plan::Replication`]; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_columns: Vec<MetadataColumn>,
    /// Columns that uniquely identify a snapshot row. Populated for
    /// [`Plan::Snapshot`]; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<Arc<str>>,
    /// Column used to detect changes for SCD2 snapshots. `Some` for
    /// [`Plan::Snapshot`]; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// When true, invalidate rows deleted from source. Only meaningful
    /// for [`Plan::Snapshot`]; `false` otherwise.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub invalidate_hard_deletes: bool,
    /// Optional lakehouse table format (Delta, Iceberg, ...). Lifted from
    /// [`TransformationPlan::format`]; `None` for non-transformation plans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<LakehouseFormat>,
    /// Format-specific options (partitioning, clustering, properties).
    /// Lifted from [`TransformationPlan::format_options`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_options: Option<LakehouseOptions>,
}

impl ModelIr {
    /// Compute the recipe-hash of this model.
    ///
    /// The hash is `blake3` over a canonical JSON encoding of `self`: keys
    /// sorted, no insignificant whitespace, `Option::None` skipped per the
    /// canonical-JSON rule documented at the top of this module.
    ///
    /// Determinism contract: given two byte-identical [`ModelIr`] values
    /// the returned hash is byte-identical. Mutating any input field
    /// (SQL, typed columns, lineage edges, materialization, governance,
    /// resolved masks, source/target refs, snapshot key/timestamp,
    /// lakehouse format) changes the hash.
    ///
    /// # Panics
    ///
    /// Panics if `self` cannot be serialized to JSON. All fields on
    /// [`ModelIr`] derive `Serialize` over infallible primitives, so this
    /// only fires on a programming error (e.g. a `Map` with non-string
    /// keys introduced after this comment was written).
    pub fn recipe_hash(&self) -> blake3::Hash {
        let canonical = canonical_json(self);
        blake3::hash(canonical.as_bytes())
    }

    /// Reconstruct a [`Plan`] from this `ModelIr`, mirroring the
    /// `From<&Plan> for ModelIr` conversion.
    ///
    /// The variant is inferred from which variant-specific fields are
    /// populated:
    ///
    /// - `unique_key` non-empty AND `updated_at` `Some` → [`Plan::Snapshot`]
    /// - `columns` `Some` (replication carries an explicit
    ///   [`ColumnSelection`]) → [`Plan::Replication`]
    /// - otherwise → [`Plan::Transformation`]
    ///
    /// Round-trip property: `ModelIr::from(&plan).to_plan_compatible()` is
    /// canonical-JSON-equal to `plan` for any well-formed input. Used by
    /// the test suite to verify the conversion is lossless.
    ///
    /// # Panics
    ///
    /// Panics if a [`ModelIr`] inferred to be a Snapshot is missing
    /// `source` or if a Replication ModelIr is missing `source`. These
    /// fields are required on the corresponding [`Plan`] variant; the
    /// `From<&Plan>` impl always populates them, so this only fires on a
    /// hand-built `ModelIr` that violates the variant contract.
    pub fn to_plan_compatible(&self) -> Plan {
        // Snapshot has both unique_key and updated_at; replication has columns.
        if !self.unique_key.is_empty() && self.updated_at.is_some() {
            let source = self
                .source
                .clone()
                .expect("Snapshot ModelIr must carry `source`");
            let updated_at = self
                .updated_at
                .clone()
                .expect("Snapshot ModelIr must carry `updated_at`");
            Plan::Snapshot(SnapshotPlan {
                source,
                target: self.target.clone(),
                unique_key: self.unique_key.clone(),
                updated_at,
                invalidate_hard_deletes: self.invalidate_hard_deletes,
                governance: self.governance.clone(),
            })
        } else if let Some(columns) = self.columns.clone() {
            let source = self
                .source
                .clone()
                .expect("Replication ModelIr must carry `source`");
            Plan::Replication(ReplicationPlan {
                source,
                target: self.target.clone(),
                strategy: self.materialization.clone(),
                columns,
                metadata_columns: self.metadata_columns.clone(),
                governance: self.governance.clone(),
            })
        } else {
            Plan::Transformation(TransformationPlan {
                sources: self.sources.clone(),
                target: self.target.clone(),
                strategy: self.materialization.clone(),
                sql: self.sql.clone(),
                governance: self.governance.clone(),
                format: self.format.clone(),
                format_options: self.format_options.clone(),
            })
        }
    }
}

impl From<&Plan> for ModelIr {
    /// Lossless structural conversion from a [`Plan`] to a [`ModelIr`].
    ///
    /// Variant-specific fields are mapped onto the flat [`ModelIr`] shape:
    ///
    /// - [`Plan::Replication`] populates `source`, `columns`,
    ///   `metadata_columns`.
    /// - [`Plan::Transformation`] populates `sources`, `format`,
    ///   `format_options`.
    /// - [`Plan::Snapshot`] populates `source`, `unique_key`, `updated_at`,
    ///   `invalidate_hard_deletes`.
    ///
    /// `name`, `typed_columns`, `lineage_edges`, and `column_masks` are not
    /// carried by [`Plan`]; they default to the table's own name and empty
    /// vectors. Callers that have richer typed-column or lineage data
    /// should populate those fields after construction.
    fn from(plan: &Plan) -> Self {
        match plan {
            Plan::Replication(p) => ModelIr {
                name: Arc::from(p.target.table.as_str()),
                sql: String::new(),
                typed_columns: Vec::new(),
                lineage_edges: Vec::new(),
                materialization: p.strategy.clone(),
                governance: p.governance.clone(),
                target: p.target.clone(),
                column_masks: Vec::new(),
                source: Some(p.source.clone()),
                sources: Vec::new(),
                columns: Some(p.columns.clone()),
                metadata_columns: p.metadata_columns.clone(),
                unique_key: Vec::new(),
                updated_at: None,
                invalidate_hard_deletes: false,
                format: None,
                format_options: None,
            },
            Plan::Transformation(p) => ModelIr {
                name: Arc::from(p.target.table.as_str()),
                sql: p.sql.clone(),
                typed_columns: Vec::new(),
                lineage_edges: Vec::new(),
                materialization: p.strategy.clone(),
                governance: p.governance.clone(),
                target: p.target.clone(),
                column_masks: Vec::new(),
                source: None,
                sources: p.sources.clone(),
                columns: None,
                metadata_columns: Vec::new(),
                unique_key: Vec::new(),
                updated_at: None,
                invalidate_hard_deletes: false,
                format: p.format.clone(),
                format_options: p.format_options.clone(),
            },
            Plan::Snapshot(p) => ModelIr {
                name: Arc::from(p.target.table.as_str()),
                sql: String::new(),
                typed_columns: Vec::new(),
                lineage_edges: Vec::new(),
                // Snapshots don't have a MaterializationStrategy on the
                // SnapshotPlan; the SCD2 logic is implicit in
                // `sql_gen::generate_snapshot_sql`. FullRefresh is the
                // closest neutral default and matches how snapshot SQL is
                // generated today.
                materialization: MaterializationStrategy::FullRefresh,
                governance: p.governance.clone(),
                target: p.target.clone(),
                column_masks: Vec::new(),
                source: Some(p.source.clone()),
                sources: Vec::new(),
                columns: None,
                metadata_columns: Vec::new(),
                unique_key: p.unique_key.clone(),
                updated_at: Some(p.updated_at.clone()),
                invalidate_hard_deletes: p.invalidate_hard_deletes,
                format: None,
                format_options: None,
            },
        }
    }
}

/// Project-level intermediate representation.
///
/// Thin wrapper that pairs the per-model IRs with the project's DAG and
/// the cross-model lineage edges. Project-level recipe-hash is *derived*
/// from the per-model hashes; it is not stored on this struct.
///
/// `ProjectIr` is an internal contract and does not derive `JsonSchema`
/// (see [`ModelIr`] for the rationale).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectIr {
    /// All models in this project. Ordering is caller-defined; the
    /// project-level recipe-hash sorts by per-model hash before combining.
    pub models: Vec<ModelIr>,
    /// DAG nodes describing model-to-model dependencies. Use
    /// [`crate::dag::topological_sort`] or
    /// [`crate::dag::execution_layers`] to derive an execution order.
    pub dag: Vec<crate::dag::DagNode>,
    /// Full cross-model column-level lineage. Each [`ModelIr`] also carries
    /// the slice of edges that target it; this field is the union and is
    /// the canonical store.
    pub lineage_edges: Vec<crate::lineage::LineageEdge>,
}

impl ProjectIr {
    /// Compute the project-level recipe-hash.
    ///
    /// Each model contributes its [`ModelIr::recipe_hash`]; the per-model
    /// hashes are sorted lexicographically by their hex representation and
    /// hashed together so the result is independent of `models` ordering.
    ///
    /// Project-level fields ([`Self::dag`], [`Self::lineage_edges`]) are
    /// not folded into the project-level hash — they are derived facts
    /// about how the per-model recipes relate, not part of any single
    /// model's recipe. Changes to the DAG or cross-model lineage that do
    /// not change a model's own recipe leave the per-model hashes (and
    /// therefore the project-level hash) untouched.
    pub fn recipe_hash(&self) -> blake3::Hash {
        let mut per_model: Vec<String> = self
            .models
            .iter()
            .map(|m| m.recipe_hash().to_hex().to_string())
            .collect();
        per_model.sort();
        let mut hasher = blake3::Hasher::new();
        for h in &per_model {
            hasher.update(h.as_bytes());
            // Length-prefix the separator so concatenation is unambiguous.
            hasher.update(b"\n");
        }
        hasher.finalize()
    }
}

/// Canonical-JSON encoder used by [`ModelIr::recipe_hash`].
///
/// `serde_json` preserves field insertion order on `Map`, which is not
/// stable enough for content-addressed hashing across different inputs of
/// the same logical value. This helper round-trips through a
/// [`serde_json::Value`] and rewrites every nested map into a
/// [`std::collections::BTreeMap`] (key-sorted). The resulting string has
/// no insignificant whitespace and skipped-on-`None` fields stay absent
/// because they were already absent at serialize time.
fn canonical_json<T: Serialize>(value: &T) -> String {
    let raw = serde_json::to_value(value)
        .expect("recipe_hash inputs must be JSON-encodable; programming error");
    let canonical = canonicalize(raw);
    serde_json::to_string(&canonical)
        .expect("BTreeMap-based serde_json::Value is always serializable")
}

fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: std::collections::BTreeMap<String, serde_json::Value> =
                map.into_iter().map(|(k, v)| (k, canonicalize(v))).collect();
            // serde_json serializes BTreeMap keys in sorted order; round-trip
            // back through Value to keep the type uniform.
            serde_json::to_value(sorted).expect("BTreeMap<String, Value> always converts to Value")
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
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

    // ----- ModelIr / ProjectIr round-trip + recipe-hash tests -----

    use crate::lineage::{LineageEdge, QualifiedColumn};
    use crate::traits::MaskStrategy;
    use crate::types::{RockyType, TypedColumn};
    use rocky_sql::lineage::TransformKind;

    fn sample_replication_model() -> ModelIr {
        ModelIr {
            name: Arc::from("orders"),
            sql: "SELECT * FROM source_catalog.src__acme.shopify.orders".into(),
            typed_columns: vec![TypedColumn {
                name: "id".into(),
                data_type: RockyType::Int64,
                nullable: false,
            }],
            lineage_edges: vec![],
            materialization: MaterializationStrategy::Incremental {
                timestamp_column: "_fivetran_synced".into(),
            },
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: true,
                auto_create_schemas: true,
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "raw__shopify".into(),
                table: "orders".into(),
            },
            column_masks: vec![],
            source: Some(SourceRef {
                catalog: "source_catalog".into(),
                schema: "src__acme__shopify".into(),
                table: "orders".into(),
            }),
            sources: vec![],
            columns: Some(ColumnSelection::All),
            metadata_columns: vec![MetadataColumn {
                name: "_loaded_by".into(),
                data_type: "STRING".into(),
                value: "NULL".into(),
            }],
            unique_key: vec![],
            updated_at: None,
            invalidate_hard_deletes: false,
            format: None,
            format_options: None,
        }
    }

    fn sample_transformation_model() -> ModelIr {
        ModelIr {
            name: Arc::from("dim_customers"),
            sql: "SELECT customer_id, email FROM stg_customers".into(),
            typed_columns: vec![
                TypedColumn {
                    name: "customer_id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "email".into(),
                    data_type: RockyType::String,
                    nullable: true,
                },
            ],
            lineage_edges: vec![LineageEdge {
                source: QualifiedColumn {
                    model: Arc::from("stg_customers"),
                    column: Arc::from("customer_id"),
                },
                target: QualifiedColumn {
                    model: Arc::from("dim_customers"),
                    column: Arc::from("customer_id"),
                },
                transform: TransformKind::Direct,
            }],
            materialization: MaterializationStrategy::Merge {
                unique_key: vec![Arc::from("customer_id")],
                update_columns: ColumnSelection::All,
            },
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "marts__customers".into(),
                table: "dim_customers".into(),
            },
            column_masks: vec![ColumnMask {
                column: Arc::from("email"),
                strategy: MaskStrategy::Hash,
            }],
            source: None,
            sources: vec![SourceRef {
                catalog: "acme_warehouse".into(),
                schema: "staging".into(),
                table: "stg_customers".into(),
            }],
            columns: None,
            metadata_columns: vec![],
            unique_key: vec![],
            updated_at: None,
            invalidate_hard_deletes: false,
            format: None,
            format_options: None,
        }
    }

    fn sample_snapshot_model() -> ModelIr {
        ModelIr {
            name: Arc::from("dim_users_history"),
            sql: String::new(),
            typed_columns: vec![],
            lineage_edges: vec![],
            materialization: MaterializationStrategy::FullRefresh,
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "snapshots".into(),
                table: "dim_users_history".into(),
            },
            column_masks: vec![],
            source: Some(SourceRef {
                catalog: "acme_warehouse".into(),
                schema: "marts".into(),
                table: "dim_users".into(),
            }),
            sources: vec![],
            columns: None,
            metadata_columns: vec![],
            unique_key: vec![Arc::from("user_id")],
            updated_at: Some("updated_at".into()),
            invalidate_hard_deletes: true,
            format: None,
            format_options: None,
        }
    }

    #[test]
    fn model_ir_replication_roundtrip_byte_stable() {
        let m = sample_replication_model();
        let json1 = serde_json::to_string(&m).unwrap();
        let back: ModelIr = serde_json::from_str(&json1).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json1, json2, "round-trip must be byte-stable");
    }

    #[test]
    fn model_ir_transformation_roundtrip_byte_stable() {
        let m = sample_transformation_model();
        let json1 = serde_json::to_string(&m).unwrap();
        let back: ModelIr = serde_json::from_str(&json1).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json1, json2, "round-trip must be byte-stable");
    }

    #[test]
    fn model_ir_snapshot_roundtrip_byte_stable() {
        let m = sample_snapshot_model();
        let json1 = serde_json::to_string(&m).unwrap();
        let back: ModelIr = serde_json::from_str(&json1).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json1, json2, "round-trip must be byte-stable");
    }

    #[test]
    fn project_ir_roundtrip_with_multiple_models() {
        let project = ProjectIr {
            models: vec![
                sample_replication_model(),
                sample_transformation_model(),
                sample_snapshot_model(),
            ],
            dag: vec![
                crate::dag::DagNode {
                    name: "orders".into(),
                    depends_on: vec![],
                },
                crate::dag::DagNode {
                    name: "dim_customers".into(),
                    depends_on: vec![],
                },
                crate::dag::DagNode {
                    name: "dim_users_history".into(),
                    depends_on: vec!["dim_customers".into()],
                },
            ],
            lineage_edges: vec![LineageEdge {
                source: QualifiedColumn {
                    model: Arc::from("stg_customers"),
                    column: Arc::from("customer_id"),
                },
                target: QualifiedColumn {
                    model: Arc::from("dim_customers"),
                    column: Arc::from("customer_id"),
                },
                transform: TransformKind::Direct,
            }],
        };
        let json1 = serde_json::to_string(&project).unwrap();
        let back: ProjectIr = serde_json::from_str(&json1).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json1, json2, "ProjectIr round-trip must be byte-stable");
        assert_eq!(back.models.len(), 3);
    }

    #[test]
    fn model_ir_missing_required_field_fails_noisily() {
        // The `sql` field has no serde default; dropping it must fail
        // deserialization rather than silently producing an empty SQL string.
        let json = r#"{
            "name": "orders",
            "typed_columns": [],
            "lineage_edges": [],
            "materialization": {"strategy": "FullRefresh"},
            "governance": {
                "permissions_file": null,
                "auto_create_catalogs": false,
                "auto_create_schemas": false
            },
            "target": {
                "catalog": "c",
                "schema": "s",
                "table": "t"
            }
        }"#;
        let err = serde_json::from_str::<ModelIr>(json).unwrap_err();
        assert!(
            err.to_string().contains("sql"),
            "expected error to mention missing `sql` field, got: {err}"
        );
    }

    #[test]
    fn recipe_hash_is_deterministic() {
        let m1 = sample_transformation_model();
        let m2 = sample_transformation_model();
        assert_eq!(
            m1.recipe_hash(),
            m2.recipe_hash(),
            "identical inputs must produce identical hashes"
        );
    }

    #[test]
    fn recipe_hash_changes_when_sql_changes() {
        let mut m = sample_transformation_model();
        let h1 = m.recipe_hash();
        m.sql.push_str(" -- a comment");
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "SQL text changes must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_column_mask_changes() {
        let mut m = sample_transformation_model();
        let h1 = m.recipe_hash();
        m.column_masks[0].strategy = MaskStrategy::Redact;
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "resolved-mask changes must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_typed_columns_change() {
        let mut m = sample_transformation_model();
        let h1 = m.recipe_hash();
        m.typed_columns.push(TypedColumn {
            name: "added_at".into(),
            data_type: RockyType::Timestamp,
            nullable: true,
        });
        let h2 = m.recipe_hash();
        assert_ne!(h1, h2);
    }

    #[test]
    fn project_recipe_hash_is_independent_of_model_order() {
        let a = sample_replication_model();
        let b = sample_transformation_model();
        let p1 = ProjectIr {
            models: vec![a.clone(), b.clone()],
            dag: vec![],
            lineage_edges: vec![],
        };
        let p2 = ProjectIr {
            models: vec![b, a],
            dag: vec![],
            lineage_edges: vec![],
        };
        assert_eq!(
            p1.recipe_hash(),
            p2.recipe_hash(),
            "project hash must sort per-model hashes before combining"
        );
    }

    #[test]
    fn empty_column_masks_omitted_from_serialization() {
        // Rule A consistency: empty Vec on `column_masks` is skipped.
        // sample_replication_model is the empty-masks fixture (transformation
        // carries a Hash mask on `email`).
        let m = sample_replication_model();
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            !json.contains("column_masks"),
            "empty column_masks must be skipped per the canonical-JSON rule, got: {json}"
        );
    }

    // ----- Plan ↔ ModelIr conversion + variant-coverage tests -----

    fn sample_replication_plan() -> Plan {
        Plan::Replication(ReplicationPlan {
            source: SourceRef {
                catalog: "source_catalog".into(),
                schema: "src__acme__shopify".into(),
                table: "orders".into(),
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "raw__shopify".into(),
                table: "orders".into(),
            },
            strategy: MaterializationStrategy::Incremental {
                timestamp_column: "_fivetran_synced".into(),
            },
            columns: ColumnSelection::Explicit(vec![
                Arc::from("id"),
                Arc::from("customer_id"),
            ]),
            metadata_columns: vec![MetadataColumn {
                name: "_loaded_by".into(),
                data_type: "STRING".into(),
                value: "NULL".into(),
            }],
            governance: GovernanceConfig {
                permissions_file: Some("perms.yaml".into()),
                auto_create_catalogs: true,
                auto_create_schemas: true,
            },
        })
    }

    fn sample_transformation_plan() -> Plan {
        Plan::Transformation(TransformationPlan {
            sources: vec![SourceRef {
                catalog: "acme_warehouse".into(),
                schema: "staging".into(),
                table: "stg_customers".into(),
            }],
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "marts".into(),
                table: "dim_customers".into(),
            },
            strategy: MaterializationStrategy::Merge {
                unique_key: vec![Arc::from("customer_id")],
                update_columns: ColumnSelection::All,
            },
            sql: "SELECT customer_id, email FROM stg_customers".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        })
    }

    fn sample_snapshot_plan() -> Plan {
        Plan::Snapshot(SnapshotPlan {
            source: SourceRef {
                catalog: "acme_warehouse".into(),
                schema: "marts".into(),
                table: "dim_users".into(),
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "snapshots".into(),
                table: "dim_users_history".into(),
            },
            unique_key: vec![Arc::from("user_id")],
            updated_at: "updated_at".into(),
            invalidate_hard_deletes: true,
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        })
    }

    /// Canonical-JSON-equality is the equivalence relation used to verify
    /// the [`Plan`] ↔ [`ModelIr`] round-trip — none of the [`Plan`]
    /// component types derive `PartialEq`, and adding the derive cascade
    /// was deemed out of scope for this PR.
    fn plans_canonical_eq(a: &Plan, b: &Plan) -> bool {
        canonical_json(a) == canonical_json(b)
    }

    #[test]
    fn plan_to_model_ir_replication_roundtrip() {
        let plan = sample_replication_plan();
        let ir = ModelIr::from(&plan);
        let back = ir.to_plan_compatible();
        assert!(
            plans_canonical_eq(&plan, &back),
            "replication round-trip must be canonical-JSON-equal\n  before: {}\n  after:  {}",
            canonical_json(&plan),
            canonical_json(&back)
        );
        assert!(matches!(back, Plan::Replication(_)));
    }

    #[test]
    fn plan_to_model_ir_replication_with_merge_strategy_roundtrip() {
        // Discrimination guardrail: `MaterializationStrategy::Merge` carries
        // its own `unique_key` field on the strategy enum. The top-level
        // `ModelIr::unique_key` field must stay empty for a Replication —
        // otherwise `to_plan_compatible` would mis-classify it as a
        // [`Plan::Snapshot`] (since updated_at would still be None this
        // wouldn't actually trigger today, but the test pins the contract).
        let plan = Plan::Replication(ReplicationPlan {
            source: SourceRef {
                catalog: "src".into(),
                schema: "raw".into(),
                table: "users".into(),
            },
            target: TargetRef {
                catalog: "tgt".into(),
                schema: "raw".into(),
                table: "users".into(),
            },
            strategy: MaterializationStrategy::Merge {
                unique_key: vec![Arc::from("id")],
                update_columns: ColumnSelection::All,
            },
            columns: ColumnSelection::All,
            metadata_columns: vec![],
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        });
        let ir = ModelIr::from(&plan);
        assert!(
            ir.unique_key.is_empty(),
            "Merge-strategy unique_key must NOT leak into top-level ModelIr.unique_key"
        );
        let back = ir.to_plan_compatible();
        assert!(
            plans_canonical_eq(&plan, &back),
            "replication-with-merge round-trip must be canonical-JSON-equal"
        );
        assert!(matches!(back, Plan::Replication(_)));
    }

    #[test]
    fn plan_to_model_ir_transformation_roundtrip() {
        let plan = sample_transformation_plan();
        let ir = ModelIr::from(&plan);
        let back = ir.to_plan_compatible();
        assert!(
            plans_canonical_eq(&plan, &back),
            "transformation round-trip must be canonical-JSON-equal\n  before: {}\n  after:  {}",
            canonical_json(&plan),
            canonical_json(&back)
        );
        assert!(matches!(back, Plan::Transformation(_)));
    }

    #[test]
    fn plan_to_model_ir_snapshot_roundtrip() {
        let plan = sample_snapshot_plan();
        let ir = ModelIr::from(&plan);
        let back = ir.to_plan_compatible();
        assert!(
            plans_canonical_eq(&plan, &back),
            "snapshot round-trip must be canonical-JSON-equal\n  before: {}\n  after:  {}",
            canonical_json(&plan),
            canonical_json(&back)
        );
        assert!(matches!(back, Plan::Snapshot(_)));
    }

    #[test]
    fn recipe_hash_changes_when_replication_source_changes() {
        let mut m = sample_replication_model();
        let h1 = m.recipe_hash();
        if let Some(src) = m.source.as_mut() {
            src.table = "different_table".into();
        } else {
            panic!("sample_replication_model must carry source");
        }
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "changing replication source must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_target_changes() {
        let mut m = sample_replication_model();
        let h1 = m.recipe_hash();
        m.target.table = "different_target".into();
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "changing target must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_unique_key_changes() {
        let mut m = sample_snapshot_model();
        let h1 = m.recipe_hash();
        m.unique_key.push(Arc::from("tenant_id"));
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "changing snapshot unique_key must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_invalidate_hard_deletes_flips() {
        let mut m = sample_snapshot_model();
        let h1 = m.recipe_hash();
        m.invalidate_hard_deletes = !m.invalidate_hard_deletes;
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "flipping invalidate_hard_deletes must propagate into the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_replication_plan_round_trips_into_model_ir_deterministically() {
        // Same plan input → same ModelIr → same recipe-hash.
        let plan = sample_replication_plan();
        let h1 = ModelIr::from(&plan).recipe_hash();
        let h2 = ModelIr::from(&plan).recipe_hash();
        assert_eq!(h1, h2, "From<&Plan> must produce a deterministic ModelIr");
    }

    #[test]
    fn invalidate_hard_deletes_default_omitted_from_serialization() {
        // Rule A bool-default extension: `false` is the semantic default and
        // must be skipped per the canonical-JSON rule.
        let m = sample_replication_model();
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            !json.contains("invalidate_hard_deletes"),
            "default-false invalidate_hard_deletes must be skipped, got: {json}"
        );
    }

    #[test]
    fn empty_sources_omitted_from_replication_serialization() {
        // Replication models populate `source` (singular) but not `sources`
        // (plural Vec). The empty Vec must be skipped per the canonical
        // JSON rule.
        let m = sample_replication_model();
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            !json.contains("\"sources\":"),
            "empty sources Vec must be skipped, got: {json}"
        );
    }

    #[test]
    fn empty_unique_key_omitted_from_replication_serialization() {
        let m = sample_replication_model();
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            !json.contains("\"unique_key\":"),
            "empty unique_key Vec must be skipped, got: {json}"
        );
    }

    #[test]
    fn columns_field_present_only_for_replication() {
        let rep = sample_replication_model();
        let tx = sample_transformation_model();
        let snap = sample_snapshot_model();

        assert!(
            serde_json::to_string(&rep).unwrap().contains("\"columns\":"),
            "replication ModelIr must serialize `columns`"
        );
        assert!(
            !serde_json::to_string(&tx).unwrap().contains("\"columns\":"),
            "transformation ModelIr must omit `columns`"
        );
        assert!(
            !serde_json::to_string(&snap).unwrap().contains("\"columns\":"),
            "snapshot ModelIr must omit `columns`"
        );
    }
}
