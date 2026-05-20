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
use crate::time_grain::TimeGrain;

/// How to materialize the target table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy")]
pub enum MaterializationStrategy {
    /// Drop and recreate the entire table.
    FullRefresh,
    /// Append rows newer than the source-side watermark.
    ///
    /// The watermark value itself is not carried on the strategy: it is
    /// runtime state read from the embedded state store before SQL
    /// generation. The SQL generator threads it into the dialect's
    /// `watermark_where` as a literal — `WHERE ts > TIMESTAMP '<prior>'`
    /// — so the WHERE clause references source only. After a successful
    /// execute the runner re-queries `MAX(ts) FROM source` and persists
    /// that as the next watermark. Keeping the field off the strategy
    /// means recipe-hash inputs are runtime-state-free.
    Incremental { timestamp_column: String },
    /// Upsert based on unique key columns.
    Merge {
        unique_key: Vec<Arc<str>>,
        update_columns: ColumnSelection,
    },
    /// SQL view — no physical storage. Each query against the target
    /// re-executes the model SELECT. Cheap to refresh, expensive to read
    /// repeatedly. Supported on every warehouse (`CREATE OR REPLACE VIEW`).
    View,
    /// Materialized view — warehouse manages refresh. Supported on
    /// Databricks, Snowflake, and BigQuery. Adapters that don't support it
    /// surface a clear error at SQL-gen time.
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
    /// Content-addressed write to a Delta UniForm table.
    ///
    /// The model's SELECT is executed by the runtime; the resulting rows
    /// are written as a content-hash-named Parquet file via the
    /// `rocky_iceberg::uniform_writer` library, and a `_delta_log/{N}.json`
    /// commit references it. Cross-engine reads (DuckDB iceberg_scan,
    /// Iceberg-aware Trino, ...) require `MSCK REPAIR TABLE ... SYNC
    /// METADATA` after each commit; the runtime issues this automatically.
    ///
    /// SQL generation does not run for this strategy — `sql_gen` returns
    /// an error, and the runner takes over the materialization path. See
    /// `rocky_iceberg::uniform_writer` for the writer surface.
    ContentAddressed {
        /// Object-store key prefix containing `_delta_log/` and the
        /// table's Parquet files. Typically `s3://<bucket>/<path>/<table>`
        /// for AWS-backed deployments. Resolved at config-parse time; the
        /// runtime constructs an `ObjectStore` from this.
        storage_prefix: String,
        /// If non-empty, the model's SELECT must produce exactly these
        /// partition columns. The runtime pre-groups rows by partition
        /// tuple and emits one `add` action per group. Empty means the
        /// target is unpartitioned.
        ///
        /// At runtime, this list is asserted equal to what
        /// `UniformWriter::discover()` returns; mismatch is a hard error.
        partition_columns: Vec<String>,
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
///
/// `last_value` is **source-side**: the `MAX(timestamp_column) FROM source`
/// observed during the last successful run. The runner re-queries source
/// post-execute and persists the value here; the next run reads it back
/// and threads it into the dialect's `watermark_where` clause as a literal
/// (no correlated subquery against target).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    /// The `MAX(timestamp_column) FROM source` from the last successful run.
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
/// Produced by `rocky_core::role_graph::flatten_role_graph` and consumed
/// by `rocky_core::traits::GovernanceAdapter::reconcile_role_graph`. The
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
/// Policy lives in `rocky_core::config::RockyConfig` (workspace-default
/// `[mask]` block plus optional per-env `[mask.<env>]` overrides). The
/// resolution-as-applied — column name plus the strategy chosen for the
/// active environment — is captured here so the recipe-hash reflects what
/// the warehouse actually sees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMask {
    /// Column name, interned for cheap cloning.
    pub column: Arc<str>,
    /// Resolved strategy for the active environment.
    pub strategy: crate::mask::MaskStrategy,
}

/// Per-model cost ceiling — the maximum allowed spend for a single run.
///
/// Both fields are optional; a `CostBudget` with both fields `None` is
/// collapsed to `cost_ceiling = None` by the sidecar-to-IR conversion in
/// `rocky-core::models::Model::to_model_ir()`. Callers that want to check
/// whether the budget carries any constraint should use [`CostBudget::is_empty`].
///
/// This type carries only the *cost dimensions* (`max_usd`,
/// `max_bytes_scanned`). Policy fields (`on_breach`, `max_duration_ms`) live
/// on `rocky-core::config::ModelBudgetConfig` and are not part of the IR.
///
/// Serialized following the canonical-JSON rule: each `Option` field is
/// omitted when `None` so the recipe-hash never sees a stale `null`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostBudget {
    /// Maximum allowed cost in USD for a single run of this model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_usd: Option<f64>,
    /// Maximum bytes scanned for a single run of this model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_scanned: Option<u64>,
}

impl CostBudget {
    /// Returns `true` when neither cost field is set.
    ///
    /// Used by `to_model_ir()` to avoid storing a vacuous
    /// `Some(CostBudget { None, None })` which would pollute recipe-hashes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.max_usd.is_none() && self.max_bytes_scanned.is_none()
    }
}

/// Per-model intermediate representation.
///
/// Carries everything Rocky needs to know about a single model to generate
/// SQL, run governance, and compute a recipe-hash: the SQL itself, the
/// typed output columns, the lineage edges that target this model, the
/// materialization strategy, governance metadata, the resolved
/// column-masking plan for the active environment, and the source / target
/// table refs plus variant-specific metadata (column selection, metadata
/// columns, snapshot key/timestamp, lakehouse format).
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
/// discriminant to canonicalise. Variant inference happens via
/// [`Self::variant`], which classifies the IR by which variant-specific
/// fields are populated.
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
    /// Target table reference. Required on every variant.
    pub target: TargetRef,
    /// Resolved column masks for the active environment. Populated at IR
    /// construction by reading
    /// `rocky_core::config::RockyConfig::resolve_mask_for_env`. Empty when no
    /// classifications matched or every resolved strategy was
    /// [`crate::mask::MaskStrategy::None`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_masks: Vec<ColumnMask>,
    /// Single-source ref. `Some` for [`ModelIrVariant::Replication`] and
    /// [`ModelIrVariant::Snapshot`]; `None` for
    /// [`ModelIrVariant::Transformation`] (which uses [`Self::sources`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceRef>,
    /// Multi-source refs (joins). Populated for
    /// [`ModelIrVariant::Transformation`]; empty for
    /// [`ModelIrVariant::Replication`] and [`ModelIrVariant::Snapshot`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<SourceRef>,
    /// Column selection. `Some` for [`ModelIrVariant::Replication`]; `None`
    /// for other variants (which select via the `sql` field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<ColumnSelection>,
    /// Extra columns added during replication (e.g.
    /// `CAST(NULL AS STRING) AS _loaded_by`). Populated for
    /// [`ModelIrVariant::Replication`]; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata_columns: Vec<MetadataColumn>,
    /// Columns that uniquely identify a snapshot row. Populated for
    /// [`ModelIrVariant::Snapshot`]; empty otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<Arc<str>>,
    /// Column used to detect changes for SCD2 snapshots. `Some` for
    /// [`ModelIrVariant::Snapshot`]; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// When true, invalidate rows deleted from source. Only meaningful
    /// for [`ModelIrVariant::Snapshot`]; `false` otherwise.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub invalidate_hard_deletes: bool,
    /// Optional lakehouse table format (Delta, Iceberg, ...). `None` for
    /// non-transformation models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<LakehouseFormat>,
    /// Format-specific options (partitioning, clustering, properties).
    /// `None` for non-transformation models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_options: Option<LakehouseOptions>,
    /// Declared per-model cost ceiling from the `[budget]` sidecar block.
    ///
    /// `None` means no ceiling was declared (or only policy fields like
    /// `on_breach` were set — those stay in `rocky-core::config`). `Some`
    /// means the user declared at least one cost dimension in their sidecar.
    ///
    /// # Runtime wiring
    ///
    /// TODO(D-2): wire into `propagate_costs` once the
    /// `CatalogClient::table_stats` provider spike (D-2) reaches a
    /// go-decision. The field is populated today; the enforcement check is
    /// not yet called.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_ceiling: Option<CostBudget>,
}

/// Inferred variant tag for a [`ModelIr`].
///
/// Returned by [`ModelIr::variant`]. The inference rule (snapshot wins
/// over replication, replication wins over transformation) is centralised
/// in [`ModelIr::variant`].
///
/// Serialises as lowercase (`"replication"` / `"transformation"` /
/// `"snapshot"`) so error messages and JSON output stay aligned with the
/// pre-typed-enum string convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelIrVariant {
    Replication,
    Transformation,
    Snapshot,
}

impl ModelIrVariant {
    /// Lowercase wire name. Used by [`std::fmt::Display`] and by callers
    /// that need the raw `&'static str` (e.g. error-template assertions).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Replication => "replication",
            Self::Transformation => "transformation",
            Self::Snapshot => "snapshot",
        }
    }
}

impl std::fmt::Display for ModelIrVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl ModelIr {
    /// Construct a replication-shaped [`ModelIr`] directly.
    ///
    /// Variant inference (see [`Self::variant`]) returns
    /// [`ModelIrVariant::Replication`] for any IR built by this constructor,
    /// since `columns` is `Some` and snapshot-shape fields stay empty.
    ///
    /// `name` defaults to `target.table`; `sql`, `typed_columns`,
    /// `lineage_edges`, `sources`, `column_masks`, `unique_key`,
    /// `updated_at`, `invalidate_hard_deletes`, `format`, and
    /// `format_options` default to their zero values. Callers that need
    /// richer typed-column or lineage data should mutate those fields
    /// after construction.
    pub fn replication(
        target: TargetRef,
        strategy: MaterializationStrategy,
        source: SourceRef,
        columns: ColumnSelection,
        metadata_columns: Vec<MetadataColumn>,
        governance: GovernanceConfig,
    ) -> Self {
        Self {
            name: Arc::from(target.table.as_str()),
            sql: String::new(),
            typed_columns: Vec::new(),
            lineage_edges: Vec::new(),
            materialization: strategy,
            governance,
            target,
            column_masks: Vec::new(),
            source: Some(source),
            sources: Vec::new(),
            columns: Some(columns),
            metadata_columns,
            unique_key: Vec::new(),
            updated_at: None,
            invalidate_hard_deletes: false,
            format: None,
            format_options: None,
            cost_ceiling: None,
        }
    }

    /// Construct a transformation-shaped [`ModelIr`] directly.
    ///
    /// Variant inference (see [`Self::variant`]) returns
    /// [`ModelIrVariant::Transformation`] for any IR built by this
    /// constructor — `columns` stays `None` and snapshot-shape fields
    /// stay empty.
    ///
    /// `name` defaults to `target.table`. Callers that have a richer
    /// project-unique identifier (e.g. the compiler-side
    /// `rocky_core::models::Model::to_model_ir`) should mutate `name` after
    /// construction. `typed_columns`, `lineage_edges`, `column_masks`,
    /// `source`, `metadata_columns`, `unique_key`, `updated_at`, and
    /// `invalidate_hard_deletes` default to their zero values.
    pub fn transformation(
        target: TargetRef,
        strategy: MaterializationStrategy,
        sources: Vec<SourceRef>,
        sql: String,
        governance: GovernanceConfig,
        format: Option<LakehouseFormat>,
        format_options: Option<LakehouseOptions>,
    ) -> Self {
        Self {
            name: Arc::from(target.table.as_str()),
            sql,
            typed_columns: Vec::new(),
            lineage_edges: Vec::new(),
            materialization: strategy,
            governance,
            target,
            column_masks: Vec::new(),
            source: None,
            sources,
            columns: None,
            metadata_columns: Vec::new(),
            unique_key: Vec::new(),
            updated_at: None,
            invalidate_hard_deletes: false,
            format,
            format_options,
            cost_ceiling: None,
        }
    }

    /// Construct a snapshot-shaped [`ModelIr`] directly.
    ///
    /// `materialization` is fixed at [`MaterializationStrategy::FullRefresh`]
    /// to match the existing snapshot SQL-gen contract — SCD2 logic is
    /// implicit in [`crate::sql_gen::generate_snapshot_sql`] and does not
    /// depend on a strategy variant.
    ///
    /// `name` defaults to `target.table`. `sql`, `typed_columns`,
    /// `lineage_edges`, `sources`, `column_masks`, `columns`,
    /// `metadata_columns`, `format`, and `format_options` default to their
    /// zero values.
    pub fn snapshot(
        target: TargetRef,
        source: SourceRef,
        unique_key: Vec<Arc<str>>,
        updated_at: String,
        invalidate_hard_deletes: bool,
        governance: GovernanceConfig,
    ) -> Self {
        Self {
            name: Arc::from(target.table.as_str()),
            sql: String::new(),
            typed_columns: Vec::new(),
            lineage_edges: Vec::new(),
            materialization: MaterializationStrategy::FullRefresh,
            governance,
            target,
            column_masks: Vec::new(),
            source: Some(source),
            sources: Vec::new(),
            columns: None,
            metadata_columns: Vec::new(),
            unique_key,
            updated_at: Some(updated_at),
            invalidate_hard_deletes,
            format: None,
            format_options: None,
            cost_ceiling: None,
        }
    }

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

    /// Predicate underlying variant inference for snapshot. The single
    /// source of truth shared by [`Self::variant`]. Snapshot takes
    /// priority over replication in inference: if both `unique_key`
    /// (non-empty) and `updated_at` (`Some`) are populated the IR is
    /// classified as a snapshot regardless of which other variant-
    /// specific fields are also set.
    fn is_snapshot_shaped(&self) -> bool {
        !self.unique_key.is_empty() && self.updated_at.is_some()
    }

    /// Inferred [`ModelIrVariant`] for this IR.
    ///
    /// Inference rules: snapshot-shaped (`unique_key` non-empty AND
    /// `updated_at` `Some`) → [`ModelIrVariant::Snapshot`]; replication-
    /// shaped (`columns` `Some`) → [`ModelIrVariant::Replication`];
    /// otherwise → [`ModelIrVariant::Transformation`].
    ///
    /// Callers that need just the lowercase string form (e.g. error
    /// message templating, JSON output) reach for [`ModelIrVariant::as_str`]
    /// or the `Display` impl.
    pub fn variant(&self) -> ModelIrVariant {
        if self.is_snapshot_shaped() {
            ModelIrVariant::Snapshot
        } else if self.columns.is_some() {
            ModelIrVariant::Replication
        } else {
            ModelIrVariant::Transformation
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
    use crate::mask::MaskStrategy;
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
            cost_ceiling: None,
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
            cost_ceiling: None,
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
            cost_ceiling: None,
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

    #[test]
    fn recipe_hash_changes_when_switching_to_content_addressed() {
        let mut m = sample_transformation_model();
        let h_before = m.recipe_hash();
        m.materialization = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset/dim_customers".into(),
            partition_columns: vec![],
        };
        let h_after = m.recipe_hash();
        assert_ne!(
            h_before, h_after,
            "switching to ContentAddressed must change the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_content_addressed_storage_prefix_changes() {
        let mut m = sample_transformation_model();
        m.materialization = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset/dim_customers".into(),
            partition_columns: vec![],
        };
        let h1 = m.recipe_hash();
        m.materialization = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset_v2/dim_customers".into(),
            partition_columns: vec![],
        };
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "changing the content-addressed storage prefix must change the recipe hash"
        );
    }

    #[test]
    fn recipe_hash_changes_when_content_addressed_partition_columns_change() {
        let mut m = sample_transformation_model();
        m.materialization = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset/dim_customers".into(),
            partition_columns: vec![],
        };
        let h1 = m.recipe_hash();
        m.materialization = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset/dim_customers".into(),
            partition_columns: vec!["region".to_string()],
        };
        let h2 = m.recipe_hash();
        assert_ne!(
            h1, h2,
            "adding a partition column must change the recipe hash"
        );
    }

    #[test]
    fn content_addressed_serde_round_trip() {
        let strategy = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://example-bucket/dataset/t".into(),
            partition_columns: vec!["region".to_string(), "year".to_string()],
        };
        let json = serde_json::to_string(&strategy).unwrap();
        let back: MaterializationStrategy = serde_json::from_str(&json).unwrap();
        match back {
            MaterializationStrategy::ContentAddressed {
                storage_prefix,
                partition_columns,
            } => {
                assert_eq!(storage_prefix, "s3://example-bucket/dataset/t");
                assert_eq!(partition_columns, vec!["region", "year"]);
            }
            other => panic!("expected ContentAddressed, got {other:?}"),
        }
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
    fn recipe_hash_is_deterministic_for_identical_sample_models() {
        // Same ModelIr inputs → same recipe-hash.
        let h1 = sample_replication_model().recipe_hash();
        let h2 = sample_replication_model().recipe_hash();
        assert_eq!(h1, h2, "identical ModelIr inputs must hash identically");
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
            serde_json::to_string(&rep)
                .unwrap()
                .contains("\"columns\":"),
            "replication ModelIr must serialize `columns`"
        );
        assert!(
            !serde_json::to_string(&tx).unwrap().contains("\"columns\":"),
            "transformation ModelIr must omit `columns`"
        );
        assert!(
            !serde_json::to_string(&snap)
                .unwrap()
                .contains("\"columns\":"),
            "snapshot ModelIr must omit `columns`"
        );
    }

    #[test]
    fn variant_matches_inferred_shape() {
        assert_eq!(
            sample_replication_model().variant(),
            ModelIrVariant::Replication
        );
        assert_eq!(
            sample_transformation_model().variant(),
            ModelIrVariant::Transformation
        );
        assert_eq!(sample_snapshot_model().variant(), ModelIrVariant::Snapshot);
    }

    /// Variant inference must honour the same priority order as the
    /// accessors: snapshot wins over replication when both fields are set.
    #[test]
    fn variant_follows_inference_priority() {
        let mut ir = sample_snapshot_model();
        ir.columns = Some(ColumnSelection::All);
        assert_eq!(ir.variant(), ModelIrVariant::Snapshot);
    }

    /// Sample fixtures must each map to their expected variant — guards
    /// against drift in the shared [`ModelIr::is_snapshot_shaped`]
    /// inference root.
    #[test]
    fn variant_matches_sample_fixtures() {
        let cases = [
            (sample_replication_model(), ModelIrVariant::Replication),
            (
                sample_transformation_model(),
                ModelIrVariant::Transformation,
            ),
            (sample_snapshot_model(), ModelIrVariant::Snapshot),
        ];
        for (ir, expected) in cases {
            assert_eq!(ir.variant(), expected);
        }
    }

    #[test]
    fn variant_as_str_matches_display() {
        assert_eq!(ModelIrVariant::Replication.as_str(), "replication");
        assert_eq!(ModelIrVariant::Transformation.as_str(), "transformation");
        assert_eq!(ModelIrVariant::Snapshot.as_str(), "snapshot");
        assert_eq!(format!("{}", ModelIrVariant::Replication), "replication");
    }

    // ----- CostBudget / cost_ceiling tests -----

    #[test]
    fn cost_ceiling_none_omitted_from_serialization() {
        // Canonical-JSON rule: `None` Option fields must be absent from JSON.
        let m = sample_replication_model(); // cost_ceiling is None
        let json = serde_json::to_string(&m).unwrap();
        assert!(
            !json.contains("cost_ceiling"),
            "absent cost_ceiling must be skipped, got: {json}"
        );
    }

    #[test]
    fn cost_ceiling_roundtrip_byte_stable() {
        let mut m = sample_transformation_model();
        m.cost_ceiling = Some(CostBudget {
            max_usd: Some(10.0),
            max_bytes_scanned: Some(1_000_000_000),
        });
        let json1 = serde_json::to_string(&m).unwrap();
        let back: ModelIr = serde_json::from_str(&json1).unwrap();
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json1, json2, "cost_ceiling round-trip must be byte-stable");
        let cb = back.cost_ceiling.unwrap();
        assert_eq!(cb.max_usd, Some(10.0));
        assert_eq!(cb.max_bytes_scanned, Some(1_000_000_000));
    }

    #[test]
    fn recipe_hash_changes_when_cost_ceiling_changes() {
        let mut m = sample_transformation_model();
        let h1 = m.recipe_hash();
        m.cost_ceiling = Some(CostBudget {
            max_usd: Some(5.0),
            max_bytes_scanned: None,
        });
        let h2 = m.recipe_hash();
        assert_ne!(h1, h2, "adding cost_ceiling must change the recipe hash");
    }

    #[test]
    fn cost_budget_is_empty() {
        let empty = CostBudget {
            max_usd: None,
            max_bytes_scanned: None,
        };
        assert!(empty.is_empty(), "both-None CostBudget must be empty");

        let with_usd = CostBudget {
            max_usd: Some(1.0),
            max_bytes_scanned: None,
        };
        assert!(
            !with_usd.is_empty(),
            "CostBudget with max_usd must not be empty"
        );

        let with_bytes = CostBudget {
            max_usd: None,
            max_bytes_scanned: Some(500_000),
        };
        assert!(
            !with_bytes.is_empty(),
            "CostBudget with max_bytes_scanned must not be empty"
        );
    }

    #[test]
    fn cost_budget_partial_none_omitted() {
        // A CostBudget with only max_usd set must omit max_bytes_scanned from JSON.
        let cb = CostBudget {
            max_usd: Some(3.5),
            max_bytes_scanned: None,
        };
        let json = serde_json::to_string(&cb).unwrap();
        assert!(
            json.contains("max_usd"),
            "present max_usd must appear in JSON"
        );
        assert!(
            !json.contains("max_bytes_scanned"),
            "absent max_bytes_scanned must be skipped, got: {json}"
        );
    }
}
