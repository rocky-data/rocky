//! Supporting value types for [`crate::CatalogClient`].
//!
//! These types are the lowest common denominator across the catalog
//! implementations that target the converged surface (Iceberg REST, Unity
//! Catalog REST, Apache Polaris, Project Nessie). Richer adapter-specific
//! representations live in the warehouse / catalog adapter crates.
//!
//! Note that Rocky already carries a `TableRef` in `rocky-ir` that models
//! the warehouse three-part name (`catalog.schema.table`). The
//! [`TableRef`] defined here is intentionally distinct: it follows the
//! catalog-side model where a namespace is a *list* of name parts (Iceberg
//! REST allows arbitrarily nested namespaces). The two are not
//! interchangeable; conversion between them is the responsibility of the
//! adapter crate that bridges Rocky's IR to a concrete catalog.

use serde::{Deserialize, Serialize};

/// A reference to a table inside a catalog.
///
/// Catalogs model namespaces as ordered sequences of parts rather than a
/// fixed two-level `schema.table` shape. Iceberg REST exposes this directly
/// via path segments under `/v1/namespaces/{ns}`; Polaris and Unity follow
/// the same model. The optional `catalog` field names the top-level
/// catalog when the client is multi-catalog-aware (e.g. Polaris federated
/// catalogs) and is left `None` otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    /// Optional top-level catalog name. `None` when the client is bound to
    /// a single catalog and the field is implicit in the connection.
    pub catalog: Option<String>,
    /// Ordered namespace parts (e.g. `["analytics", "marketing"]`).
    pub namespace: Vec<String>,
    /// The unqualified table name.
    pub name: String,
}

impl TableRef {
    /// Convenience constructor for the common single-namespace case.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            catalog: None,
            namespace: vec![namespace.into()],
            name: name.into(),
        }
    }
}

/// A branch or tag attached to a table.
///
/// Branches and tags are unified here because both Iceberg's spec (in which
/// they are `SnapshotReference`s embedded in table metadata) and Nessie's
/// catalog-level branch resources can be projected onto this single shape.
/// The [`BranchKind`] discriminates the two cases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    /// The branch or tag name as the catalog stores it.
    pub name: String,
    /// The snapshot the branch / tag currently points at. `None` when the
    /// catalog has not yet attached a snapshot (a freshly created branch
    /// against an empty table, for example).
    pub snapshot_id: Option<i64>,
    /// Whether this reference is a branch (mutable head) or a tag
    /// (immutable label).
    pub kind: BranchKind,
}

/// Discriminates branch references from tag references.
///
/// The serde representation is kebab-case to match REST-spec convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BranchKind {
    /// A mutable named pointer that advances with each commit.
    Branch,
    /// An immutable label pinned to a specific snapshot.
    Tag,
}

/// A simple permission grant on a table.
///
/// This is intentionally minimal — `principal` identifies the grantee in
/// whatever form the catalog uses (user, group, role, service principal)
/// and `privilege` is a catalog-specific privilege name (e.g. `SELECT`,
/// `MODIFY`, `USE_CATALOG`). Catalog-specific extensions to the grant
/// model (Polaris subcatalog RBAC, Unity ABAC predicates, etc.) live in
/// adapter crates rather than this trait surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// Catalog-specific principal identifier.
    pub principal: String,
    /// Catalog-specific privilege name.
    pub privilege: String,
}

/// A table schema as the catalog exposes it.
///
/// This is deliberately the lowest common denominator: every catalog that
/// implements the converged surface can produce a `name + type-string +
/// nullability` triple per column. Richer schema representations (Iceberg
/// `Schema`, Spark `StructType`, Arrow `Schema`) belong inside the adapter
/// crate that produced or consumed this value, where the field-id and
/// nested-type information they carry is meaningful.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Columns in declaration order.
    pub columns: Vec<ColumnSchema>,
}

/// A single column inside a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name as the catalog stores it.
    pub name: String,
    /// Catalog-specific type representation, rendered as a string. The
    /// exact spelling is catalog-defined (e.g. Iceberg `string`,
    /// Spark `STRING`, Unity `varchar(255)`).
    pub type_str: String,
    /// Whether the column accepts NULL values.
    pub nullable: bool,
}

/// Per-table statistics that a catalog can serve back without running a
/// scan-cost query against the warehouse.
///
/// Every field is `Option<u64>` because catalogs vary in what they
/// natively expose. Iceberg REST returns `total-records` and
/// `total-files-size` in a snapshot's `summary` map when the writer
/// populated them — older or pre-aggregation snapshots may omit either.
/// Unity Catalog's REST surface does not serve table stats at all, so
/// [`crate::CatalogClient::table_stats`] returns
/// [`crate::CatalogError::UnsupportedOperation`] from that impl; the
/// SQL-driven stats path (`DESCRIBE DETAIL` for bytes, `ANALYZE TABLE`
/// or `SELECT COUNT(*)` for rows) lives on the per-adapter surface
/// rather than this trait.
///
/// Callers that need the cost-model's `rocky_core::cost::TableStats`
/// (which requires both `row_count` and `avg_row_bytes`) convert at the
/// binding site: skip the model when either field is `None`,
/// `propagate_costs` then falls through to upstream-inferred estimates
/// for that node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStats {
    /// Total row count across all files in the current snapshot, if the
    /// catalog reports it.
    pub row_count: Option<u64>,
    /// Total size in bytes across all data files in the current
    /// snapshot, if the catalog reports it.
    pub total_bytes: Option<u64>,
    /// Number of data files in the current snapshot, if the catalog
    /// reports it.
    pub file_count: Option<u64>,
}

impl TableStats {
    /// Construct an empty `TableStats` with every field `None`.
    ///
    /// Used by implementations that want to populate fields
    /// individually as they parse the catalog response.
    pub fn empty() -> Self {
        Self {
            row_count: None,
            total_bytes: None,
            file_count: None,
        }
    }
}

/// One table's contribution to a multi-table transaction.
///
/// A [`crate::CatalogClient::commit_transaction`] call applies a slice of
/// these atomically. The current shape is deliberately minimal: it carries
/// the compare-and-swap pre-condition (`expected_snapshot_id`) and the
/// target snapshot. Real writers will extend the value carried by each
/// commit (manifest pointers, schema updates, partition spec changes) as
/// the converged surface widens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCommit {
    /// The table this commit applies to.
    pub table: TableRef,
    /// The snapshot the caller expects the table to be at before the
    /// commit lands. `None` means "no pre-condition" — typically used when
    /// the commit creates the table.
    pub expected_snapshot_id: Option<i64>,
    /// The snapshot id the table should advance to after this commit.
    pub new_snapshot_id: i64,
}
