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
/// these atomically. The headline pair is the compare-and-swap
/// pre-condition (`expected_snapshot_id`) and the target snapshot the
/// commit advances to (`new_snapshot_id`); the remaining fields widen the
/// surface to multi-ref commits and Iceberg-spec metadata mutations
/// (schema updates, partition spec changes) without giving up the
/// catalog-agnostic shape of the trait.
///
/// The [`Self::branch`] field selects which named ref the commit
/// advances — `None` defaults to `"main"` to preserve the v1 contract.
/// The two `extra_*` fields are escape hatches: catalog-agnostic typed
/// `MetadataUpdate` variants would let callers express schema and
/// partition-spec changes against the trait directly, but typing the
/// full Iceberg-spec update set in this crate is a meaningful surface
/// widening that needs cross-catalog (Polaris, Nessie, Unity) evidence
/// before it lands. Until then, adapters that natively speak Iceberg
/// metadata updates (today, the [`crate::CatalogClient`] impl on
/// `rocky_iceberg::IcebergCatalogClientAdapter`) honour
/// [`Self::extra_requirements`] / [`Self::extra_updates`] as opaque JSON
/// blobs appended to the synthesized `assert-ref-snapshot-id` /
/// `set-snapshot-ref` pair. Adapters that do not natively speak Iceberg
/// metadata updates ignore the extra payload and surface
/// `commit_transaction` as [`crate::CatalogError::UnsupportedOperation`]
/// the same way they do today.
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
    /// The named ref this commit advances. `None` defaults to `"main"`
    /// to preserve the v1 single-branch contract; pass `Some("branch")`
    /// to target a non-default branch.
    ///
    /// Adapters honour this field on both the assertion side
    /// (`assert-ref-snapshot-id` is pinned against the selected ref) and
    /// the update side (`set-snapshot-ref` advances the same ref). Tag
    /// refs are *not* writable through `commit_transaction` — they pin
    /// an immutable snapshot by spec; mutating a tag would belong on a
    /// dedicated `create_tag` / `drop_tag` extension surface.
    pub branch: Option<String>,
    /// Extra Iceberg-spec assertion entries appended to the request's
    /// `requirements` list. The synthesized `assert-ref-snapshot-id`
    /// (when `expected_snapshot_id.is_some()`) or `assert-create` (when
    /// `expected_snapshot_id.is_none()`) is always emitted first; these
    /// entries follow.
    ///
    /// Each entry is a raw JSON object matching the Iceberg REST
    /// [`commit-table-update`](https://iceberg.apache.org/spec/#commit-table-updates)
    /// requirement shapes (e.g. `assert-table-uuid`, `assert-last-assigned-field-id`).
    /// Empty vector preserves the v1 wire shape exactly.
    pub extra_requirements: Vec<serde_json::Value>,
    /// Extra Iceberg-spec metadata updates appended to the request's
    /// `updates` list. The synthesized `set-snapshot-ref` is always
    /// emitted first; these entries follow.
    ///
    /// Each entry is a raw JSON object matching the Iceberg REST
    /// [`commit-table-update`](https://iceberg.apache.org/spec/#commit-table-updates)
    /// action shapes (e.g. `add-schema`, `set-current-schema`,
    /// `add-partition-spec`, `set-default-spec`, `add-snapshot`,
    /// `set-properties`). Empty vector preserves the v1 wire shape
    /// exactly.
    pub extra_updates: Vec<serde_json::Value>,
}

impl TableCommit {
    /// Construct a minimal `TableCommit` against the default `main`
    /// branch with no extra requirements or updates.
    ///
    /// This is the v1 shape — kept as a convenience constructor so
    /// existing call sites stay terse. Use struct-literal syntax or the
    /// `with_*` setters to opt into multi-ref / schema / partition-spec
    /// payloads.
    pub fn new(table: TableRef, expected_snapshot_id: Option<i64>, new_snapshot_id: i64) -> Self {
        Self {
            table,
            expected_snapshot_id,
            new_snapshot_id,
            branch: None,
            extra_requirements: Vec::new(),
            extra_updates: Vec::new(),
        }
    }

    /// Builder setter: target a non-default branch.
    ///
    /// `None` resets to the default `"main"` ref. Pass `Some(name)` to
    /// advance a named branch instead.
    #[must_use]
    pub fn with_branch(mut self, branch: Option<String>) -> Self {
        self.branch = branch;
        self
    }

    /// Builder setter: append extra requirements to the commit.
    ///
    /// The synthesized `assert-ref-snapshot-id` / `assert-create` entry
    /// is always emitted first; the entries here follow in order.
    #[must_use]
    pub fn with_extra_requirements(mut self, requirements: Vec<serde_json::Value>) -> Self {
        self.extra_requirements = requirements;
        self
    }

    /// Builder setter: append extra metadata updates to the commit.
    ///
    /// The synthesized `set-snapshot-ref` entry is always emitted first;
    /// the entries here follow in order.
    #[must_use]
    pub fn with_extra_updates(mut self, updates: Vec<serde_json::Value>) -> Self {
        self.extra_updates = updates;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_table() -> TableRef {
        TableRef {
            catalog: None,
            namespace: vec!["analytics".into()],
            name: "orders".into(),
        }
    }

    #[test]
    fn table_commit_new_initialises_with_main_default() {
        // The v1 contract: `new` leaves `branch = None` and both
        // `extra_*` empty — the wire-side translation reads `branch =
        // None` as `"main"`, so existing call sites preserve their
        // behaviour byte-for-byte.
        let commit = TableCommit::new(sample_table(), Some(42), 43);
        assert_eq!(commit.expected_snapshot_id, Some(42));
        assert_eq!(commit.new_snapshot_id, 43);
        assert!(commit.branch.is_none(), "branch defaults to None (= main)");
        assert!(commit.extra_requirements.is_empty());
        assert!(commit.extra_updates.is_empty());
    }

    #[test]
    fn table_commit_with_branch_sets_named_ref() {
        let commit =
            TableCommit::new(sample_table(), Some(42), 43).with_branch(Some("dev_feature".into()));
        assert_eq!(commit.branch.as_deref(), Some("dev_feature"));
    }

    #[test]
    fn table_commit_with_branch_none_resets_to_default() {
        // Passing `None` to `with_branch` after a previous setter must
        // round-trip back to the default — important if a builder
        // chain mutates the field conditionally.
        let commit = TableCommit::new(sample_table(), Some(42), 43)
            .with_branch(Some("dev".into()))
            .with_branch(None);
        assert!(commit.branch.is_none());
    }

    #[test]
    fn table_commit_extras_round_trip() {
        let req = json!({ "type": "assert-table-uuid", "uuid": "deadbeef" });
        let upd = json!({ "action": "add-schema", "schema": { "schema-id": 1 } });
        let commit = TableCommit::new(sample_table(), None, 1)
            .with_extra_requirements(vec![req.clone()])
            .with_extra_updates(vec![upd.clone()]);
        assert_eq!(commit.extra_requirements, vec![req]);
        assert_eq!(commit.extra_updates, vec![upd]);
    }
}
