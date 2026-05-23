//! The [`CatalogClient`] trait — Rocky's catalog abstraction.

use async_trait::async_trait;

use crate::error::CatalogResult;
use crate::types::{BranchRef, Grant, TableCommit, TableRef, TableSchema, TableStats};

/// A read/write client for a data catalog.
///
/// `CatalogClient` absorbs the surface that is convergent across Iceberg
/// REST, Apache Polaris, Unity Catalog, and Project Nessie: namespace and
/// table discovery, table lifecycle, atomic multi-table commits, branch /
/// tag listing, and a thin governance surface. Adapter-specific extensions
/// (catalog-specific RBAC, branch merge semantics, materialized views) are
/// expected to live as additional trait methods or types on the adapter
/// crate that composes a `CatalogClient`, rather than being added here.
///
/// Operations that are not universally available — tagging and grants on
/// Iceberg REST, for example — must return
/// [`crate::CatalogError::UnsupportedOperation`] from implementations that
/// cannot serve them, rather than emulating the behaviour through a
/// side-channel. Callers are expected to interpret the variant as a soft
/// signal and pick an adapter-specific fallback (typically SQL DDL) when
/// the catalog does not expose the operation over REST.
///
/// The trait is `Send + Sync` because adapters are routinely composed
/// across async task boundaries; the `async_trait` macro is used so the
/// trait remains object-safe for `dyn CatalogClient` use.
#[async_trait]
pub trait CatalogClient: Send + Sync {
    // ---------------------------------------------------------------------
    // Discovery (read-only)
    // ---------------------------------------------------------------------

    /// List the tables that live directly under `namespace`.
    ///
    /// `namespace` is an ordered sequence of namespace parts (e.g.
    /// `["analytics", "marketing"]`). Implementations should paginate
    /// internally and return the complete list; the trait does not expose
    /// a cursor so callers do not have to handle catalog-specific paging
    /// semantics. Implementations that cannot enumerate the namespace
    /// (because it does not exist) must return
    /// [`crate::CatalogError::NamespaceNotFound`].
    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableRef>>;

    /// Describe `table` — return its current column schema as the catalog
    /// sees it.
    ///
    /// Implementations that cannot resolve the table must return
    /// [`crate::CatalogError::TableNotFound`].
    async fn describe_table(&self, table: &TableRef) -> CatalogResult<TableSchema>;

    // ---------------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------------

    /// Declare `table` in the catalog with the given `schema`.
    ///
    /// This is a metadata-only operation: it registers the table in the
    /// catalog. It does not run any compute, allocate storage, or copy
    /// data. Implementations that prefer to materialise an empty table at
    /// declaration time may do so, but the trait contract is "the table
    /// exists in the catalog and `describe_table` resolves" after this
    /// returns `Ok(())`.
    async fn create_table(&self, table: &TableRef, schema: &TableSchema) -> CatalogResult<()>;

    /// Drop the catalog declaration for `table`.
    ///
    /// This is a metadata-only operation. It deregisters the table from
    /// the catalog and, depending on catalog semantics, may or may not
    /// reclaim the underlying storage. It does not run any compute on the
    /// warehouse side. Implementations that cannot find the table must
    /// return [`crate::CatalogError::TableNotFound`].
    async fn drop_table(&self, table: &TableRef) -> CatalogResult<()>;

    // ---------------------------------------------------------------------
    // Transactions
    // ---------------------------------------------------------------------

    /// Apply a batch of [`TableCommit`]s atomically.
    ///
    /// Mirrors Iceberg REST's `POST /v1/transactions/commit` and the
    /// equivalent endpoints on Polaris and Unity. All commits in the
    /// slice succeed together or none of them do. Implementations must
    /// return [`crate::CatalogError::CommitConflict`] when the catalog's
    /// compare-and-swap rejects the commit (a concurrent writer landed
    /// against the same base snapshot); the caller is then expected to
    /// re-read state and retry.
    ///
    /// Each [`TableCommit`] carries three optional widening fields on
    /// top of the headline snapshot-advance pair:
    /// [`TableCommit::branch`] selects a non-default ref (defaults to
    /// `"main"`); [`TableCommit::extra_requirements`] and
    /// [`TableCommit::extra_updates`] append Iceberg-spec
    /// `commit-table-update` JSON entries (e.g. `add-schema`,
    /// `add-partition-spec`, `set-default-spec`) onto the wire payload.
    /// Adapters that do not natively speak the Iceberg update set MAY
    /// surface `commit_transaction` as
    /// [`crate::CatalogError::UnsupportedOperation`] regardless of
    /// whether the extra payload is populated.
    async fn commit_transaction(&self, commits: &[TableCommit]) -> CatalogResult<()>;

    // ---------------------------------------------------------------------
    // Branches / tags
    // ---------------------------------------------------------------------

    /// List the branches and tags attached to `table`.
    ///
    /// The Iceberg spec models branches and tags as `SnapshotReference`
    /// entries embedded in table metadata; Nessie exposes them as
    /// first-class catalog resources. Implementations project both shapes
    /// onto a uniform [`Vec<BranchRef>`] so callers do not have to branch
    /// on the underlying model. Mutation of branches (create, fast-forward,
    /// merge) is intentionally not part of this trait — those operations
    /// diverge sharply between catalogs and belong on per-adapter
    /// extension surfaces.
    async fn list_branches(&self, table: &TableRef) -> CatalogResult<Vec<BranchRef>>;

    // ---------------------------------------------------------------------
    // Statistics
    // ---------------------------------------------------------------------

    /// Return the row count, byte total, and file count for `table` as
    /// the catalog currently reports them.
    ///
    /// Each field of [`TableStats`] is `Option<u64>` because catalogs
    /// disagree on what they expose: Iceberg REST returns
    /// `total-records` and `total-files-size` only when the writer
    /// populated them in the current snapshot's `summary` map; Unity
    /// Catalog REST exposes none of them and surfaces this method as
    /// [`crate::CatalogError::UnsupportedOperation`]. Callers consuming
    /// the result for cost estimation are expected to treat a `None`
    /// field as "fall through to upstream-inferred estimate" rather
    /// than a hard failure.
    ///
    /// The trait does not promise that the numbers are recent — they
    /// are whatever the catalog has at read time. For Iceberg this is
    /// the head snapshot summary; for catalogs that go through this
    /// trait via a SQL-execution shim (out of scope today) it would be
    /// whatever `DESCRIBE DETAIL` or `ANALYZE TABLE` last populated.
    async fn table_stats(&self, table: &TableRef) -> CatalogResult<TableStats>;

    // ---------------------------------------------------------------------
    // Governance
    // ---------------------------------------------------------------------

    /// Attach a `key = value` tag to `table`.
    ///
    /// The value space is catalog-specific. Catalogs that do not expose
    /// tagging over their REST surface — Iceberg REST today has no tag
    /// endpoint — must return
    /// [`crate::CatalogError::UnsupportedOperation`]; callers typically
    /// fall back to an adapter-specific SQL DDL path on that signal.
    async fn tag_table(&self, table: &TableRef, key: &str, value: &str) -> CatalogResult<()>;

    /// List the permission grants currently in effect for `table`.
    ///
    /// Catalogs that do not expose RBAC over REST must return
    /// [`crate::CatalogError::UnsupportedOperation`]. The [`Grant`] shape
    /// here is the lowest common denominator; richer catalog-specific
    /// grant models stay in adapter crates.
    async fn get_grants(&self, table: &TableRef) -> CatalogResult<Vec<Grant>>;

    /// Apply `grant` to `table`.
    ///
    /// Catalogs that do not expose grant mutation over REST must return
    /// [`crate::CatalogError::UnsupportedOperation`]. Implementations
    /// should be idempotent: re-applying a grant that already exists must
    /// return `Ok(())`.
    async fn apply_grant(&self, table: &TableRef, grant: &Grant) -> CatalogResult<()>;

    /// Revoke `grant` from `table`.
    ///
    /// Catalogs that do not expose grant mutation over REST must return
    /// [`crate::CatalogError::UnsupportedOperation`]. Implementations
    /// should be idempotent: revoking a grant that does not exist must
    /// return `Ok(())`.
    async fn revoke_grant(&self, table: &TableRef, grant: &Grant) -> CatalogResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::InMemoryCatalogClient;
    use crate::types::{ColumnSchema, TableRef, TableSchema};

    fn sample_table() -> TableRef {
        TableRef {
            catalog: Some("cat".into()),
            namespace: vec!["analytics".into()],
            name: "orders".into(),
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    type_str: "long".into(),
                    nullable: false,
                },
                ColumnSchema {
                    name: "amount".into(),
                    type_str: "decimal(10,2)".into(),
                    nullable: true,
                },
            ],
        }
    }

    /// Exercise the trait through a `dyn CatalogClient` reference — this
    /// fails to compile if the trait stops being object-safe.
    #[tokio::test]
    async fn trait_is_object_safe() {
        let client: Box<dyn CatalogClient> = Box::new(InMemoryCatalogClient::new());
        client
            .create_table(&sample_table(), &sample_schema())
            .await
            .expect("create_table should succeed on empty stub");

        let schema = client
            .describe_table(&sample_table())
            .await
            .expect("describe_table should resolve a just-created table");
        assert_eq!(schema, sample_schema());
    }

    #[tokio::test]
    async fn list_tables_returns_registered_tables() {
        let client = InMemoryCatalogClient::new();
        let table = sample_table();
        client
            .create_table(&table, &sample_schema())
            .await
            .expect("create_table");

        let listed = client
            .list_tables(&["analytics".to_string()])
            .await
            .expect("list_tables");
        // The in-memory stub is single-catalog so it strips the optional
        // catalog field on listing; the namespace + name must match.
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].namespace, table.namespace);
        assert_eq!(listed[0].name, table.name);
    }

    #[tokio::test]
    async fn describe_unknown_table_is_not_found() {
        let client = InMemoryCatalogClient::new();
        let err = client.describe_table(&sample_table()).await.unwrap_err();
        assert!(
            matches!(err, crate::CatalogError::TableNotFound(_)),
            "expected TableNotFound, got {err:?}"
        );
    }

    #[tokio::test]
    async fn governance_methods_are_unsupported_on_stub() {
        let client = InMemoryCatalogClient::new();
        let table = sample_table();
        client
            .create_table(&table, &sample_schema())
            .await
            .expect("create_table");

        let tag_err = client
            .tag_table(&table, "owner", "analytics")
            .await
            .unwrap_err();
        assert!(matches!(
            tag_err,
            crate::CatalogError::UnsupportedOperation(_)
        ));

        let grants_err = client.get_grants(&table).await.unwrap_err();
        assert!(matches!(
            grants_err,
            crate::CatalogError::UnsupportedOperation(_)
        ));
    }

    #[tokio::test]
    async fn drop_table_removes_registration() {
        let client = InMemoryCatalogClient::new();
        let table = sample_table();
        client
            .create_table(&table, &sample_schema())
            .await
            .expect("create_table");

        client.drop_table(&table).await.expect("drop_table");
        let err = client.describe_table(&table).await.unwrap_err();
        assert!(matches!(err, crate::CatalogError::TableNotFound(_)));
    }
}
