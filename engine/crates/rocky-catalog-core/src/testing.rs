//! Test utilities for [`crate::CatalogClient`] — primarily an in-memory
//! stub that downstream crates can wire into their own tests.
//!
//! Enable via `features = ["testing"]` in `Cargo.toml`. The module is also
//! compiled unconditionally under `cfg(test)` for this crate's own tests.
//!
//! [`InMemoryCatalogClient`] is not a faithful catalog simulation — it
//! supports table CRUD only and returns
//! [`crate::CatalogError::UnsupportedOperation`] from governance and
//! transaction methods. Its purpose is to verify that the trait shape is
//! object-safe and ergonomic, not to drive real workloads.
//!
//! Tests that need richer behaviour (commit-conflict simulation, branch
//! manipulation, grant tracking) are expected to layer their own stubs on
//! top of [`CatalogClient`] in the crate that owns the test.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::client::CatalogClient;
use crate::error::{CatalogError, CatalogResult};
use crate::types::{BranchRef, Grant, TableCommit, TableRef, TableSchema, TableStats};

/// In-memory [`CatalogClient`] stub backed by a [`HashMap`] of
/// `(namespace, table-name)` to [`TableSchema`].
///
/// This stub is intentionally minimal:
///
/// - `create_table` / `describe_table` / `drop_table` / `list_tables`
///   behave as a naive in-memory catalog. The optional `catalog` field on
///   [`TableRef`] is ignored for keying — the stub is single-catalog.
/// - `list_branches` returns an empty vector unconditionally.
/// - `commit_transaction`, `tag_table`, and the grant methods return
///   [`CatalogError::UnsupportedOperation`].
///
/// Downstream crates that want a sharper stub should wrap or replace this
/// type rather than extending it; the trait surface is the contract.
#[derive(Debug, Default)]
pub struct InMemoryCatalogClient {
    tables: Mutex<HashMap<Key, TableSchema>>,
    /// Per-table stub stats, indexed by the same `(namespace, name)`
    /// key as [`Self::tables`]. Populated by
    /// [`InMemoryCatalogClient::set_table_stats`]; lookups return
    /// [`TableStats::empty()`] when no stats are registered, matching
    /// the "table exists but catalog reports no stats" case from real
    /// catalogs.
    stats: Mutex<HashMap<Key, TableStats>>,
}

/// Internal key used by the stub. Equivalent to `(namespace, name)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    namespace: Vec<String>,
    name: String,
}

impl Key {
    fn from_ref(table: &TableRef) -> Self {
        Self {
            namespace: table.namespace.clone(),
            name: table.name.clone(),
        }
    }
}

impl InMemoryCatalogClient {
    /// Construct an empty stub.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register stub statistics for `table`.
    ///
    /// Test helper used by downstream crates to feed deterministic
    /// stats into a cost-propagation test without standing up a real
    /// catalog. Overwrites any previously registered stats for the same
    /// `(namespace, name)` key.
    pub fn set_table_stats(&self, table: &TableRef, stats: TableStats) {
        let mut guard = self.stats.lock().expect("stats mutex poisoned");
        guard.insert(Key::from_ref(table), stats);
    }
}

#[async_trait]
impl CatalogClient for InMemoryCatalogClient {
    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableRef>> {
        let guard = self.tables.lock().expect("tables mutex poisoned");
        let mut out: Vec<TableRef> = guard
            .keys()
            .filter(|key| key.namespace == namespace)
            .map(|key| TableRef {
                catalog: None,
                namespace: key.namespace.clone(),
                name: key.name.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn describe_table(&self, table: &TableRef) -> CatalogResult<TableSchema> {
        let guard = self.tables.lock().expect("tables mutex poisoned");
        guard
            .get(&Key::from_ref(table))
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(format_table(table)))
    }

    async fn create_table(&self, table: &TableRef, schema: &TableSchema) -> CatalogResult<()> {
        let mut guard = self.tables.lock().expect("tables mutex poisoned");
        guard.insert(Key::from_ref(table), schema.clone());
        Ok(())
    }

    async fn drop_table(&self, table: &TableRef) -> CatalogResult<()> {
        let mut guard = self.tables.lock().expect("tables mutex poisoned");
        guard
            .remove(&Key::from_ref(table))
            .map(|_| ())
            .ok_or_else(|| CatalogError::TableNotFound(format_table(table)))
    }

    async fn table_stats(&self, table: &TableRef) -> CatalogResult<TableStats> {
        // The stub honours stats explicitly registered via
        // [`Self::set_table_stats`]; otherwise the table is required to
        // exist (`describe_table`-style semantics) and we return the
        // empty-stats baseline — mirrors what a catalog returns when a
        // table exists but the writer never populated the summary keys.
        let stats_guard = self.stats.lock().expect("stats mutex poisoned");
        let key = Key::from_ref(table);
        if let Some(stats) = stats_guard.get(&key) {
            return Ok(stats.clone());
        }
        drop(stats_guard);
        let tables_guard = self.tables.lock().expect("tables mutex poisoned");
        if tables_guard.contains_key(&key) {
            Ok(TableStats::empty())
        } else {
            Err(CatalogError::TableNotFound(format_table(table)))
        }
    }

    async fn commit_transaction(&self, _commits: &[TableCommit]) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "InMemoryCatalogClient does not support multi-table transactions",
        ))
    }

    async fn list_branches(&self, _table: &TableRef) -> CatalogResult<Vec<BranchRef>> {
        Ok(Vec::new())
    }

    async fn tag_table(&self, _table: &TableRef, _key: &str, _value: &str) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "InMemoryCatalogClient does not support tagging",
        ))
    }

    async fn get_grants(&self, _table: &TableRef) -> CatalogResult<Vec<Grant>> {
        Err(CatalogError::UnsupportedOperation(
            "InMemoryCatalogClient does not expose grants",
        ))
    }

    async fn apply_grant(&self, _table: &TableRef, _grant: &Grant) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "InMemoryCatalogClient does not support apply_grant",
        ))
    }

    async fn revoke_grant(&self, _table: &TableRef, _grant: &Grant) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "InMemoryCatalogClient does not support revoke_grant",
        ))
    }
}

fn format_table(table: &TableRef) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(cat) = &table.catalog {
        parts.push(cat.clone());
    }
    parts.extend(table.namespace.iter().cloned());
    parts.push(table.name.clone());
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BranchKind, ColumnSchema};

    #[test]
    fn table_ref_round_trips_through_serde() {
        let table = TableRef {
            catalog: Some("cat".into()),
            namespace: vec!["a".into(), "b".into()],
            name: "orders".into(),
        };
        let json = serde_json::to_string(&table).expect("serialize TableRef");
        let restored: TableRef = serde_json::from_str(&json).expect("deserialize TableRef");
        assert_eq!(table, restored);
    }

    #[test]
    fn branch_ref_round_trips_through_serde() {
        let branch = BranchRef {
            name: "main".into(),
            snapshot_id: Some(123),
            kind: BranchKind::Branch,
        };
        let json = serde_json::to_string(&branch).expect("serialize BranchRef");
        let restored: BranchRef = serde_json::from_str(&json).expect("deserialize BranchRef");
        assert_eq!(branch, restored);
    }

    #[test]
    fn branch_kind_serializes_as_kebab_case() {
        let json = serde_json::to_string(&BranchKind::Branch).expect("serialize");
        assert_eq!(json, "\"branch\"");
        let json = serde_json::to_string(&BranchKind::Tag).expect("serialize");
        assert_eq!(json, "\"tag\"");
    }

    #[test]
    fn grant_round_trips_through_serde() {
        let grant = Grant {
            principal: "analytics@example.com".into(),
            privilege: "SELECT".into(),
        };
        let json = serde_json::to_string(&grant).expect("serialize Grant");
        let restored: Grant = serde_json::from_str(&json).expect("deserialize Grant");
        assert_eq!(grant, restored);
    }

    #[test]
    fn table_schema_round_trips_through_serde() {
        let schema = TableSchema {
            columns: vec![ColumnSchema {
                name: "id".into(),
                type_str: "long".into(),
                nullable: false,
            }],
        };
        let json = serde_json::to_string(&schema).expect("serialize TableSchema");
        let restored: TableSchema = serde_json::from_str(&json).expect("deserialize TableSchema");
        assert_eq!(schema, restored);
    }
}
