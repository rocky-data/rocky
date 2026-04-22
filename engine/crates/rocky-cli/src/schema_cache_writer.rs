//! Arc 7 wave 2 wave-2 PR 2 — `rocky run` write tap for the schema cache.
//!
//! Sibling to `source_schemas.rs` (the read helper wired in PR 1b). The two
//! ends of the cache share the same `SchemaCacheConfig` gate, key shape, and
//! `state.redb` table — only the direction of travel differs.
//!
//! The write tap hangs off `rocky run`'s batch `DESCRIBE TABLE` pass. Any code
//! path that already fetches columns via
//! `BatchCheckAdapter::batch_describe_schema` passes the results through
//! [`persist_batch_describe`] so downstream compiles (and the LSP's per-keystroke
//! typecheck) can resolve leaf `FROM <schema>.<table>` references against real
//! warehouse types without a round-trip.
//!
//! **Best-effort only.** A cache write that fails for any reason must not
//! fail the surrounding `rocky run` — the cache is observability plumbing,
//! not a hard dependency. Failures are logged at `warn!` level with enough
//! context to debug (the key that failed + the error) and execution
//! continues.
//!
//! Dedup within one run is by the composed `schema_cache_key` so the same
//! `(catalog, schema, table)` triple — if reached more than once inside a
//! single `rocky run` — pays redb's write cost exactly once. Databricks
//! already de-duplicates at the `(catalog, schema)` pair level via
//! `batch_describe_schema`, but declaring the tighter guarantee at the
//! cache-writer layer keeps the invariant local when PR 3's
//! `rocky discover --with-schemas` reuses this helper.

use std::collections::HashSet;

use chrono::Utc;
use tracing::{debug, warn};

use rocky_core::config::SchemaCacheConfig;
use rocky_core::ir::ColumnInfo;
use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
use rocky_core::state::StateStore;

/// State tracked across a single `rocky run` so repeated writes of the same
/// `(catalog, schema, table)` key are suppressed after the first successful
/// persist. Constructed once at the start of the run, threaded through every
/// describe-tap call, dropped at end of run.
#[derive(Default)]
pub(crate) struct SchemaCacheWriteTap {
    /// Keys already persisted during this run. Composed via
    /// [`schema_cache_key`] so lookups match what [`persist_batch_describe`]
    /// writes into redb.
    seen: HashSet<String>,
}

impl SchemaCacheWriteTap {
    /// Number of keys persisted so far. Used by tests and for the
    /// once-per-run summary log; callers should not rely on this during
    /// the run itself.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.seen.len()
    }
}

/// Persist the columns returned by a single `batch_describe_schema` call.
///
/// - `cols_by_table` is exactly what `BatchCheckAdapter::batch_describe_schema`
///   returns: `HashMap<lowercased_table_name, Vec<ColumnInfo>>`.
/// - Writes every table in `cols_by_table`, not just the ones the current run
///   touches. The `DESCRIBE` cost was paid for the whole schema; populating
///   the cache for sibling tables is free signal for future compiles that
///   reference them (e.g. a different `--filter` or a downstream model that
///   joins another table in the same source schema).
/// - Gates on `config.enabled`. When `false`, the helper short-circuits to a
///   no-op before touching redb.
/// - Deduplicates via `tap.seen`. A key that's already been persisted in this
///   run is skipped.
/// - On write failure, logs `warn!` and continues. A failed entry is not
///   inserted into `tap.seen` so a later retry has a chance to succeed if
///   the underlying cause was transient.
pub(crate) fn persist_batch_describe(
    store: &StateStore,
    config: &SchemaCacheConfig,
    tap: &mut SchemaCacheWriteTap,
    catalog: &str,
    schema: &str,
    cols_by_table: &std::collections::HashMap<String, Vec<ColumnInfo>>,
) {
    if !config.enabled {
        return;
    }

    let now = Utc::now();
    for (table, cols) in cols_by_table {
        let key = schema_cache_key(catalog, schema, table);
        if tap.seen.contains(&key) {
            continue;
        }

        let entry = SchemaCacheEntry {
            columns: cols.iter().map(column_info_to_stored).collect(),
            cached_at: now,
        };

        match store.write_schema_cache_entry(&key, &entry) {
            Ok(()) => {
                tap.seen.insert(key);
            }
            Err(e) => {
                warn!(
                    target: "rocky::schema_cache",
                    error = %e,
                    key = %key,
                    "schema cache write failed; cache will miss for this source on next compile"
                );
            }
        }
    }

    debug!(
        target: "rocky::schema_cache",
        catalog,
        schema,
        tables = cols_by_table.len(),
        persisted_total = tap.seen.len(),
        "persisted batch describe into schema cache"
    );
}

/// Convert an adapter-reported column into the redb-persistable shape.
/// Both types are `(name, data_type, nullable)` triples today; keeping the
/// conversion explicit makes it obvious when either side adds a field.
fn column_info_to_stored(col: &ColumnInfo) -> StoredColumn {
    StoredColumn {
        name: col.name.clone(),
        data_type: col.data_type.clone(),
        nullable: col.nullable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn cols(rows: &[(&str, &str, bool)]) -> Vec<ColumnInfo> {
        rows.iter()
            .map(|(n, t, nullable)| ColumnInfo {
                name: (*n).to_string(),
                data_type: (*t).to_string(),
                nullable: *nullable,
            })
            .collect()
    }

    fn batch(entries: &[(&str, Vec<ColumnInfo>)]) -> HashMap<String, Vec<ColumnInfo>> {
        entries
            .iter()
            .map(|(t, c)| ((*t).to_string(), c.clone()))
            .collect()
    }

    #[test]
    fn writes_entry_when_enabled() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        let config = SchemaCacheConfig::default();
        let mut tap = SchemaCacheWriteTap::default();
        let batch = batch(&[("orders", cols(&[("id", "BIGINT", false)]))]);

        persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &batch);

        let key = schema_cache_key("cat", "staging", "orders");
        let fetched = store.read_schema_cache_entry(&key).unwrap().unwrap();
        assert_eq!(fetched.columns.len(), 1);
        assert_eq!(fetched.columns[0].name, "id");
        assert_eq!(fetched.columns[0].data_type, "BIGINT");
        assert!(!fetched.columns[0].nullable);
        assert_eq!(tap.len(), 1);
    }

    #[test]
    fn writes_nothing_when_disabled() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        let config = SchemaCacheConfig {
            enabled: false,
            ttl_seconds: 86_400,
            replicate: false,
        };
        let mut tap = SchemaCacheWriteTap::default();
        let batch = batch(&[("orders", cols(&[("id", "BIGINT", false)]))]);

        persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &batch);

        let key = schema_cache_key("cat", "staging", "orders");
        assert!(store.read_schema_cache_entry(&key).unwrap().is_none());
        assert_eq!(tap.len(), 0);
    }

    #[test]
    fn dedups_repeated_key_within_one_run() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        let config = SchemaCacheConfig::default();
        let mut tap = SchemaCacheWriteTap::default();

        // First call writes.
        let first_batch = batch(&[("orders", cols(&[("id", "BIGINT", false)]))]);
        persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &first_batch);

        // Second call with the same key should be skipped by the dedup set —
        // evidence: overwriting with a different column list has no effect
        // on what's in redb.
        let second_batch = batch(&[(
            "orders",
            cols(&[("id", "STRING", true), ("new_col", "INT", true)]),
        )]);
        persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &second_batch);

        let key = schema_cache_key("cat", "staging", "orders");
        let fetched = store.read_schema_cache_entry(&key).unwrap().unwrap();
        assert_eq!(
            fetched.columns.len(),
            1,
            "second write should be dedup-suppressed"
        );
        assert_eq!(fetched.columns[0].data_type, "BIGINT");
        assert_eq!(tap.len(), 1);
    }

    #[test]
    fn writes_all_tables_in_batch_not_just_selected() {
        // `batch_describe_schema(cat, sch)` returns the full schema — every
        // table in it becomes a cache entry so future compiles that filter
        // to a sibling table also benefit from the already-paid DESCRIBE.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        let config = SchemaCacheConfig::default();
        let mut tap = SchemaCacheWriteTap::default();
        let batch = batch(&[
            ("orders", cols(&[("id", "BIGINT", false)])),
            ("customers", cols(&[("cust_id", "STRING", false)])),
            ("line_items", cols(&[("line_id", "BIGINT", false)])),
        ]);

        persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &batch);

        let all = store.list_schema_cache().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(tap.len(), 3);
        for table in ["orders", "customers", "line_items"] {
            let key = schema_cache_key("cat", "staging", table);
            assert!(
                store.read_schema_cache_entry(&key).unwrap().is_some(),
                "missing key {key}"
            );
        }
    }

    #[test]
    fn distinct_catalogs_do_not_collide() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        let config = SchemaCacheConfig::default();
        let mut tap = SchemaCacheWriteTap::default();
        let batch_prod = batch(&[("orders", cols(&[("id", "BIGINT", false)]))]);
        let batch_sandbox = batch(&[("orders", cols(&[("id", "STRING", true)]))]);

        persist_batch_describe(&store, &config, &mut tap, "prod", "staging", &batch_prod);
        persist_batch_describe(
            &store,
            &config,
            &mut tap,
            "sandbox",
            "staging",
            &batch_sandbox,
        );

        let prod_key = schema_cache_key("prod", "staging", "orders");
        let sandbox_key = schema_cache_key("sandbox", "staging", "orders");
        let prod_entry = store.read_schema_cache_entry(&prod_key).unwrap().unwrap();
        let sandbox_entry = store
            .read_schema_cache_entry(&sandbox_key)
            .unwrap()
            .unwrap();

        assert_eq!(prod_entry.columns[0].data_type, "BIGINT");
        assert_eq!(sandbox_entry.columns[0].data_type, "STRING");
        assert_eq!(tap.len(), 2);
    }

    #[test]
    fn round_trip_through_reader() {
        // End-to-end check: write via `persist_batch_describe`, then read via
        // the same loader PR 1b plugged into every CLI compile callsite.
        // Protects the writer/reader contract from drifting — e.g. if the
        // key composition ever diverged between ends, this test would fail.
        use crate::source_schemas::load_cached_source_schemas;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            let config = SchemaCacheConfig::default();
            let mut tap = SchemaCacheWriteTap::default();
            let batch = batch(&[("orders", cols(&[("id", "BIGINT", false)]))]);
            persist_batch_describe(&store, &config, &mut tap, "cat", "staging", &batch);
        }

        let config = SchemaCacheConfig::default();
        let map = load_cached_source_schemas(&config, &path);
        assert_eq!(map.len(), 1);
        // Reader drops the catalog prefix — typecheck resolves against
        // `FROM staging.orders`, not `FROM cat.staging.orders`.
        let typed = map.get("staging.orders").expect("missing typed schema");
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].name, "id");
    }

    #[test]
    fn signature_does_not_propagate_errors() {
        // Type-level guarantee: `persist_batch_describe` returns `()`, not a
        // `Result`. The best-effort contract — "a cache-write failure must
        // never fail the run" — is enforced by the signature itself, so no
        // caller can accidentally propagate. This test would fail to compile
        // if the return type were ever changed to `Result<(), _>`.
        fn _assert_unit_return<F>(_: F)
        where
            F: Fn(
                &StateStore,
                &SchemaCacheConfig,
                &mut SchemaCacheWriteTap,
                &str,
                &str,
                &HashMap<String, Vec<ColumnInfo>>,
            ),
        {
        }
        _assert_unit_return(persist_batch_describe);
    }
}
