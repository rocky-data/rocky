//! Read-path helper: load the persisted schema cache into the shape the
//! typecheck pipeline expects.
//!
//! `rocky-core::schema_cache` holds the low-level redb types
//! ([`SchemaCacheEntry`], [`StoredColumn`]) — wire-neutral, because
//! `rocky-core` does not depend on `rocky-compiler`. This module bridges
//! the gap: it reads the cache, filters out TTL-expired entries, and
//! converts each `StoredColumn` into the [`TypedColumn`] the compiler
//! consumes via `CompilerConfig.source_schemas`.
//!
//! Key-shape conversion note: the cache is keyed by
//! `"<catalog>.<schema>.<table>"` so multi-pipeline projects sharing a
//! schema across catalogs don't collide. The compiler wants plain
//! `"<schema>.<table>"` because typecheck resolves against the unqualified
//! form that SQL `FROM <schema>.<table>` produces. This loader strips the
//! catalog prefix on read. When two cached entries share the same
//! `<schema>.<table>` under different catalogs (a legitimate
//! multi-pipeline setup), the last one wins — this matches the behaviour
//! a compiler operating without catalog awareness would see from a live
//! `batch_describe_schema` call today.
//!
//! Arc 7 wave 2 wave-2 PR 1a: this module is ready to be called but has
//! no callers yet. PR 1b replaces the 10 `HashMap::new()` callsites with
//! `load_source_schemas_from_cache(...)` calls.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use rocky_core::schema_cache::StoredColumn;
use rocky_core::state::StateStore;

use crate::compile::default_type_mapper;
use crate::types::TypedColumn;

/// Load cached source schemas as the `TypedColumn`-shaped map the typecheck
/// pipeline expects.
///
/// - Reads every entry from `state.redb`'s `schema_cache` table.
/// - Drops entries whose `cached_at + ttl < now`.
/// - Converts each [`StoredColumn`] into a [`TypedColumn`] via
///   [`default_type_mapper`] — the same mapper `compile.rs`'s
///   `load_source_schemas_from_seed` uses (wave-1), so seed and cache paths
///   produce type-compatible maps.
/// - Rekeys `"<catalog>.<schema>.<table>" -> "<schema>.<table>"` so the
///   result drops straight into `CompilerConfig.source_schemas` without
///   further massaging.
///
/// Returns an empty map when the cache is cold or every entry is expired
/// — identical to the `HashMap::new()` the current callsites pass. That
/// keeps the typecheck degrading-to-`Unknown` fallback at cold start, so
/// wiring this in is a pure no-op on a fresh install.
///
/// # Errors
///
/// Returns the underlying [`rocky_core::state::StateError`] wrapped via
/// `anyhow::Error` when the cache scan fails (corrupt DB, I/O error, JSON
/// deserialization). The compiler's CLI callsites already return
/// `anyhow::Result`, so this signature slots in without error-type
/// conversions.
pub fn load_source_schemas_from_cache(
    state: &StateStore,
    now: DateTime<Utc>,
    ttl: Duration,
) -> anyhow::Result<HashMap<String, Vec<TypedColumn>>> {
    let entries = state.list_schema_cache()?;
    let mut out: HashMap<String, Vec<TypedColumn>> = HashMap::with_capacity(entries.len());

    for (key, entry) in entries {
        if entry.is_expired(now, ttl) {
            continue;
        }
        let Some(compiler_key) = strip_catalog_prefix(&key) else {
            // Malformed key ("<catalog>.<schema>.<table>" requires two
            // dots). Skip rather than error — a hand-edited or
            // future-format key shouldn't take down the whole compile.
            continue;
        };
        let typed = entry
            .columns
            .into_iter()
            .map(stored_column_to_typed_column)
            .collect();
        out.insert(compiler_key, typed);
    }

    Ok(out)
}

/// Translate `"<catalog>.<schema>.<table>"` -> `"<schema>.<table>"`.
///
/// Returns `None` when the key has fewer than two dots — that's a shape
/// the write path shouldn't produce, but a cached DB from an older format
/// could contain. Silent skip (see [`load_source_schemas_from_cache`]) is
/// the right move because typecheck degrading to `Unknown` for one source
/// is strictly less bad than failing the whole compile on a malformed key.
fn strip_catalog_prefix(cache_key: &str) -> Option<String> {
    let first_dot = cache_key.find('.')?;
    let rest = &cache_key[first_dot + 1..];
    // Ensure the remainder has at least one more dot — "<schema>.<table>".
    if !rest.contains('.') {
        return None;
    }
    Some(rest.to_string())
}

fn stored_column_to_typed_column(col: StoredColumn) -> TypedColumn {
    TypedColumn {
        name: col.name,
        data_type: default_type_mapper(&col.data_type),
        nullable: col.nullable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::schema_cache::{SchemaCacheEntry, schema_cache_key};
    use tempfile::TempDir;

    use crate::types::RockyType;

    fn temp_state() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        (store, dir)
    }

    fn seed(
        store: &StateStore,
        catalog: &str,
        schema: &str,
        table: &str,
        cached_at: DateTime<Utc>,
    ) {
        let key = schema_cache_key(catalog, schema, table);
        let entry = SchemaCacheEntry {
            columns: vec![
                StoredColumn {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                },
                StoredColumn {
                    name: "email".into(),
                    data_type: "STRING".into(),
                    nullable: true,
                },
            ],
            cached_at,
        };
        store.write_schema_cache_entry(&key, &entry).unwrap();
    }

    #[test]
    fn empty_cache_returns_empty_map() {
        let (store, _dir) = temp_state();
        let now = Utc::now();
        let ttl = Duration::hours(24);

        let map = load_source_schemas_from_cache(&store, now, ttl).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn valid_entry_converts_to_typed_columns() {
        let (store, _dir) = temp_state();
        let now = Utc::now();
        seed(&store, "cat", "staging", "orders", now);

        let map = load_source_schemas_from_cache(&store, now, Duration::hours(24)).unwrap();
        assert_eq!(map.len(), 1);
        let cols = map
            .get("staging.orders")
            .expect("catalog stripped from key");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, RockyType::Int64); // default_type_mapper("BIGINT")
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "email");
        assert_eq!(cols[1].data_type, RockyType::String);
        assert!(cols[1].nullable);
    }

    #[test]
    fn expired_entry_is_filtered_out() {
        let (store, _dir) = temp_state();
        let now = Utc::now();
        // Seed with an old cached_at so it's past TTL.
        seed(&store, "cat", "s", "t", now - Duration::hours(48));

        let map = load_source_schemas_from_cache(&store, now, Duration::hours(24)).unwrap();
        assert!(
            map.is_empty(),
            "stale entry should be filtered, got {map:?}"
        );
    }

    #[test]
    fn mix_of_fresh_and_expired_surfaces_only_fresh() {
        let (store, _dir) = temp_state();
        let now = Utc::now();
        seed(&store, "cat", "s", "fresh", now);
        seed(&store, "cat", "s", "stale", now - Duration::hours(48));

        let map = load_source_schemas_from_cache(&store, now, Duration::hours(24)).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("s.fresh"));
        assert!(!map.contains_key("s.stale"));
    }

    #[test]
    fn catalog_stripped_on_read() {
        let (store, _dir) = temp_state();
        let now = Utc::now();
        seed(&store, "ProdCat", "Staging", "Orders", now);

        let map = load_source_schemas_from_cache(&store, now, Duration::hours(24)).unwrap();
        assert!(map.contains_key("staging.orders"));
        assert!(!map.contains_key("prodcat.staging.orders"));
    }

    #[test]
    fn strip_catalog_prefix_requires_two_dots() {
        assert_eq!(
            strip_catalog_prefix("cat.schema.table").as_deref(),
            Some("schema.table")
        );
        // One-dot keys are skipped by the read path — this was legal in
        // earlier prototypes where some surfaces used "<schema>.<table>"
        // directly. Today's write path always produces three components.
        assert!(strip_catalog_prefix("only.one").is_none());
        assert!(strip_catalog_prefix("nope").is_none());
    }

    #[test]
    fn unknown_type_maps_to_rocky_type_unknown() {
        // Sanity: the mapper passes through unrecognised types as Unknown,
        // which is how the compiler already handles a missing type today.
        let col = StoredColumn {
            name: "weird".into(),
            data_type: "PURPLE_MONKEY".into(),
            nullable: true,
        };
        let typed = stored_column_to_typed_column(col);
        assert_eq!(typed.data_type, RockyType::Unknown);
    }
}
