//! Persisted cache of warehouse `DESCRIBE TABLE` results.
//!
//! The cache is the mechanism that lets `rocky compile` / `rocky lsp`
//! typecheck leaf models against real warehouse column types without
//! paying a live `DESCRIBE` round-trip on every call. A `rocky run` or
//! `rocky discover --with-schemas` writes here on success; the compiler
//! loads a TTL-filtered snapshot on every subsequent compile.
//!
//! This module deliberately holds only the redb-persistable types. The
//! `TypedColumn`-shaped loader lives in `rocky-compiler::schema_cache` —
//! `rocky-core` does not depend on `rocky-compiler`, so the compiler-side
//! wrapper owns the `ColumnInfo`/`TypedColumn` conversion.
//!
//! Value shape in `state.redb`: `serde_json::to_vec(&SchemaCacheEntry)` —
//! matches the encoding every other table in the state store uses
//! (watermarks, run history, DAG snapshots, ...). Keys are
//! `"<catalog>.<schema>.<table>"` lowercased so multi-pipeline projects
//! sharing a source across catalogs don't collide (see
//! [`schema_cache_key`]).
//!
//! Replication posture: the `SCHEMA_CACHE` table defaults to
//! `replicate = false` in `state_sync` — a dev cloning a fresh checkout
//! should warm their local cache from their own `rocky run`, not inherit
//! another machine's stale entries. See the design doc §5.7.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// A cached `DESCRIBE TABLE` result for a single source table.
///
/// Persisted in `state.redb` under the `SCHEMA_CACHE` table. The columns
/// are stored as [`StoredColumn`] — adapter-neutral strings — so that
/// `rocky-core` does not need to depend on `rocky-compiler`'s `RockyType`.
/// The compiler-side loader converts these into `TypedColumn` via the
/// existing `default_type_mapper` before typecheck sees them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCacheEntry {
    /// The columns as reported by the warehouse, in `information_schema`
    /// order.
    pub columns: Vec<StoredColumn>,
    /// When the entry was written. Read path compares this against the
    /// configured TTL to decide whether the entry is a hit.
    pub cached_at: DateTime<Utc>,
}

/// Adapter-neutral column metadata.
///
/// Stored as strings so `rocky-core` doesn't need to depend on
/// `rocky-compiler`'s `RockyType`. Downstream code in `rocky-compiler`
/// converts these into `TypedColumn` via the existing `default_type_mapper`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredColumn {
    /// Column name as reported by the warehouse.
    pub name: String,
    /// Raw warehouse type string (e.g. `"BIGINT"`, `"DECIMAL(10,2)"`,
    /// `"STRING"`). Interpreted by the `default_type_mapper` at read time.
    pub data_type: String,
    /// `true` if the column is nullable in the source.
    pub nullable: bool,
}

impl SchemaCacheEntry {
    /// Returns `true` when `now - cached_at > ttl`.
    ///
    /// Used by the read path to filter stale entries out of the loaded
    /// map before handing it to the compiler. Expiry is checked at read
    /// time, not at write time, because the TTL is a config surface the
    /// user can tune without needing the cache to be re-stamped.
    pub fn is_expired(&self, now: DateTime<Utc>, ttl: Duration) -> bool {
        now.signed_duration_since(self.cached_at) > ttl
    }
}

/// Compose the canonical redb key for a schema cache entry.
///
/// Shape: `"<catalog>.<schema>.<table>"`, with every component lowercased.
/// The catalog is included so two pipelines sharing a schema+table name
/// across distinct catalogs (e.g. `prod.staging.orders` vs
/// `sandbox.staging.orders`) don't clobber each other. The compiler-side
/// loader strips the catalog prefix on read because typecheck resolves
/// against the plain `<schema>.<table>` form that SQL `FROM` clauses
/// produce.
pub fn schema_cache_key(catalog: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        catalog.to_ascii_lowercase(),
        schema.to_ascii_lowercase(),
        table.to_ascii_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_at(cached_at: DateTime<Utc>) -> SchemaCacheEntry {
        SchemaCacheEntry {
            columns: vec![StoredColumn {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }],
            cached_at,
        }
    }

    #[test]
    fn is_expired_true_when_older_than_ttl() {
        let cached = Utc::now() - Duration::hours(25);
        let entry = entry_at(cached);
        let now = Utc::now();
        let ttl = Duration::hours(24);
        assert!(entry.is_expired(now, ttl));
    }

    #[test]
    fn is_expired_false_when_fresh() {
        let cached = Utc::now() - Duration::hours(1);
        let entry = entry_at(cached);
        let now = Utc::now();
        let ttl = Duration::hours(24);
        assert!(!entry.is_expired(now, ttl));
    }

    #[test]
    fn is_expired_false_at_exact_boundary() {
        // `>` (strict), not `>=`: an entry exactly at the TTL boundary is
        // still a hit. Matches the anomaly-threshold convention at
        // `state.rs:test_anomaly_threshold_boundary`.
        let now = Utc::now();
        let ttl = Duration::hours(24);
        let cached = now - ttl;
        let entry = entry_at(cached);
        assert!(!entry.is_expired(now, ttl));
    }

    #[test]
    fn schema_cache_key_lowercases_and_joins() {
        assert_eq!(
            schema_cache_key("ProdCat", "Staging", "Orders"),
            "prodcat.staging.orders"
        );
    }

    #[test]
    fn schema_cache_key_round_trips_ascii() {
        // Key is purely lowercased ASCII — no quoting, no normalisation
        // beyond ascii lowercase. Non-ASCII names are left to upstream
        // identifier validation (rocky-sql).
        assert_eq!(schema_cache_key("c", "s", "t"), "c.s.t");
    }
}
