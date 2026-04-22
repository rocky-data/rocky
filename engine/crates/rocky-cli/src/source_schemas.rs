//! Shared helper for loading the Arc 7 wave 2 wave-2 schema cache into the
//! typecheck pipeline.
//!
//! Every `rocky <verb>` whose internal flow calls
//! `rocky_compiler::compile::compile` used to pass
//! `source_schemas: HashMap::new()` — which forces the typechecker to treat
//! every leaf `FROM <schema>.<table>` as `RockyType::Unknown`. Wave-1 seeded
//! that map from DuckDB (`--with-seed`); wave-2 seeds it from a persisted
//! redb cache written by prior `rocky run` / `rocky discover --with-schemas`
//! invocations (PRs 2-3).
//!
//! This helper collapses the read-path boilerplate — config gating, optional
//! state-store open, TTL application, log emission — into one call. PR 1b
//! wires every CLI callsite to it. PR 1a infrastructure (`rocky-core::
//! schema_cache`, `StateStore::list_schema_cache`,
//! `rocky-compiler::schema_cache::load_source_schemas_from_cache`) is the
//! load-bearing plumbing below.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use chrono::Utc;
use tracing::{debug, info};

use rocky_compiler::schema_cache::load_source_schemas_from_cache;
use rocky_compiler::types::TypedColumn;
use rocky_core::config::SchemaCacheConfig;
use rocky_core::state::StateStore;

/// Once-per-process guard for the "N/M sources hit" info log emitted from
/// CLI commands.
///
/// CLI processes are short-lived, so guarding on the process is equivalent
/// to "once per invocation." Quiet by default — the log fires only when
/// `load_source_schemas_from_cache` returns at least one entry but fewer
/// than the caller expected (mixed hit/miss case). Cold cache = no log,
/// full-hit cache = no log. See the design doc §5.6 for the rationale.
static CLI_PARTIAL_HIT_LOGGED: OnceLock<()> = OnceLock::new();

/// Load `source_schemas` for a CLI compile, honouring `[cache.schemas]`.
///
/// Precedence (matches the design doc §4.4):
/// 1. `config.enabled == false` -> empty map (strict-CI posture).
/// 2. `state_path` doesn't exist -> empty map (fresh clone, cold cache).
///    Does **not** create `state.redb` as a side effect — calling
///    `StateStore::open_read_only` on a non-existent path would call
///    `Database::create` and leave a fresh empty file behind for any user
///    who runs `rocky compile` before their first `rocky run`.
/// 3. Open fails (corrupt DB, permission error) -> empty map + debug log.
///    A broken cache should never fail typecheck.
/// 4. Scan fails (rare — version mismatch on a table we didn't create
///    here) -> empty map + debug log.
/// 5. Otherwise -> the TTL-filtered map from
///    [`load_source_schemas_from_cache`].
///
/// Emits an [`tracing::info!`] line once per CLI process (via
/// [`CLI_PARTIAL_HIT_LOGGED`]) when the scan returns at least one entry —
/// gives visibility into staleness without per-compile noise.
///
/// The target `rocky::schema_cache` keeps the log easy to filter in
/// deployments that ship structured JSON logs.
pub(crate) fn load_cached_source_schemas(
    config: &SchemaCacheConfig,
    state_path: &Path,
) -> HashMap<String, Vec<TypedColumn>> {
    if !config.enabled {
        return HashMap::new();
    }

    // Gate on existence so we don't create an empty `state.redb` as a side
    // effect of `rocky compile` on a fresh checkout.
    if !state_path.exists() {
        return HashMap::new();
    }

    // `open_read_only` doesn't take the advisory write lock, so concurrent
    // `rocky run` writers are unaffected. This matters for CI systems that
    // run `rocky compile` and `rocky run` side by side.
    let store = match StateStore::open_read_only(state_path) {
        Ok(s) => s,
        Err(e) => {
            debug!(
                error = %e,
                path = %state_path.display(),
                "schema cache: state store open failed; degrading to Unknown types"
            );
            return HashMap::new();
        }
    };

    let map = match load_source_schemas_from_cache(&store, Utc::now(), config.ttl()) {
        Ok(m) => m,
        Err(e) => {
            debug!(
                error = %e,
                "schema cache: scan failed; degrading to Unknown types"
            );
            return HashMap::new();
        }
    };

    if !map.is_empty() && CLI_PARTIAL_HIT_LOGGED.set(()).is_ok() {
        info!(
            target: "rocky::schema_cache",
            sources_hit = map.len(),
            "schema cache: {} source(s) hit — run `rocky run` (write tap lands in PR 2) or \
             `rocky discover --with-schemas` (PR 3) to warm-cache more sources",
            map.len(),
        );
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration};
    use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use tempfile::TempDir;

    fn seed_cache(store: &StateStore, schema: &str, table: &str, cached_at: DateTime<Utc>) {
        let key = schema_cache_key("cat", schema, table);
        let entry = SchemaCacheEntry {
            columns: vec![StoredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            cached_at,
        };
        store.write_schema_cache_entry(&key, &entry).unwrap();
    }

    #[test]
    fn returns_empty_when_config_disabled() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "staging", "orders", Utc::now());
        }

        let config = SchemaCacheConfig {
            enabled: false,
            ttl_seconds: 86_400,
            replicate: false,
        };
        let map = load_cached_source_schemas(&config, &path);
        assert!(map.is_empty(), "disabled config must short-circuit");
    }

    #[test]
    fn returns_empty_when_state_path_missing() {
        let tmp = TempDir::new().unwrap();
        // Intentionally do NOT create state.redb — simulate fresh clone.
        let path = tmp.path().join("state.redb");
        assert!(!path.exists());

        let config = SchemaCacheConfig::default();
        let map = load_cached_source_schemas(&config, &path);
        assert!(map.is_empty());
        // Critical: must not have been created as a side effect.
        assert!(!path.exists(), "loader must not create state.redb");
    }

    #[test]
    fn returns_cached_entries_when_present() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "staging", "orders", Utc::now());
            seed_cache(&store, "staging", "customers", Utc::now());
        }

        let config = SchemaCacheConfig::default();
        let map = load_cached_source_schemas(&config, &path);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("staging.orders"));
        assert!(map.contains_key("staging.customers"));
    }

    #[test]
    fn filters_stale_entries_past_ttl() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            // Cached 48h ago -> stale at 24h TTL.
            seed_cache(
                &store,
                "staging",
                "orders",
                Utc::now() - Duration::hours(48),
            );
        }

        let config = SchemaCacheConfig::default();
        let map = load_cached_source_schemas(&config, &path);
        assert!(map.is_empty(), "stale entry must be filtered");
    }
}
