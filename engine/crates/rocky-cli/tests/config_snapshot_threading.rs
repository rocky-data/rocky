//! WP-01 PR-B integration tests: fingerprint-at-load `LoadedConfig` + `Arc`
//! config threading (#1120 config-swap TOCTOU).
//!
//! What is pinned here:
//! - `run()` executes the CALLER'S loaded snapshot, not the on-disk file: a
//!   `rocky.toml` swapped to a materially-different config B after config A
//!   was loaded neither redirects execution nor changes the recorded
//!   `RunRecord::config_hash`. RED before this stage: `run()` re-read the
//!   config path internally (the old run.rs main load) and fingerprinted the
//!   path again at the gate, so the swapped B was what executed and what was
//!   stamped.
//! - the load-time fingerprint is byte-identical to the path-based
//!   `output::config_fingerprint` for an unswapped file (parity pin), so
//!   `RunRecord.config_hash` history stays comparable across the change.

#![cfg(feature = "duckdb")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Seed `raw__acme.orders` in the persistent DuckDB file, dropping the
/// connection before returning so `rocky run` can reopen the file.
async fn seed_source(db: &Path) {
    let a = DuckDbWarehouseAdapter::open(db).expect("seed open");
    a.execute_statement("CREATE SCHEMA IF NOT EXISTS raw__acme")
        .await
        .unwrap();
    a.execute_statement("DROP TABLE IF EXISTS raw__acme.orders")
        .await
        .unwrap();
    a.execute_statement("CREATE TABLE raw__acme.orders AS SELECT 1 AS id, 'widget' AS name")
        .await
        .unwrap();
}

/// A minimal local-state DuckDB replication project (same fixture shape as
/// `remote_state_authority.rs`, without the remote `[state]` rig). The target
/// schema template is parameterized so config "A" and config "B" resolve
/// materially different destinations.
fn config_body(db: &Path, schema_prefix: &str) -> String {
    format!(
        r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.poc]
strategy = "full_refresh"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
catalog_template = "wh"
schema_template = "{schema_prefix}__{{source}}"

[pipeline.poc.target.governance]
auto_create_schemas = true
"#,
        db = db.display()
    )
}

/// Count tables named `orders` under `schema` in the warehouse file.
async fn orders_tables_in_schema(db: &Path, schema: &str) -> i64 {
    let adapter = DuckDbWarehouseAdapter::open(db).expect("verify open");
    let conn = adapter.shared_connector();
    let guard = conn.lock().unwrap();
    let out = guard
        .execute_sql(&format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = '{schema}' AND table_name = 'orders'"
        ))
        .expect("information_schema query");
    let cell = &out.rows[0][0];
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("expected integer cell, got {cell:?}"))
}

/// Swap drill (#1120): load config A fingerprinted, overwrite `rocky.toml`
/// with a materially-different config B, then call
/// `run(config_path, Arc(A), …)`. The run must execute A's config — A's
/// target schema is materialized, B's is not — and must stamp
/// `RunRecord.config_hash` with A's load-time fingerprint, not a re-read of
/// the (now-B) path.
///
/// RED before this stage on BOTH assertions: `run()`'s internal re-load
/// picked B (so `swapped__acme` was built) and the gate-path
/// `config_fingerprint(config_path)` re-read stamped fp(B).
#[tokio::test]
async fn run_executes_the_loaded_snapshot() {
    let dir = tempfile::tempdir().expect("create project temp dir");
    let db = dir.path().join("wh.duckdb");
    seed_source(&db).await;
    let config_path = dir.path().join("rocky.toml");
    let state_path = dir.path().join(".rocky-state.redb");

    // Load config A (fingerprinted) — the snapshot the caller gates on.
    std::fs::write(&config_path, config_body(&db, "staging")).expect("write config A");
    let loaded_a = Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(&config_path).expect("load config A"),
    );

    // SWAP: overwrite the on-disk file with a materially-different config B
    // (a different target schema template) before execution begins.
    std::fs::write(&config_path, config_body(&db, "swapped")).expect("write config B");
    let fp_b = rocky_cli::output::config_fingerprint(&config_path);
    assert_ne!(
        loaded_a.fingerprint, fp_b,
        "sanity: A and B must have different fingerprints"
    );

    // Execute with the THREADED snapshot A.
    rocky_cli::commands::run(
        &config_path,
        Arc::clone(&loaded_a),
        None, // filter
        None, // pipeline_name_arg — single pipeline resolves
        &state_path,
        None,  // governance_override
        false, // output_json
        None,  // models_dir
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &rocky_cli::commands::PartitionRunOptions::default(),
        None, // model_name_filter
        None, // cache_ttl_override
        None, // idempotency_key
        None, // env
        &rocky_cli::commands::DeferOptions::default(),
        &rocky_cli::commands::SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None, // run_id_override
        None, // governed_ctx
        false,
    )
    .await
    .expect("the run must execute the loaded snapshot A");

    // (1) EXECUTION followed A: A's target schema was materialized, B's not.
    assert_eq!(
        orders_tables_in_schema(&db, "staging__acme").await,
        1,
        "the run must materialize config A's target (staging__acme.orders)"
    );
    assert_eq!(
        orders_tables_in_schema(&db, "swapped__acme").await,
        0,
        "the swapped-in config B's target must NOT be materialized"
    );

    // (2) The RECORD stamps A's load-time fingerprint (the F10 gate-path
    // fix), never a re-read of the swapped path.
    let store =
        rocky_core::state::StateStore::open_read_only(&state_path).expect("open state store");
    let runs = store.list_runs(10).expect("list runs");
    assert_eq!(runs.len(), 1, "exactly one run was recorded");
    assert_eq!(
        runs[0].config_hash, loaded_a.fingerprint,
        "RunRecord.config_hash must be the EXECUTED snapshot's fingerprint (A)"
    );
    assert_ne!(
        runs[0].config_hash, fp_b,
        "RunRecord.config_hash must not describe the swapped-in file (B)"
    );
}

/// Parity pin: for an unswapped file, the fingerprint captured by
/// `load_rocky_config_fingerprinted` is byte-identical to rocky-cli's
/// path-based `output::config_fingerprint` — including for a config whose
/// `freshness.overrides` `HashMap` carries multiple keys (both sides hash the
/// raw bytes, so map iteration order cannot diverge them).
#[test]
fn fingerprint_parity_with_path_based_config_fingerprint() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let path: PathBuf = dir.path().join("rocky.toml");
    std::fs::write(
        &path,
        r#"
[adapter.db]
type = "duckdb"
path = "wh.duckdb"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "db"

[pipeline.silver.checks]
enabled = true

[pipeline.silver.checks.freshness]
threshold_seconds = 3600

[pipeline.silver.checks.freshness.overrides]
"raw__us_west__shopify" = 7200
"raw__eu__stripe" = 1800
"#,
    )
    .expect("write config");

    let loaded =
        rocky_core::config::load_rocky_config_fingerprinted(&path).expect("fingerprinted load");
    let path_based = rocky_cli::output::config_fingerprint(&path);
    assert_eq!(
        loaded.fingerprint, path_based,
        "load-time fingerprint must equal the path-based fingerprint for an unswapped file \
         (RunRecord.config_hash history stays comparable)"
    );
}
