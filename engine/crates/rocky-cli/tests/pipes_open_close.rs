//! #2166: a Pipes-mode run sends Dagster's `opened` handshake first and
//! `closed` last, on every pipeline type — transformation, quality, and
//! snapshot (even when the pipeline itself fails).
//!
//! These three tests used to live in `commands::run_local`'s own
//! `#[cfg(test)] mod tests`, sharing that module's process with roughly two
//! thousand other lib tests. `cargo test`'s default `--test-threads=4` runs
//! several of those tests concurrently, and because `DAGSTER_PIPES_CONTEXT` /
//! `DAGSTER_PIPES_MESSAGES` are process-global env vars, ANY other test that
//! happens to call `commands::run` (or `run_transformation` / `run_quality` /
//! `run_snapshot` directly) while one of these three held the vars set would
//! itself activate Pipes mode and write into this test's own capture file —
//! `crate::testing::lock_pipes_env()` only serializes the tests that
//! deliberately set these vars against EACH OTHER, not against the ~2200
//! other lib tests that never touch them but still read them indirectly via
//! `PipesEmitter::detect()` (`src/pipes.rs:149`) at every one of its call
//! sites (`src/commands/run.rs:2766`, `src/commands/run_local.rs:129/559/
//! 1383`) and via `AuditContext::detect()` (`src/commands/run_audit.rs:67`,
//! whose `detect_session_source` reads the same context var at line 131).
//! Confirmed empirically: at 4 threads, repeated runs of the full
//! `rocky-cli` lib suite failed 2 tests out of 2 tries (and once, a THIRD
//! failed a stricter way — a corrupted JSONL line from two processes
//! writing the same file at once); at 1 thread, all three always passed.
//!
//! Moving them here removes the shared-process hazard without touching
//! `detect()` or any other test: an integration test file under `tests/`
//! compiles to its own binary, so both plain `cargo test` and `cargo
//! nextest run` (what CI uses) already give it a dedicated OS process no
//! other test can observe. The one remaining race — these three tests
//! against EACH OTHER, since they all set the same process-global vars —
//! is handled the same way the rest of this file's siblings handle a
//! process-global override: a local, poison-tolerant lock (see
//! `remote_testing::serial_guard()` in `remote_state_bypass.rs` for the
//! established pattern this mirrors).
//!
//! Drives the real `rocky_cli::commands::run` end-to-end (real DuckDB, no
//! mocks) — the same public entry point `remote_state_bypass.rs`'s
//! `drive_run` uses — so these tests now also exercise the outer Pipes
//! `detect()`/`closed()` pair in `run.rs` itself, not just the inner one in
//! `run_local.rs`, closer to what a real Dagster launch does.

#![cfg(feature = "duckdb")]

use std::io::Write as _;
use std::path::Path;
use std::sync::Mutex;

use rocky_cli::commands::{DeferOptions, PartitionRunOptions, SkipRunOptions};
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Serializes these three tests against each other (all three mutate the
/// same process-global `DAGSTER_PIPES_*` env vars). Poison-tolerant: one
/// test panicking mid-run must not wedge the other two. This file is its
/// own test binary, so nothing outside it can observe the vars while held.
static ENV_LOCK: Mutex<()> = Mutex::new(());
fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `base64(zlib(json))` — what every real `dagster_pipes.encode_param` call
/// produces, and the only shape `PipesEmitter::detect` accepts (#2163).
/// Duplicated rather than shared across files/crates, matching the existing
/// convention in `pipes.rs` / `run_local.rs`.
fn encode_pipes_param(value: &serde_json::Value) -> String {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(serde_json::to_string(value).unwrap().as_bytes())
        .unwrap();
    let compressed = encoder.finish().unwrap();
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, compressed)
}

fn read_pipes_jsonl(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Sets both Pipes env vars for the duration of `f`, restores whatever was
/// there before (even if `f` panics — this guard is about correctness under
/// `cargo test`'s parallelism against this file's OTHER two tests, not
/// about the mutation-check, which does not panic), and returns the
/// captured message stream.
#[allow(clippy::await_holding_lock)]
async fn run_with_pipes_capture<T, Fut: std::future::Future<Output = T>>(
    messages_path: &Path,
    f: impl FnOnce() -> Fut,
) -> (Vec<serde_json::Value>, T) {
    let _g = lock_env();
    let context_env = encode_pipes_param(&serde_json::json!({}));
    let messages_env =
        encode_pipes_param(&serde_json::json!({"path": messages_path.to_str().unwrap()}));
    let prior_context = std::env::var(rocky_cli::pipes::ENV_PIPES_CONTEXT).ok();
    let prior_messages = std::env::var(rocky_cli::pipes::ENV_PIPES_MESSAGES).ok();
    // SAFETY: serialised by ENV_LOCK; this file is its own process, so no
    // other test binary can observe these vars while they are set.
    unsafe {
        std::env::set_var(rocky_cli::pipes::ENV_PIPES_CONTEXT, &context_env);
        std::env::set_var(rocky_cli::pipes::ENV_PIPES_MESSAGES, &messages_env);
    }

    let result = f().await;

    // SAFETY: serialised by ENV_LOCK.
    unsafe {
        match prior_context {
            Some(v) => std::env::set_var(rocky_cli::pipes::ENV_PIPES_CONTEXT, v),
            None => std::env::remove_var(rocky_cli::pipes::ENV_PIPES_CONTEXT),
        }
        match prior_messages {
            Some(v) => std::env::set_var(rocky_cli::pipes::ENV_PIPES_MESSAGES, v),
            None => std::env::remove_var(rocky_cli::pipes::ENV_PIPES_MESSAGES),
        }
    }

    (read_pipes_jsonl(messages_path), result)
}

/// Drive the real `commands::run`, ungoverned, single pipeline, no filters —
/// the same shape `remote_state_bypass.rs`'s `drive_run` uses.
async fn drive_run(
    config_path: &Path,
    state_path: &Path,
    models_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let loaded = std::sync::Arc::new(rocky_core::config::load_rocky_config_fingerprinted(
        config_path,
    )?);
    rocky_cli::commands::run(
        config_path,
        loaded,
        None, // filter
        None, // pipeline_name_arg — single pipeline resolves
        state_path,
        None,  // governance_override
        false, // output_json
        models_dir,
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &PartitionRunOptions::default(),
        None, // model_name_filter
        None, // cache_ttl_override
        None, // idempotency_key
        None, // env
        &DeferOptions::default(),
        &SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None,  // run_id_override
        None,  // governed_ctx
        false, // assume_fresh_state
        None,  // #1460
    )
    .await
    .map(|_| ())
}

#[tokio::test]
async fn run_transformation_pipes_path_opens_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir(&models_dir).unwrap();
    let db = dir.path().join("t.duckdb");
    let state_path = dir.path().join(".rocky-state.redb");
    let messages_path = dir.path().join("messages.jsonl");
    let config_path = dir.path().join("rocky.toml");

    {
        let a = DuckDbWarehouseAdapter::open(&db).expect("seed open");
        a.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
            .await
            .unwrap();
        a.execute_statement("CREATE TABLE main.src AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)")
            .await
            .unwrap();
    }
    std::fs::write(models_dir.join("stg.sql"), "SELECT id FROM main.src\n").unwrap();
    std::fs::write(
        models_dir.join("stg.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"stg\"\n",
    )
    .unwrap();
    std::fs::write(
        &config_path,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target.governance]
auto_create_schemas = true
"#,
            db = db.display()
        ),
    )
    .unwrap();

    let (lines, result) = run_with_pipes_capture(&messages_path, || {
        drive_run(&config_path, &state_path, Some(models_dir.as_path()))
    })
    .await;
    result.expect("full-DAG transformation run should succeed");

    assert!(lines.len() >= 2, "{lines:?}");
    assert_eq!(lines[0]["method"], "opened", "{lines:?}");
    assert_eq!(lines[0]["params"], serde_json::json!({"extras": {}}));
    assert_eq!(
        lines.last().unwrap()["method"],
        "closed",
        "run_transformation's Pipes path must call closed() (#2166): {lines:?}"
    );
}

#[tokio::test]
async fn run_quality_pipes_path_opens_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("q.duckdb");
    let state_path = dir.path().join(".rocky-state.redb");
    let messages_path = dir.path().join("messages.jsonl");
    let config_path = dir.path().join("rocky.toml");

    {
        let a = DuckDbWarehouseAdapter::open(&db).expect("seed open");
        a.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
            .await
            .unwrap();
        a.execute_statement("CREATE TABLE main.present AS SELECT 1 AS id")
            .await
            .unwrap();
    }
    // `catalog_name_for_path("q.duckdb") == "q"` (rocky_duckdb::dialect).
    std::fs::write(
        &config_path,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.qa]
type = "quality"

[pipeline.qa.target]
adapter = "default"

[[pipeline.qa.tables]]
catalog = "q"
schema = "main"
table = "present"

[pipeline.qa.checks]
enabled = true
row_count = true
"#,
            db = db.display()
        ),
    )
    .unwrap();

    let (lines, result) = run_with_pipes_capture(&messages_path, || {
        drive_run(&config_path, &state_path, None)
    })
    .await;
    result.expect("quality run should succeed");

    assert!(lines.len() >= 2, "{lines:?}");
    assert_eq!(lines[0]["method"], "opened", "{lines:?}");
    assert_eq!(
        lines.last().unwrap()["method"],
        "closed",
        "run_quality's Pipes path must call closed() (#2166): {lines:?}"
    );
}

#[tokio::test]
async fn run_snapshot_pipes_path_opens_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.duckdb");
    let state_path = dir.path().join(".rocky-state.redb");
    let messages_path = dir.path().join("messages.jsonl");
    let config_path = dir.path().join("rocky.toml");

    {
        let a = DuckDbWarehouseAdapter::open(&db).expect("seed open");
        a.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
            .await
            .unwrap();
        a.execute_statement(
            "CREATE TABLE main.src AS SELECT 1 AS id, CURRENT_TIMESTAMP AS updated_at",
        )
        .await
        .unwrap();
    }
    // `catalog_name_for_path("s.duckdb") == "s"` (rocky_duckdb::dialect).
    std::fs::write(
        &config_path,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.dim]
type = "snapshot"
unique_key = ["id"]
updated_at = "updated_at"

[pipeline.dim.source]
catalog = "s"
schema = "main"
table = "src"

[pipeline.dim.target]
catalog = "s"
schema = "main"
table = "src_history"

[pipeline.dim.target.governance]
auto_create_schemas = true
"#,
            db = db.display()
        ),
    )
    .unwrap();

    // DuckDB's parser refuses the `MERGE ... WHEN NOT MATCHED THEN INSERT
    // (*) VALUES (source.*, ...)` SQL `generate_snapshot_sql` emits for this
    // pipeline type — a pre-existing dialect gap, unrelated to Pipes and out
    // of scope here. `closed()` must still fire on THIS failure path (it
    // runs before the `tables_failed > 0` bail in `run_snapshot`), which is
    // exactly the case worth pinning: a Pipes launch must not lose its
    // `closed` message just because the pipeline's own work failed.
    let (lines, result) = run_with_pipes_capture(&messages_path, || {
        drive_run(&config_path, &state_path, None)
    })
    .await;

    let err = result.expect_err(
        "this test's snapshot SQL is expected to fail on DuckDB's MERGE dialect gap; \
         if it now succeeds, DuckDB gained support and this test should assert Ok instead",
    );
    assert!(
        err.to_string().contains("snapshot pipeline failed"),
        "expected the known DuckDB-dialect failure, got a different error (a real regression?): {err}"
    );

    assert!(lines.len() >= 2, "{lines:?}");
    assert_eq!(lines[0]["method"], "opened", "{lines:?}");
    assert_eq!(
        lines.last().unwrap()["method"],
        "closed",
        "run_snapshot's Pipes path must call closed() even on its failure path (#2166): {lines:?}"
    );
}
