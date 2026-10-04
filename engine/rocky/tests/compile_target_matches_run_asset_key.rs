//! The Dagster asset-key contract for a `${VAR}`-templated `[target]` (#1919).
//!
//! `dagster-rocky` builds a model's asset key from `rocky compile`'s
//! `models_detail[].target` and matches it against `rocky run`'s
//! `materializations[].asset_key`. If the two print the target differently,
//! every materialization for that model is silently dropped in Dagster.
//!
//! #1919 prints a resolved `${VAR}` value as `${NAME}` on config echoes, but
//! leaves target coordinates resolved on purpose, exactly as `rocky run` does.
//! This test pins both halves: the target matches `run`'s asset key, and a
//! non-target sidecar value from `${VAR}` still prints as `${NAME}`.
//!
//! Spawns the real `rocky` binary against a tiny DuckDB fixture.

use std::fs;
use std::path::Path;
use std::process::Command;

const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"

[pipeline.transform.target]
adapter = "default"
"#;

/// Both values are 8 bytes or more, so both are registered as secrets.
const TARGET_SCHEMA: &str = "staging_t1919_resolved";
const OWNER_SECRET: &str = "owner_t1919_secret_value";

const MODEL_SQL: &str = "SELECT order_id, amount FROM raw.orders\n";

const MODEL_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "${ROCKY_T1919_TARGET_SCHEMA}"
table = "stg_orders"

[tags]
owner = "${ROCKY_T1919_OWNER}"
"#;

const SEED_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging_t1919_resolved;
CREATE OR REPLACE TABLE raw.orders AS
SELECT i AS order_id, CAST(i AS DOUBLE) AS amount
FROM generate_series(1, 5) AS t(i);
"#;

fn rocky(dir: &Path, args: &[&str]) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .args(args)
        .args(["--output", "json"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .env("ROCKY_T1919_TARGET_SCHEMA", TARGET_SCHEMA)
        .env("ROCKY_T1919_OWNER", OWNER_SECRET)
        .output()
        .expect("spawn rocky");
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    assert!(
        out.status.success(),
        "rocky {args:?} failed\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("rocky {args:?} stdout is not JSON: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}")
    })
}

#[test]
fn compile_target_equals_run_asset_key_for_a_var_templated_target() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(SEED_SQL).expect("seed sql");
    }
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    fs::write(models_dir.join("stg_orders.sql"), MODEL_SQL).expect("write model sql");
    fs::write(models_dir.join("stg_orders.toml"), MODEL_TOML).expect("write model toml");

    let compile = rocky(dir, &["compile", "--models", "models"]);
    let details = compile["models_detail"]
        .as_array()
        .expect("models_detail array");
    let detail = details
        .iter()
        .find(|d| d["name"] == "stg_orders")
        .unwrap_or_else(|| panic!("PRECONDITION: stg_orders compiled: {compile}"));
    let target = &detail["target"];
    let compile_key = vec![
        target["catalog"].as_str().expect("catalog").to_string(),
        target["schema"].as_str().expect("schema").to_string(),
        target["table"].as_str().expect("table").to_string(),
    ];

    let run = rocky(dir, &["run", "--pipeline", "transform", "--models", "models"]);
    let run_keys: Vec<Vec<String>> = run["materializations"]
        .as_array()
        .expect("materializations array")
        .iter()
        .map(|m| {
            serde_json::from_value(m["asset_key"].clone()).expect("asset_key is a string list")
        })
        .collect();

    // The Dagster matching contract: compile's target is run's asset key.
    assert_eq!(
        compile_key,
        vec!["fixture", TARGET_SCHEMA, "stg_orders"],
        "compile target must print resolved, as run does: {detail}"
    );
    assert!(
        run_keys.contains(&compile_key),
        "compile target {compile_key:?} not among run asset keys {run_keys:?}"
    );

    // A non-target sidecar value from `${VAR}` still prints as `${NAME}`.
    assert_eq!(detail["tags"]["owner"], "${ROCKY_T1919_OWNER}");
    let compile_text = compile.to_string();
    assert!(
        !compile_text.contains(OWNER_SECRET),
        "non-target secret leaked from compile: {compile_text}"
    );
}
