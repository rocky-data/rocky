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

    let run = rocky(
        dir,
        &["run", "--pipeline", "transform", "--models", "models"],
    );
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

/// Write a project with the given model files, seed the DuckDB fixture, and
/// return the temp dir.
fn project(models: &[(&str, &str)]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(SEED_SQL).expect("seed sql");
    }
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    for (file, body) in models {
        fs::write(models_dir.join(file), body).expect("write model file");
    }
    tmp
}

/// Run `rocky` with extra environment variables, returning the parsed JSON.
fn rocky_env(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> serde_json::Value {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    cmd.arg("-c")
        .arg(dir.join("rocky.toml"))
        .args(args)
        .args(["--output", "json"])
        .current_dir(dir)
        .env("RUST_LOG", "error");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("spawn rocky");
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

fn model_detail<'a>(compile: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    compile["models_detail"]
        .as_array()
        .expect("models_detail array")
        .iter()
        .find(|d| d["name"] == name)
        .unwrap_or_else(|| panic!("model '{name}' not in compile output: {compile}"))
}

fn dag_node<'a>(dag: &'a serde_json::Value, label: &str) -> &'a serde_json::Value {
    dag["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| n["label"] == label)
        .unwrap_or_else(|| panic!("node '{label}' not in dag output: {dag}"))
}

/// #1919 follow-up (P1-1): a `${VAR}` whose value is an enum tag
/// (`type = "${MODE}"` → `time_interval`), or whose value is part of a field
/// name (`time_column = "${COL}"` → `time_col`, inside `time_column`), must
/// not abort `rocky compile` or `rocky dag`. Both used to round-trip the
/// strategy through a renderer that rewrote the tag and the key, and then
/// failed to read it back. (`incremental` is refused on transformation models
/// by E037, so the same collision is shown on `time_interval`.)
#[test]
fn a_var_equal_to_a_strategy_tag_or_a_key_fragment_does_not_abort_compile_or_dag() {
    const MODEL_TOML: &str = r#"
[strategy]
type = "${ROCKY_T1919_MODE}"
time_column = "${ROCKY_T1919_COL}"
granularity = "day"

[target]
catalog = "fixture"
schema = "staging_t1919_resolved"
table = "stg_events"
"#;
    let tmp = project(&[
        (
            "stg_events.sql",
            "SELECT order_id, DATE '2024-01-01' AS time_col FROM raw.orders \
             WHERE time_col >= @start_date AND time_col < @end_date\n",
        ),
        ("stg_events.toml", MODEL_TOML),
    ]);
    let env = [
        ("ROCKY_T1919_MODE", "time_interval"),
        ("ROCKY_T1919_COL", "time_col"),
    ];

    let compile = rocky_env(tmp.path(), &["compile", "--models", "models"], &env);
    let strategy = &model_detail(&compile, "stg_events")["strategy"];
    assert_eq!(strategy["type"], "time_interval", "{strategy}");
    assert_eq!(strategy["time_column"], "time_col", "{strategy}");

    let dag = rocky_env(tmp.path(), &["dag", "--models", "models"], &env);
    let strategy = &dag_node(&dag, "stg_events")["strategy"];
    assert_eq!(strategy["type"], "time_interval", "{strategy}");
    assert_eq!(strategy["time_column"], "time_col", "{strategy}");
}

/// #1919 follow-up (P1-2, P1-3) — the engine half of the dagster-rocky
/// partition contract. `dagster_rocky.partitions.partitions_def_for_model_detail`
/// passes `models_detail[].strategy.first_partition` to Dagster as a start
/// date, so a `${VAR}` first partition must print as the resolved date, never
/// as `${NAME}` (Dagster cannot parse that). The dagster half is
/// `test_partitions.py::test_compile_contract_first_partition_from_a_var`.
///
/// Model names and `depends_on` print exactly as the engine runs them too —
/// even when a registered value is the model's name — because dagster-rocky
/// matches them against `rocky run`'s model names, contract file stems and
/// `rocky dag` labels.
#[test]
fn compile_prints_first_partition_and_model_names_resolved() {
    const FIRST: &str = "2024-01-01";
    const UPSTREAM_TOML: &str = r#"
[strategy]
type = "time_interval"
time_column = "order_date"
granularity = "day"
first_partition = "${ROCKY_T1919_FIRST}"

[target]
catalog = "fixture"
schema = "staging_t1919_resolved"
table = "stg_orders_daily"

[tags]
owner = "${ROCKY_T1919_MODEL_NAME}"
"#;
    const DOWNSTREAM_TOML: &str = r#"
depends_on = ["stg_orders_daily"]

[target]
catalog = "fixture"
schema = "staging_t1919_resolved"
table = "fct_orders_daily"
"#;
    let tmp = project(&[
        (
            "stg_orders_daily.sql",
            "SELECT order_id, DATE '2024-01-01' AS order_date FROM raw.orders \
             WHERE order_date >= @start_date AND order_date < @end_date\n",
        ),
        ("stg_orders_daily.toml", UPSTREAM_TOML),
        (
            "fct_orders_daily.sql",
            "SELECT order_id FROM stg_orders_daily\n",
        ),
        ("fct_orders_daily.toml", DOWNSTREAM_TOML),
    ]);
    // The model's own name is a registered value: it is printed resolved.
    let env = [
        ("ROCKY_T1919_FIRST", FIRST),
        ("ROCKY_T1919_MODEL_NAME", "stg_orders_daily"),
    ];

    let compile = rocky_env(tmp.path(), &["compile", "--models", "models"], &env);
    let upstream = model_detail(&compile, "stg_orders_daily");
    let strategy = &upstream["strategy"];
    assert_eq!(strategy["type"], "time_interval", "{strategy}");
    assert_eq!(strategy["granularity"], "day", "{strategy}");
    assert_eq!(strategy["first_partition"], FIRST, "{strategy}");
    // Tags still print as `${NAME}`.
    assert_eq!(upstream["tags"]["owner"], "${ROCKY_T1919_MODEL_NAME}");
    let downstream = model_detail(&compile, "fct_orders_daily");
    assert_eq!(
        downstream["depends_on"],
        serde_json::json!(["stg_orders_daily"]),
        "{downstream}"
    );

    let dag = rocky_env(tmp.path(), &["dag", "--models", "models"], &env);
    let node = dag_node(&dag, "stg_orders_daily");
    assert_eq!(node["strategy"]["first_partition"], FIRST, "{node}");
}
