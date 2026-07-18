//! `rocky plan --model` must describe exactly what `rocky apply` executes.

use std::fs;
use std::process::Command;

const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

const MODEL_TOML: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "fixture"
schema = "main"
"#;

#[test]
fn model_plan_matches_applied_scope() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let config = dir.join("rocky.toml");

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE SCHEMA raw__orders;
             CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .expect("seed source");
    }

    fs::write(&config, ROCKY_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    for (name, sql) in [("known", "SELECT 1 AS id"), ("other", "SELECT 2 AS id")] {
        fs::write(models.join(format!("{name}.sql")), sql).expect("write model sql");
        fs::write(models.join(format!("{name}.toml")), MODEL_TOML).expect("write model config");
    }

    let plan = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["plan", "--model", "known"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run plan");
    assert!(
        plan.status.success(),
        "plan failed: {}",
        String::from_utf8_lossy(&plan.stderr)
    );

    let plan: serde_json::Value =
        serde_json::from_slice(&plan.stdout).expect("plan output should be JSON");
    assert_eq!(plan["models"], serde_json::json!(["known"]));
    assert_eq!(plan["execution_layers"], serde_json::json!([["known"]]));
    assert_eq!(plan["statements"].as_array().map(Vec::len), Some(1));
    assert_eq!(plan["statements"][0]["target"], "fixture.main.known");

    let plan_id = plan["plan_id"].as_str().expect("persisted plan ID");
    let apply = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["apply", plan_id])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("apply plan");
    assert!(
        apply.status.success(),
        "apply failed: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    for (schema, table, expected) in [
        ("main", "known", 1_i64),
        ("main", "other", 0),
        ("staging__orders", "orders", 0),
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables
                 WHERE table_schema = ? AND table_name = ?",
                [schema, table],
                |row| row.get(0),
            )
            .expect("query table scope");
        assert_eq!(
            count, expected,
            "unexpected materialization of {schema}.{table}"
        );
    }
}

/// #1171 (Edge 2): `--model <name>` against a `--models <dir>` that compiles to
/// zero models must ERROR, not silently degrade to a replication plan that
/// `rocky apply` would execute as a full replication. This path skips the
/// conventional-`models/` governance preview (no conventional `models/` dir),
/// so before the fix it fell through to the replication-plan branch and exited
/// 0 with `plan_kind = "replication"`.
#[test]
fn model_plan_via_empty_models_dir_errors_not_replication() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE SCHEMA raw__orders;
             CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .expect("seed source");
    }

    let config = dir.join("rocky.toml");
    fs::write(&config, ROCKY_TOML).expect("write config");
    // No conventional `models/` dir. A separate `--models` dir that holds only
    // a `_defaults.toml` stub compiles to zero models.
    let alt = dir.join("altmodels");
    fs::create_dir(&alt).expect("create altmodels");
    fs::write(
        alt.join("_defaults.toml"),
        "[strategy]\ntype = \"full_refresh\"\n",
    )
    .expect("write stub");

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["plan", "--models"])
        .arg(&alt)
        .args(["--model", "foo"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run plan");

    assert!(
        !out.status.success(),
        "a model-scoped plan over a zero-model dir must fail, not degrade to replication; \
         stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("refusing to fall back to a replication plan"),
        "expected a model-scope error, got stderr: {stderr}"
    );
    // Nothing on stdout should describe a replication plan the operator could apply.
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("\"plan_kind\":\"replication\""),
        "must not emit a replication plan for a --model invocation; stdout: {stdout}"
    );
}

/// #1171 (Edge 1): when a run-plan persist failure occurs while `--model` is
/// set, `rocky plan` must ERROR rather than fall through to the replication
/// branch and pair transformation statements with a replication `plan_id`
/// (which `apply` would run as replication). Reproduced by squatting the
/// content-addressed run-plan path with a directory so the write fails; the
/// plan_id is stable across runs, so the squat is deterministic.
#[test]
fn model_plan_persist_failure_errors_not_replication() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE SCHEMA raw__orders;
             CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .expect("seed source");
    }

    let config = dir.join("rocky.toml");
    fs::write(&config, ROCKY_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    for (name, sql) in [("known", "SELECT 1 AS id"), ("other", "SELECT 2 AS id")] {
        fs::write(models.join(format!("{name}.sql")), sql).expect("write model sql");
        fs::write(models.join(format!("{name}.toml")), MODEL_TOML).expect("write model config");
    }

    // First run succeeds and persists a run plan; capture its (content-addressed) id.
    let first = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["plan", "--model", "known"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run first plan");
    assert!(
        first.status.success(),
        "first plan failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: serde_json::Value =
        serde_json::from_slice(&first.stdout).expect("plan output should be JSON");
    assert_eq!(first["plan_kind"], "run");
    let plan_id = first["plan_id"]
        .as_str()
        .expect("persisted plan ID")
        .to_string();

    // Squat the run-plan path with a directory so the next persist fails.
    let plan_path = dir
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.json"));
    fs::remove_file(&plan_path).expect("remove first plan file");
    fs::create_dir_all(&plan_path).expect("squat plan path with a directory");

    let second = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["plan", "--model", "known"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run second plan");

    assert!(
        !second.status.success(),
        "a run-plan persist failure under --model must fail, not degrade to replication; \
         stdout: {}",
        String::from_utf8_lossy(&second.stdout)
    );
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("refusing to fall back to a replication plan"),
        "expected a model-scope error, got stderr: {stderr}"
    );
    let stdout = String::from_utf8_lossy(&second.stdout);
    assert!(
        !stdout.contains("\"plan_kind\":\"replication\""),
        "must not emit a replication plan for a --model invocation; stdout: {stdout}"
    );
}

/// #1171 (Edge 3): `--model` and `--dag` are contradictory — `--model` runs a
/// single model and skips replication, while the apply-time DAG runner ignores
/// the model selector and executes every pipeline (including replication). The
/// combination must be rejected at plan time so a plan that `apply` would
/// over-execute as a full DAG is never persisted.
#[test]
fn model_plan_with_dag_is_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE SCHEMA raw__orders;
             CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .expect("seed source");
    }

    let config = dir.join("rocky.toml");
    fs::write(&config, ROCKY_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write model sql");
    fs::write(models.join("known.toml"), MODEL_TOML).expect("write model config");

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(&config)
        .args(["plan", "--model", "known", "--dag"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run plan");

    assert!(
        !out.status.success(),
        "--model + --dag must be rejected at plan time; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--model and --dag are mutually exclusive"),
        "expected a mutual-exclusion error, got stderr: {stderr}"
    );
}
