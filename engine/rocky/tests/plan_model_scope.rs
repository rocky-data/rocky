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
