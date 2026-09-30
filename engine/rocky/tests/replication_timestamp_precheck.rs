//! The binary refuses a missing replication timestamp before creating its target.

use std::fs;
use std::process::Command;

#[test]
fn missing_incremental_timestamp_fails_only_its_table_before_copy() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("fixture.duckdb");
    let conn = duckdb::Connection::open(&db_path).expect("open source database");
    conn.execute_batch(
        "CREATE SCHEMA raw__demo;
         CREATE TABLE raw__demo.orders AS SELECT 1 AS id, 'one' AS name;
         CREATE TABLE raw__demo.events AS
             SELECT 2 AS id, TIMESTAMP '2026-09-01 00:00:00' AS TS;",
    )
    .expect("seed source tables");
    drop(conn);
    fs::write(
        tmp.path().join("rocky.toml"),
        r#"[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "incremental"
timestamp_column = "ts"

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
"#,
    )
    .expect("write config");

    let run = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "--config", "rocky.toml", "run"])
        .current_dir(tmp.path())
        .env("RUST_LOG", "error")
        .output()
        .expect("run rocky");
    assert!(
        !run.status.success(),
        "missing timestamp must fail the run: {}",
        String::from_utf8_lossy(&run.stdout)
    );
    let out: serde_json::Value = serde_json::from_slice(&run.stdout).unwrap_or_else(|e| {
        panic!(
            "run did not emit JSON ({e}): stdout={} stderr={}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        )
    });
    assert_eq!(out["tables_failed"], 1, "{out}");
    assert_eq!(out["tables_copied"], 1, "{out}");
    let errors = out["errors"].as_array().expect("per-table errors");
    assert_eq!(errors.len(), 1, "{out}");
    assert_eq!(errors[0]["failure_kind"], "compile-error", "{out}");
    let message = errors[0]["error"].as_str().expect("error text");
    assert!(
        message.contains("source `raw__demo.orders` has no column `ts`"),
        "{message}"
    );
    assert!(
        message.contains("the pipeline's timestamp_column"),
        "{message}"
    );
    assert!(message.contains("columns: id, name"), "{message}");

    let conn = duckdb::Connection::open(&db_path).expect("reopen database");
    let missing_targets: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'staging__demo' AND table_name = 'orders'",
            [],
            |row| row.get(0),
        )
        .expect("look up refused target");
    assert_eq!(missing_targets, 0, "no copy statement may create orders");
    let copied_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM staging__demo.events", [], |row| {
            row.get(0)
        })
        .expect("sibling target exists");
    assert_eq!(copied_rows, 1, "case-folded TS column must copy");
}
