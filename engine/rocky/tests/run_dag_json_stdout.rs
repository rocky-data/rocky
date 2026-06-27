//! Integration test for the `rocky run --dag --output json` stdout contract.
//!
//! Regression guard: under `-o json` the unified-DAG path must emit *exactly*
//! one JSON document on stdout (the `DagRunOutput`). Historically each DAG
//! sub-run (replication load, transformation, seed) was dispatched with
//! `json = false` and printed its human summary line — "Copied N tables …",
//! "transformation pipeline complete …" — to stdout *before* the JSON payload,
//! so an orchestrator had to slice from the first `{`. Those lines now route to
//! stderr; stdout stays pure JSON.
//!
//! Spawns the real `rocky` binary against a tiny DuckDB fixture with both a
//! replication (`ingest`) and a transformation (`transform`) pipeline, so both
//! summary printers are exercised on one run.

use std::fs;
use std::process::Command;

/// A replication pipeline + a transformation pipeline over one DuckDB file.
/// `ingest` copies `raw__orders.orders` into `staging__orders`; `transform`
/// builds a model from the raw table. Both sub-runs print a human summary in
/// table mode — under `-o json` those must land on stderr.
const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"
timestamp_column = "_updated_at"

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

[pipeline.ingest.execution]
concurrency = 1

[pipeline.transform]
type = "transformation"
depends_on = ["ingest"]

[pipeline.transform.target]
adapter = "default"
"#;

const MODEL_SQL: &str = "SELECT order_id, customer_id, amount FROM raw__orders.orders\n";

const MODEL_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "staging"
"#;

const SEED_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 5) AS customer_id,
    CAST(i AS DOUBLE) AS amount,
    TIMESTAMP '2026-01-01' + INTERVAL (i) SECOND AS _updated_at
FROM generate_series(1, 10) AS t(i);
"#;

#[test]
fn run_dag_json_stdout_is_a_single_json_document() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // Seed the DuckDB fixture in-process (the binary links duckdb via
    // rocky-duckdb; this dev-dep shares it).
    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(SEED_SQL).expect("seed sql");
    }

    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    fs::write(models_dir.join("stg_orders.sql"), MODEL_SQL).expect("write model sql");
    fs::write(models_dir.join("stg_orders.toml"), MODEL_TOML).expect("write model toml");

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .arg("run")
        .arg("--dag")
        .arg("--output")
        .arg("json")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky");

    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");

    // stdout must parse as exactly one JSON value with no leading text — the
    // core contract. `from_str` rejects both a human preamble and trailing
    // junk after the document.
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not a single JSON document: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    });
    assert_eq!(
        parsed.get("command").and_then(|v| v.as_str()),
        Some("run --dag"),
        "expected a DagRunOutput on stdout, got: {stdout}"
    );

    // No human summary line may appear on stdout.
    for needle in ["Copied", "pipeline complete", "Seed complete"] {
        assert!(
            !stdout.contains(needle),
            "human summary '{needle}' leaked onto stdout:\n{stdout}"
        );
    }

    // The summaries must still be emitted — just on stderr. The replication
    // load's "Copied …" line is deterministic regardless of model success.
    assert!(
        stderr.contains("Copied"),
        "expected the replication summary on stderr, got:\n{stderr}"
    );
}
