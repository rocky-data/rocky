//! A `type = "manual"` discovery adapter lists the schemas and tables its
//! config declares (#1994).
//!
//! Before the fix, the registry registered nothing for `manual`. So `validate`
//! passed and `plan`, `discover` and `run` failed with
//! `no discovery adapter named '<name>'`.
//!
//! ```text
//!   warehouse holds  raw__orders.orders, raw__orders.refunds, raw__extra.ignored
//!   manual declares  raw__orders: [orders]
//!   discover / plan / run see exactly  raw__orders.orders
//! ```

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter.local]
type = "duckdb"
path = "fixture.duckdb"

[adapter.local_discovery]
type = "manual"
kind = "discovery"

[[adapter.local_discovery.schemas]]
name = "raw__orders"
tables = ["orders"]

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source]
adapter = "local"

[pipeline.ingest.source.discovery]
adapter = "local_discovery"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
adapter = "local"
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

/// The warehouse holds more than the manual list declares, so a test that
/// passes proves the list, not the warehouse, decides what is discovered.
fn seed(dir: &Path, config: &str) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders AS SELECT 1 AS id;
         CREATE TABLE raw__orders.refunds AS SELECT 1 AS id;
         CREATE SCHEMA raw__extra;
         CREATE TABLE raw__extra.ignored AS SELECT 1 AS id;",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), config).expect("write config");
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn json(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON ({e}); stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn manual_discovery_drives_validate_discover_plan_and_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, CONFIG);

    let validate = rocky(dir, &["validate"]);
    assert_ok(&validate, "validate");
    assert_eq!(json(&validate)["valid"], serde_json::json!(true));

    let discover = rocky(dir, &["discover"]);
    assert_ok(&discover, "discover");
    let discover = json(&discover);
    let sources = discover["sources"].as_array().expect("sources");
    assert_eq!(sources.len(), 1, "only the declared schema: {discover}");
    assert_eq!(sources[0]["source_type"], "manual");
    let tables: Vec<&str> = sources[0]["tables"]
        .as_array()
        .expect("tables")
        .iter()
        .map(|t| t["name"].as_str().expect("name"))
        .collect();
    assert_eq!(tables, ["orders"], "only the declared table: {discover}");

    let plan = rocky(dir, &["plan"]);
    assert_ok(&plan, "plan");

    let run = rocky(dir, &["run"]);
    assert_ok(&run, "run");
    let run = json(&run);
    assert_eq!(run["tables_copied"], 1, "{run}");

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen");
    let copied: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = 'staging__orders'",
            [],
            |r| r.get(0),
        )
        .expect("count copied tables");
    assert_eq!(copied, 1, "refunds is not declared, so it is not copied");
}

/// A manual adapter with no schemas can never discover a table, so
/// `validate` refuses it and names the fix.
#[test]
fn validate_refuses_a_manual_adapter_with_no_schemas() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let config = CONFIG.replace(
        "[[adapter.local_discovery.schemas]]\nname = \"raw__orders\"\ntables = [\"orders\"]\n",
        "",
    );
    assert_ne!(config, CONFIG, "the fixture must drop the schemas block");
    seed(dir, &config);

    let validate = json(&rocky(dir, &["validate"]));
    assert_eq!(validate["valid"], serde_json::json!(false), "{validate}");
    let text = validate.to_string();
    assert!(text.contains("V057"), "{validate}");
    assert!(
        text.contains("[[adapter.local_discovery.schemas]]"),
        "{validate}"
    );
}
