//! `[[table_overrides]]` match counts through the real binary (R8).
//!
//! `rocky plan` and `rocky run` call one counting function, after `--filter`
//! and before `run` excludes tables missing from the source. So the two
//! report the same counts for the same project, and a rule that matches only
//! a discovered table missing from the source still counts that table.

use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["-c", "rocky.toml", "--state-path", "state.redb"])
        .args(args)
        .output()
        .expect("spawn rocky")
}

fn json(out: &Output, what: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "`{what}` did not print JSON ({e}); exit {:?}\nstdout: {stdout}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

/// A DuckDB replication project whose manual discovery lists two tables in
/// `raw__orders` and one in `raw__customers`. Only `raw__orders.orders` and
/// `raw__customers.customers` exist in the warehouse: `order_items` is
/// discovered but missing from the source, so `run` excludes it.
fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let toml = r#"
[adapter.warehouse]
type = "duckdb"
kind = "data"
path = "warehouse.duckdb"

[adapter.local_discovery]
type = "manual"
kind = "discovery"

[[adapter.local_discovery.schemas]]
name = "raw__orders"
tables = ["orders", "order_items"]

[[adapter.local_discovery.schemas]]
name = "raw__customers"
tables = ["customers"]

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "warehouse"

[pipeline.poc.source.discovery]
adapter = "local_discovery"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "warehouse"
catalog_template = "warehouse"
schema_template = "staging__{source}"

# 0: only a table that is missing from the source.
[[pipeline.poc.table_overrides]]
strategy = "full_refresh"
[pipeline.poc.table_overrides.match]
table = "order_items"

# 1: every table of one connector.
[[pipeline.poc.table_overrides]]
strategy = "full_refresh"
[pipeline.poc.table_overrides.match]
connector = "raw__orders"

# 2: nothing.
[[pipeline.poc.table_overrides]]
strategy = "full_refresh"
[pipeline.poc.table_overrides.match]
connector = "raw__nope"
"#;
    std::fs::write(dir.path().join("rocky.toml"), toml).expect("write rocky.toml");
    let conn = duckdb::Connection::open(dir.path().join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders (id INTEGER);
         INSERT INTO raw__orders.orders VALUES (1), (2);
         CREATE SCHEMA raw__customers;
         CREATE TABLE raw__customers.customers (id INTEGER);
         INSERT INTO raw__customers.customers VALUES (1);",
    )
    .expect("seed");
    drop(conn);
    dir
}

fn counts(body: &serde_json::Value) -> Vec<(u64, u64)> {
    body["override_match_counts"]
        .as_array()
        .unwrap_or_else(|| panic!("no override_match_counts: {body:#}"))
        .iter()
        .map(|row| {
            (
                row["rule_index"].as_u64().unwrap(),
                row["matched_tables"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn plan_and_run_report_the_same_override_match_counts() {
    let dir = project();

    let plan = rocky(dir.path(), &["-o", "json", "plan"]);
    assert!(
        plan.status.success(),
        "plan\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&plan.stdout),
        String::from_utf8_lossy(&plan.stderr)
    );
    let plan_body = json(&plan, "plan");

    let run = rocky(dir.path(), &["-o", "json", "run"]);
    let run_body = json(&run, "run");
    assert!(
        run_body["excluded_tables"]
            .as_array()
            .is_some_and(|rows| rows
                .iter()
                .any(|r| r["table_name"] == "order_items" && r["reason"] == "missing_from_source")),
        "the fixture must exclude order_items as missing: {run_body:#}"
    );

    // Rule 0 matches a table run excludes; it is counted before that.
    let expected = vec![(0, 1), (1, 2), (2, 0)];
    assert_eq!(counts(&plan_body), expected, "plan: {plan_body:#}");
    assert_eq!(counts(&run_body), expected, "run: {run_body:#}");
    assert_eq!(
        plan_body["override_match_counts"], run_body["override_match_counts"],
        "plan and run must agree row for row"
    );

    // The "matches nothing" warning stays, from the same count: only rule 2.
    let warned: Vec<u64> = run_body["override_warnings"]
        .as_array()
        .expect("override_warnings")
        .iter()
        .map(|w| w["rule_index"].as_u64().unwrap())
        .collect();
    assert_eq!(warned, [2], "{run_body:#}");

    // `--filter` narrows both the same way.
    let plan = rocky(
        dir.path(),
        &["-o", "json", "plan", "--filter", "source=customers"],
    );
    let run = rocky(
        dir.path(),
        &["-o", "json", "run", "--filter", "source=customers"],
    );
    let expected = vec![(0, 0), (1, 0), (2, 0)];
    assert_eq!(counts(&json(&plan, "plan --filter")), expected);
    assert_eq!(counts(&json(&run, "run --filter")), expected);

    // `--filter table=` keeps one table: only it counts, still before the
    // missing-from-source exclusion.
    let plan = rocky(
        dir.path(),
        &["-o", "json", "plan", "--filter", "table=order_items"],
    );
    let run = rocky(
        dir.path(),
        &["-o", "json", "run", "--filter", "table=order_items"],
    );
    let expected = vec![(0, 1), (1, 1), (2, 0)];
    assert_eq!(counts(&json(&plan, "plan --filter table=")), expected);
    assert_eq!(counts(&json(&run, "run --filter table=")), expected);
}
