//! A quality run whose error-severity checks fail carries the verdict in
//! `check_gate_failed`, and `tables_failed` stays a count of tables (#1816).
//!
//! End-to-end through the real binary, because the defect was in which field
//! carried the gate and the unit test could not tell: it used one table with
//! one check, so the failed-check count written into `tables_failed` was 1,
//! the same number a table count would give. Two failing checks on one table
//! pull the two apart. Reading the emitted payload also pins the ORDER: the
//! fields are stamped before the JSON is printed, not after (#1788), which a
//! unit test that calls the helper directly cannot see.
//!
//! ```text
//!   1 table, 2 failing error-severity checks
//!     exit code   1
//!     JSON        status = Failure, check_gate_failed = true, tables_failed = 0
//!     history     Failure                    (rocky history)
//! ```
//!
//! The honest-failure half is pinned too: `fail_on_error = false` leaves the
//! run at exit 0 / `Success` with no gate and still no failed table.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// A quality pipeline over DuckDB with two assertions on one table that the
/// seeded row violates. `extra` appends the per-case knob to `[checks]`.
fn config(extra: &str) -> String {
    format!(
        r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.dq]
type = "quality"

[pipeline.dq.target]
adapter = "default"

[[pipeline.dq.tables]]
catalog = "fixture"
schema = "main"
table = "orders"

[pipeline.dq.checks]
enabled = true
row_count = true
{extra}

[[pipeline.dq.checks.assertions]]
table = "orders"
type = "not_null"
column = "name"

[[pipeline.dq.checks.assertions]]
table = "orders"
type = "accepted_values"
column = "status"
values = ["pending", "shipped"]
"#
    )
}

/// One row that violates both assertions: a NULL `name` and a `status`
/// outside the accepted list. The row count check passes (one row).
fn seed(dir: &Path, extra: &str) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE main.orders AS
             SELECT 1 AS id, CAST(NULL AS VARCHAR) AS name, 'lost' AS status;",
    )
    .expect("seed table");
    drop(conn);
    fs::write(dir.join("rocky.toml"), config(extra)).expect("write config");
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
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

/// The failed checks in a run payload, by name.
fn failed_checks(out: &serde_json::Value) -> Vec<String> {
    out["check_results"]
        .as_array()
        .expect("check_results")
        .iter()
        .flat_map(|t| t["checks"].as_array().expect("checks").iter())
        .filter(|c| c["passed"] == false)
        .map(|c| c["name"].as_str().expect("name").to_string())
        .collect()
}

/// The default posture: `fail_on_error` defaults to `true` and the
/// assertions' severity defaults to `error`, so the run fails. The gate says
/// so, the table count does not, and the payload was stamped before it was
/// printed.
#[test]
fn a_failed_quality_gate_rides_check_gate_failed_on_the_wire() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(1),
        "a failed quality gate fails the run; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let out = json(&run);
    let failed = failed_checks(&out);
    assert_eq!(
        failed.len(),
        2,
        "precondition: two failed checks on one table, so a check count and a \
         table count cannot coincide: {out}"
    );
    assert_eq!(
        out["check_results"].as_array().map(Vec::len),
        Some(1),
        "precondition: one table: {out}"
    );

    assert_eq!(out["status"], "Failure", "JSON status: {out}");
    assert_eq!(
        out["check_gate_failed"],
        serde_json::json!(true),
        "the gate carries the verdict: {out}"
    );
    assert_eq!(
        out["tables_failed"], 0,
        "two failed checks are not two failed tables, and not one: {out}"
    );

    // The persisted record, read back through the CLI that orchestrators and
    // the schedule reconciler read, agrees. (`rocky history` does not project
    // the gate flag; the unit test in `run_local.rs` pins that the record
    // inherits it.)
    let history = rocky(dir, &["history"]);
    assert!(
        history.status.success(),
        "{}",
        String::from_utf8_lossy(&history.stderr)
    );
    let history = json(&history);
    let runs = history["runs"].as_array().expect("runs");
    assert_eq!(runs.len(), 1, "one run persisted: {history}");
    assert_eq!(
        runs[0]["status"], "Failure",
        "the record must not say Success: {history}"
    );
}

/// The gate off: the same failures leave the run at exit 0 / `Success`, with
/// no gate and still no failed table. The check outcomes are still reported.
#[test]
fn fail_on_error_off_leaves_the_run_green_and_the_counts_honest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "fail_on_error = false");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let out = json(&run);
    assert_eq!(
        failed_checks(&out).len(),
        2,
        "the failures are still reported: {out}"
    );
    assert_eq!(out["status"], "Success", "{out}");
    assert!(
        out.get("check_gate_failed").is_none(),
        "the gate is not tripped, and an untripped gate is omitted: {out}"
    );
    assert_eq!(out["tables_failed"], 0, "{out}");
}
