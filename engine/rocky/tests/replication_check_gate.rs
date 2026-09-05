//! A replication run whose declared error-severity check fails must not exit 0
//! and must not persist `Success` (#1598).
//!
//! End-to-end through the real binary, because the defect spanned four
//! surfaces that each read the outcome differently: the process exit code, the
//! `--output json` payload, the persisted run record, and the resume gate. A
//! unit test can pin any one of them and still miss the disagreement.
//!
//! ```text
//!   copy 1 table  ->  not_null assertion fails (error severity)
//!     exit code   2      (partial success)
//!     JSON        status = PartialFailure, check_gate_failed = true
//!     record      PartialFailure            (rocky history)
//!     resume      refused: it would copy nothing
//! ```
//!
//! The honest-failure half is pinned too: `severity = "warning"` and
//! `fail_on_error = false` must leave the run at exit 0 / `Success`.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// A replication pipeline over DuckDB with one `not_null` assertion on a
/// column the source leaves NULL. `extra` appends the per-case knob.
fn config(extra: &str) -> String {
    format!(
        r#"
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
schema_template = "staging__{{source}}"

[pipeline.ingest.target.governance]
auto_create_schemas = true

[[pipeline.ingest.checks.assertions]]
table = "orders"
type = "not_null"
column = "name"
{extra}
"#
    )
}

/// A source table with one violating row, so the `not_null` assertion fails
/// against data that copied cleanly.
fn seed(dir: &Path, extra: &str) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders AS
             SELECT 1 AS id, CAST(NULL AS VARCHAR) AS name;",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), config(extra)).expect("write config");
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

/// The default posture: `fail_on_error` defaults to `true` and the assertion's
/// severity defaults to `error`, so the run fails. Every surface must say so,
/// and `tables_failed` must stay a count of tables.
#[test]
fn a_failed_error_severity_check_fails_the_run_on_every_surface() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(2),
        "a copied-then-violating run is a partial failure; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let out = json(&run);
    assert_eq!(out["status"], "PartialFailure", "JSON status: {out}");
    assert_eq!(out["check_gate_failed"], serde_json::json!(true));
    assert_eq!(
        out["tables_failed"], 0,
        "a failed check is not a failed table"
    );
    assert_eq!(out["tables_copied"], 1, "the copy itself succeeded");
    let failed: Vec<&serde_json::Value> = out["check_results"]
        .as_array()
        .expect("check_results")
        .iter()
        .flat_map(|t| t["checks"].as_array().expect("checks"))
        .filter(|c| c["passed"] == serde_json::json!(false))
        .collect();
    assert_eq!(failed.len(), 1, "one failed check: {out}");

    // The persisted record, read back through the CLI that orchestrators and
    // the schedule reconciler read.
    let history = rocky(dir, &["history"]);
    assert!(history.status.success());
    let history = json(&history);
    let latest = &history["runs"][0];
    assert_eq!(
        latest["status"], "PartialFailure",
        "the record must not say Success: {history}"
    );

    // Both resume entry points refuse: every table copied, so a resume would
    // copy nothing, run no check, and exit 0 on still-violating data.
    let run_id = latest["run_id"].as_str().expect("run_id");
    for args in [vec!["run", "--resume", run_id], vec!["run", "--resume-latest"]] {
        let resumed = rocky(dir, &args);
        assert!(
            !resumed.status.success(),
            "{args:?} must refuse; stdout: {}",
            String::from_utf8_lossy(&resumed.stdout)
        );
        let stderr = String::from_utf8_lossy(&resumed.stderr);
        assert!(
            stderr.contains("nothing to resume")
                && stderr.contains("copied every table it planned to copy"),
            "{args:?} gave the wrong refusal: {stderr}"
        );
    }
}

/// `fail_on_error = false` is the documented escape hatch. It must leave the
/// exit code and the recorded status exactly where they were.
#[test]
fn fail_on_error_false_keeps_the_run_green() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "\n[pipeline.ingest.checks]\nfail_on_error = false\n");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "fail_on_error = false must not fail the run; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = json(&run);
    assert_eq!(out["status"], "Success");
    assert!(out.get("check_gate_failed").is_none(), "gate not tripped");
}

/// A warning-severity check is advisory. It reports and never gates.
#[test]
fn a_warning_severity_check_failure_keeps_the_run_green() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "severity = \"warning\"\n");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "a warning-severity failure must not fail the run; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = json(&run);
    assert_eq!(out["status"], "Success");
    assert!(out.get("check_gate_failed").is_none(), "gate not tripped");

    // It is still reported — advisory, not hidden.
    let failed = out["check_results"]
        .as_array()
        .expect("check_results")
        .iter()
        .flat_map(|t| t["checks"].as_array().expect("checks"))
        .filter(|c| c["passed"] == serde_json::json!(false))
        .count();
    assert_eq!(failed, 1, "the warning still appears in the output: {out}");
}
