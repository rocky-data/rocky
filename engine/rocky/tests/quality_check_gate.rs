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

/// A schema-wide quality target (`table` omitted) on `schema`, with the row
/// count check only, so the seeded row passes everything the pipeline runs.
/// `extra` appends the per-case knob to `[checks]`.
fn schema_wide_config(schema: &str, extra: &str) -> String {
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
schema = "{schema}"

[pipeline.dq.checks]
enabled = true
row_count = true
{extra}
"#
    )
}

/// One row in `main.orders` that violates both assertions of [`config`]: a
/// NULL `name` and a `status` outside the accepted list. The row count check
/// passes (one row).
fn seed_db(dir: &Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE main.orders AS
             SELECT 1 AS id, CAST(NULL AS VARCHAR) AS name, 'lost' AS status;",
    )
    .expect("seed table");
    drop(conn);
}

fn seed(dir: &Path, extra: &str) {
    seed_db(dir);
    fs::write(dir.join("rocky.toml"), config(extra)).expect("write config");
}

/// A quality pipeline whose assertions all PASS on the seeded row, so the only
/// thing that can fail the run is what `extra` adds.
///
/// The shared [`config`] fixture violates two assertions on purpose, which
/// makes it useless for attributing an exit code: a run against it exits 1
/// whatever quarantine does. The independent review of #1922 caught exactly
/// that in the first version of the test below.
fn clean_config(extra: &str) -> String {
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
"#
    )
}

/// One row that PASSES every assertion `clean_config` declares: a non-null
/// name. So a non-zero exit can only come from what the test adds.
fn seed_clean_db(dir: &Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE main.orders AS SELECT 1 AS id, 'ada' AS name, 'shipped' AS status;",
    )
    .expect("seed table");
    drop(conn);
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

/// [`rocky`] with `--output table`: the human summary. Explicit, because a
/// run whose stdout is not a terminal defaults to JSON.
fn rocky_text(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "table"])
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

/// #1811. A schema-wide target whose schema lists as EMPTY reported a clean
/// run. #1786 closed the case where listing the schema errors; a renamed or
/// never-created schema is not an error to `information_schema`, it is zero
/// rows, and the check loop iterated nothing, emitted nothing, and the run
/// said `Success`, exit 0, having checked no table at all. Through the real
/// binary and the real expansion against a real missing schema, as the issue
/// asks: the helper-level test could not see this path.
#[test]
fn a_schema_wide_target_that_lists_no_tables_is_not_a_clean_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_db(dir);
    fs::write(dir.join("rocky.toml"), schema_wide_config("ghost", "")).expect("write config");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(1),
        "a run that checked nothing is not green; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );

    let out = json(&run);
    assert_eq!(out["status"], "Failure", "JSON status: {out}");
    assert_eq!(out["check_gate_failed"], serde_json::json!(true), "{out}");
    assert_eq!(
        out["tables_failed"], 0,
        "no table failed; none was found: {out}"
    );
    let results = out["check_results"].as_array().expect("check_results");
    assert_eq!(results.len(), 1, "one entry, for the target itself: {out}");
    assert_eq!(
        results[0]["asset_key"],
        serde_json::json!(["fixture", "ghost"]),
        "keyed by the schema, since no table is known: {out}"
    );
    let check = &results[0]["checks"][0];
    assert_eq!(check["name"], "schema_expansion", "{out}");
    assert_eq!(check["passed"], false, "{out}");
    assert_eq!(check["severity"], "error", "{out}");
    let reason = check["not_evaluated"]
        .as_str()
        .expect("not_evaluated reason");
    assert!(
        reason.contains("listed no tables") && reason.contains("fixture.ghost"),
        "the reason says what happened and names the schema: {reason}"
    );
}

/// The gate off: the empty expansion is still reported as a check the engine
/// could not evaluate, but `fail_on_error = false` leaves the run green.
#[test]
fn an_empty_schema_expansion_is_reported_even_when_the_gate_is_off() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_db(dir);
    fs::write(
        dir.join("rocky.toml"),
        schema_wide_config("ghost", "fail_on_error = false"),
    )
    .expect("write config");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = json(&run);
    assert_eq!(out["status"], "Success", "{out}");
    assert!(out.get("check_gate_failed").is_none(), "{out}");
    assert_eq!(
        failed_checks(&out),
        vec!["schema_expansion".to_string()],
        "the unevaluated target is still in the payload: {out}"
    );
}

/// The discriminator: the same schema-wide target on a schema that HAS a
/// table checks that table and is green. The fix keys on an empty listing,
/// not on schema-wide targets as such.
#[test]
fn a_schema_wide_target_with_a_table_still_checks_it_and_passes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_db(dir);
    fs::write(dir.join("rocky.toml"), schema_wide_config("main", "")).expect("write config");

    let run = rocky(dir, &["run"]);
    assert_eq!(
        run.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = json(&run);
    assert_eq!(out["status"], "Success", "{out}");
    let results = out["check_results"].as_array().expect("check_results");
    assert_eq!(results.len(), 1, "the one table the schema lists: {out}");
    assert_eq!(
        results[0]["asset_key"],
        serde_json::json!(["fixture", "main", "orders"]),
        "{out}"
    );
    assert!(failed_checks(&out).is_empty(), "{out}");
    assert_eq!(results[0]["checks"][0]["name"], "row_count", "{out}");
    assert_eq!(results[0]["checks"][0]["passed"], true, "{out}");
}

/// The human summary counts tables actually checked, not `check_results`
/// entries: with the schema-level entry for an empty expansion it said
/// "across 1 table(s)" for a run that found none (round-one review nit).
#[test]
fn the_text_summary_counts_zero_tables_for_an_empty_expansion() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_db(dir);
    fs::write(dir.join("rocky.toml"), schema_wide_config("ghost", "")).expect("write config");

    let run = rocky_text(dir, &["run"]);
    assert_eq!(run.status.code(), Some(1));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        text.contains("across 0 table(s)")
            && text.contains("1 schema target(s) could not be expanded"),
        "the summary says nothing was checked and why: {text}"
    );
}

/// A quarantine compile failure trips the check gate, and the exit code is
/// ATTRIBUTABLE TO IT ALONE.
///
/// The failure here is quarantine-only: an invalid `suffix_valid` makes
/// `suffixed_table_name` refuse the derived identifier. No assertion is
/// involved, every declared check passes, so the only thing that can make this
/// run exit non-zero is the quarantine refusal.
///
/// That matters because the obvious fixture does NOT prove this. A refused
/// quarantine EXPRESSION is also an assertion — quarantine lowers
/// error-severity assertions — so the assertion path refuses the same text and
/// the run would exit 1 with or without the gate change. The independent
/// review of #1922 caught that in the first version of this test, and the
/// co-firing case is pinned separately below.
///
/// Before the gate change, `run_local` warned and skipped: the table was never
/// split and nothing in `RunOutput` said so.
#[test]
fn a_quarantine_compile_failure_trips_the_gate_on_its_own() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_clean_db(dir);
    fs::write(
        dir.join("rocky.toml"),
        clean_config(
            r#"
[pipeline.dq.checks.quarantine]
enabled = true
suffix_valid = "not a valid identifier"
"#,
        ),
    )
    .expect("write config");

    let run = rocky(dir, &["run"]);
    let out = json(&run);

    // The precondition that makes the exit code mean something.
    let other_failures: Vec<String> = failed_checks(&out)
        .into_iter()
        .filter(|n| !n.starts_with("quarantine"))
        .collect();
    assert!(
        other_failures.is_empty(),
        "precondition: every non-quarantine check must pass, or the exit code \
         is not attributable to quarantine: {other_failures:?} in {out}"
    );

    assert_eq!(
        run.status.code(),
        Some(1),
        "a quarantine compile failure must fail the run, not warn and \
         continue; stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(out["check_gate_failed"], serde_json::json!(true), "{out}");

    let check = out["check_results"]
        .as_array()
        .expect("check_results")
        .iter()
        .flat_map(|t| t["checks"].as_array().expect("checks").iter())
        .find(|c| c["name"] == "quarantine:compile")
        .unwrap_or_else(|| panic!("no quarantine check on the wire: {out}"));
    assert_eq!(check["passed"], serde_json::json!(false), "{out}");
    assert!(
        check["not_evaluated"].as_str().is_some(),
        "the reason must be carried, not dropped: {out}"
    );
}

/// A refused quarantine EXPRESSION is refused twice, on both paths, and both
/// refusals reach the wire.
///
/// This is not redundancy to remove: the two carry different reasons. The
/// assertion path says the check could not be generated; the quarantine path
/// says the split did not happen. An operator needs the second to know their
/// rows were not separated.
///
/// The exit code is deliberately NOT asserted as attributable here — the
/// assertion refusal alone would produce it. What is asserted is that the
/// quarantine-specific result exists beside it.
#[test]
fn a_refused_quarantine_expression_is_refused_on_both_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_clean_db(dir);
    // `my_udf` is off the allowlist: an unknown scalar function spliced into
    // a CTAS that runs with warehouse credentials. Any name outside
    // CHECK_EXPRESSION_FUNCTIONS takes the same path.
    fs::write(
        dir.join("rocky.toml"),
        clean_config(
            r#"
[pipeline.dq.checks.quarantine]
enabled = true

[[pipeline.dq.checks.assertions]]
table = "orders"
type = "expression"
expression = "my_udf(id) IS NOT NULL"
"#,
        ),
    )
    .expect("write config");

    let run = rocky(dir, &["run"]);
    let out = json(&run);
    let names = failed_checks(&out);

    assert!(
        names.iter().any(|n| n.starts_with("expression")),
        "the assertion path must refuse it: {names:?} in {out}"
    );
    assert!(
        names.iter().any(|n| n == "quarantine:compile"),
        "the quarantine path must refuse it too, with its own reason: {names:?} in {out}"
    );

    let quarantine_check = out["check_results"]
        .as_array()
        .expect("check_results")
        .iter()
        .flat_map(|t| t["checks"].as_array().expect("checks").iter())
        .find(|c| c["name"] == "quarantine:compile")
        .expect("asserted above");
    let reason = quarantine_check["not_evaluated"]
        .as_str()
        .unwrap_or_else(|| panic!("the reason must be carried: {out}"));
    assert!(
        reason.contains("my_udf"),
        "the reason must name the function to remove: {reason}"
    );

    // The split did not happen, which is the point of failing the run.
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    let split_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_name = 'orders__valid'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(split_exists, 0, "no quarantine statement may have run");
}

/// The control: an ordinary predicate still compiles and still splits.
/// Without this, the test above would also pass on a change that refused
/// every quarantine expression.
#[test]
fn an_ordinary_quarantine_predicate_still_splits() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_clean_db(dir);
    fs::write(
        dir.join("rocky.toml"),
        clean_config(
            r#"
[pipeline.dq.checks.quarantine]
enabled = true

[[pipeline.dq.checks.assertions]]
table = "orders"
type = "expression"
expression = "id >= 0"
"#,
        ),
    )
    .expect("write config");

    let run = rocky(dir, &["run"]);
    let out = json(&run);
    let names = failed_checks(&out);
    assert!(
        !names.iter().any(|n| n.starts_with("quarantine")),
        "an ordinary predicate must not be refused: {out}"
    );

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    let split_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_name = 'orders__valid'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(split_exists, 1, "the split must still happen: {out}");
}
