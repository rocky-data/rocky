//! `rocky ci`'s process exit code and its JSON `exit_code` describe the same run.
//!
//! They did not. A clean run with warnings printed `"exit_code": 4` and exited
//! `0`, so a CI step gating on `$?` and one gating on `jq .exit_code` reached
//! opposite conclusions about the same run (#2030). Since the fix, a clean run
//! is `0` on both surfaces, warnings or not, and a failed run is `1` on both.
//!
//! The warnings-only fixture leans on `W056`: an `incremental` model with
//! neither `lookback` nor `unique_key`. That is a warning by design, and it
//! leaves compile and tests passing, which is exactly the corner where the two
//! surfaces used to disagree.

use std::fs;
use std::process::{Command, Output};

const RAW_SQL: &str = "SELECT * FROM (VALUES (1, 10.0, TIMESTAMP '2026-01-01 00:00:00')) \
     AS t(order_id, amount, updated_at)\n";
const RAW_TOML: &str = "[target]\ncatalog = \"w\"\nschema = \"s\"\n";

/// `updated_at` passes straight through from `raw_orders`, so the watermark
/// is provable (no E046).
const ORDERS_SQL: &str = "SELECT order_id, amount, updated_at FROM raw_orders\n";

/// An append-only incremental model with no `lookback` and no `unique_key`:
/// the W056 case.
const ORDERS_TOML: &str = "depends_on = [\"raw_orders\"]\n\
     [strategy]\ntype = \"incremental\"\ntimestamp_column = \"updated_at\"\n\
     [target]\ncatalog = \"w\"\nschema = \"s\"\n";

/// A full-refresh `orders`, so no warning.
const PLAIN_TOML: &str =
    "depends_on = [\"raw_orders\"]\n[target]\ncatalog = \"w\"\nschema = \"s\"\n";

const SUMMARY_SQL: &str = "SELECT COUNT(*) AS n FROM orders\n";
const SUMMARY_TOML: &str = "depends_on = [\"orders\"]\n[target]\ncatalog = \"w\"\nschema = \"s\"\n";

/// A model that reads a column `orders` does not have, so compile fails.
const BROKEN_SUMMARY_SQL: &str = "SELECT no_such_column FROM orders\n";

fn run_ci(dir: &std::path::Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "ci", "--models"])
        .arg(dir.join("models"))
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("the rocky binary runs")
}

fn write_project(dir: &std::path::Path, orders_sidecar: &str, summary_sql: &str) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("raw_orders.sql"), RAW_SQL).unwrap();
    fs::write(models.join("raw_orders.toml"), RAW_TOML).unwrap();
    fs::write(models.join("orders.sql"), ORDERS_SQL).unwrap();
    fs::write(models.join("orders.toml"), orders_sidecar).unwrap();
    fs::write(models.join("summary.sql"), summary_sql).unwrap();
    fs::write(models.join("summary.toml"), SUMMARY_TOML).unwrap();
}

/// Read `exit_code` out of the JSON payload without pulling in a JSON crate:
/// the field is an integer, so the digits after the key are the whole value.
fn json_exit_code(stdout: &str) -> i32 {
    let at = stdout
        .find("\"exit_code\"")
        .unwrap_or_else(|| panic!("no exit_code in: {stdout}"));
    let rest = &stdout[at..];
    let colon = rest.find(':').expect("exit_code has a value");
    rest[colon + 1..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect::<String>()
        .parse()
        .expect("exit_code is an integer")
}

/// The warnings-only run: compile and tests pass, a warning is emitted, and
/// both surfaces say 0.
#[test]
fn a_warnings_only_ci_run_exits_with_the_code_its_json_reports() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path(), ORDERS_TOML, SUMMARY_SQL);

    let out = run_ci(tmp.path());
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        stdout.contains("\"compile_ok\": true") && stdout.contains("\"tests_ok\": true"),
        "the fixture must compile and test clean, or it is testing the wrong corner: {stdout}"
    );
    assert!(
        stdout.contains("\"W056\""),
        "the fixture must emit its warning, or it is testing the wrong corner: {stdout}"
    );
    assert_eq!(
        json_exit_code(&stdout),
        0,
        "a clean run with warnings reports 0: {stdout}"
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "the process must exit with the code the JSON reports: {stdout}"
    );
}

/// A clean run with no warning is 0 on both surfaces too.
#[test]
fn a_clean_ci_run_exits_zero_on_both_surfaces() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path(), PLAIN_TOML, SUMMARY_SQL);

    let out = run_ci(tmp.path());
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(!stdout.contains("\"W056\""), "{stdout}");
    assert_eq!(json_exit_code(&stdout), 0, "{stdout}");
    assert_eq!(out.status.code(), Some(0), "{stdout}");
}

/// A failed run is 1 on both surfaces, so the agreement is not just "always 0".
#[test]
fn a_failed_ci_run_exits_one_on_both_surfaces() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path(), PLAIN_TOML, BROKEN_SUMMARY_SQL);

    let out = run_ci(tmp.path());
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(stdout.contains("\"compile_ok\": false"), "{stdout}");
    assert_eq!(json_exit_code(&stdout), 1, "{stdout}");
    assert_eq!(out.status.code(), Some(1), "{stdout}");
}
