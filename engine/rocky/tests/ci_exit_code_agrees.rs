//! `rocky ci`'s process exit code and its JSON `exit_code` describe the same run.
//!
//! They did not. The command exited only `if !result.passed()`, and `passed()`
//! is `compile_ok && tests_ok` — which a warnings-only run satisfies. So a run
//! that compiled and tested clean but emitted an advisory warning printed
//! `"exit_code": 4` and exited 0. A CI step gating on `$?` and one gating on
//! `jq .exit_code` reached opposite conclusions about the same run (#2030).
//!
//! The fixture leans on `D012`: a bare read of a model whose configured target
//! table has a different name. That is a warning by design — the edge may be
//! false on a warehouse run — and it leaves compile and tests passing, which is
//! exactly the corner where the two surfaces used to disagree.

use std::fs;
use std::process::{Command, Output};

const ORDERS_SQL: &str = "SELECT * FROM (VALUES (1, 10.0)) AS t(order_id, amount)\n";

/// `orders`'s target table is `orders_v2`, so a BARE read of `orders`
/// downstream is the D012 case.
const ORDERS_TOML: &str = "[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders_v2\"\n";

const SUMMARY_SQL: &str = "SELECT COUNT(*) AS n FROM orders\n";
const SUMMARY_TOML: &str =
    "depends_on = [\"orders\"]\n[target]\ncatalog = \"w\"\nschema = \"s\"\n";

/// A clean project: no renamed target, so no warning.
const PLAIN_TOML: &str = "[target]\ncatalog = \"w\"\nschema = \"s\"\n";

fn run_ci(dir: &std::path::Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "ci", "--models"])
        .arg(dir.join("models"))
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("the rocky binary runs")
}

fn write_project(dir: &std::path::Path, orders_sidecar: &str) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("orders.sql"), ORDERS_SQL).unwrap();
    fs::write(models.join("orders.toml"), orders_sidecar).unwrap();
    fs::write(models.join("summary.sql"), SUMMARY_SQL).unwrap();
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
/// BOTH surfaces say 4.
#[test]
fn a_warnings_only_ci_run_exits_with_the_code_its_json_reports() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path(), ORDERS_TOML);

    let out = run_ci(tmp.path());
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert!(
        stdout.contains("\"compile_ok\": true") && stdout.contains("\"tests_ok\": true"),
        "the fixture must compile and test clean, or it is testing the wrong corner: {stdout}"
    );
    assert_eq!(
        json_exit_code(&stdout),
        4,
        "a warning should report 4: {stdout}"
    );
    assert_eq!(
        out.status.code(),
        Some(4),
        "the process must exit with the code the JSON reports: {stdout}"
    );
}

/// And the clean run still exits 0 on both surfaces, so the fix did not simply
/// make `rocky ci` fail more often.
#[test]
fn a_clean_ci_run_exits_zero_on_both_surfaces() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path(), PLAIN_TOML);

    let out = run_ci(tmp.path());
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert_eq!(json_exit_code(&stdout), 0, "{stdout}");
    assert_eq!(out.status.code(), Some(0), "{stdout}");
}
