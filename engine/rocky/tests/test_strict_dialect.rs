//! `rocky test` on a project whose warehouse is not DuckDB (#2755).
//!
//! Fixture `[[test]]` blocks run on DuckDB. The binary must say so in table
//! output, and `--strict-dialect` must refuse the test and fail the run. The
//! Snowflake adapter is never connected to, so no credentials are needed.

use std::fs;
use std::process::{Command, Output};

const SNOWFLAKE_TOML: &str = "[adapter.snow]\ntype = \"snowflake\"\naccount = \"example\"\n\
     [pipeline.trans]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
     [pipeline.trans.target]\nadapter = \"snow\"\n";

fn write_project(dir: &std::path::Path) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("orders.sql"), "SELECT 0 AS id, 0 AS amount").unwrap();
    fs::write(
        models.join("orders.toml"),
        "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"wh\"\nschema = \"main\"\n",
    )
    .unwrap();
    fs::write(
        models.join("flagged.sql"),
        "SELECT id, amount FROM orders WHERE amount > 100",
    )
    .unwrap();
    fs::write(
        models.join("flagged.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\
         [target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
         [[test]]\nname = \"keeps_high\"\n\n\
         [[test.given]]\nref = \"orders\"\n\
         rows = [ { id = 1, amount = 150 }, { id = 2, amount = 50 } ]\n\n\
         [test.expect]\nrows = [ { id = 1, amount = 150 } ]\n",
    )
    .unwrap();
    fs::write(dir.join("rocky.toml"), SNOWFLAKE_TOML).unwrap();
}

fn rocky(dir: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("the rocky binary runs")
}

#[test]
fn table_output_warns_and_marks_each_unit_test() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path());
    let out = rocky(tmp.path(), &["--output", "table", "test"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}");
    assert!(
        stdout.contains("warning: 1 unit test(s) ran on DuckDB"),
        "{stdout}"
    );
    assert!(
        stdout.contains("flagged::keeps_high [ran on DuckDB, target Snowflake]"),
        "{stdout}"
    );
}

#[test]
fn strict_dialect_refuses_and_fails_the_run() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path());
    let out = rocky(
        tmp.path(),
        &["--output", "json", "test", "--strict-dialect"],
    );
    assert!(!out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    let result = &json["unit_tests"]["results"][0];
    assert_eq!(result["passed"], false, "{json}");
    assert_eq!(result["dialect"]["refused"], true, "{json}");
    assert_eq!(result["dialect"]["ran_on_duckdb"], false, "{json}");
}

#[test]
fn strict_dialect_conflicts_with_declarative() {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path());
    let out = rocky(tmp.path(), &["test", "--strict-dialect", "--declarative"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cannot be used with"), "{stderr}");
}
