//! `--strict-contracts` / `[contracts] strict = true` end to end, through the
//! real `rocky` binary: a contract that declares a column type Rocky cannot
//! check is `I003` (info) by default and the `E059` error when strict.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, amount DECIMAL(10, 2));\n\
    INSERT INTO raw.orders VALUES (1, 10.50), (2, 20.00);\n";

fn write_model(root: &Path, name: &str, sql: &str, column: &str, type_name: &str) {
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
    let contracts = root.join("contracts");
    fs::create_dir_all(&contracts).unwrap();
    fs::write(
        contracts.join(format!("{name}.contract.toml")),
        format!("[[columns]]\nname = \"{column}\"\ntype = \"{type_name}\"\n"),
    )
    .unwrap();
}

fn project(model: &str, sql: &str, column: &str, type_name: &str, seed: bool) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    write_model(tmp.path(), model, sql, column, type_name);
    if seed {
        fs::create_dir(tmp.path().join("data")).unwrap();
        fs::write(tmp.path().join("data/seed.sql"), SEED).unwrap();
    }
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> (Output, serde_json::Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("spawn rocky");
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON: {e}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, report)
}

fn compile(root: &Path, extra: &[&str]) -> (Output, serde_json::Value) {
    let mut args = vec!["compile", "--models", "models", "--contracts", "contracts"];
    args.extend_from_slice(extra);
    rocky(root, &args)
}

fn codes(report: &serde_json::Value) -> Vec<String> {
    report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap().to_string())
        .collect()
}

fn diagnostic<'a>(report: &'a serde_json::Value, code: &str) -> &'a serde_json::Value {
    report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["code"] == code)
        .unwrap_or_else(|| panic!("no {code} in {report}"))
}

const UNKNOWN: &str = "SELECT order_id FROM raw.orders";

#[test]
fn an_unchecked_contract_type_is_i003_and_exits_zero_by_default() {
    let tmp = project("m", UNKNOWN, "order_id", "Int64", false);
    let (output, report) = compile(tmp.path(), &[]);
    assert!(output.status.success(), "{report}");
    assert!(codes(&report).contains(&"I003".to_string()), "{report}");
    assert!(!codes(&report).contains(&"E059".to_string()), "{report}");
}

#[test]
fn strict_flag_makes_it_e059_naming_model_column_reason_and_fix() {
    let tmp = project("m", UNKNOWN, "order_id", "Int64", false);
    let (output, report) = compile(tmp.path(), &["--strict-contracts"]);
    assert!(!output.status.success(), "{report}");
    assert_eq!(report["has_errors"], true);
    assert!(!codes(&report).contains(&"I003".to_string()), "{report}");
    let e059 = diagnostic(&report, "E059");
    assert_eq!(e059["severity"], "Error");
    let message = e059["message"].as_str().unwrap();
    assert!(message.contains("'order_id'"), "{message}");
    assert!(message.contains("'m'"), "{message}");
    assert!(message.contains("Int64"), "{message}");
    assert!(message.contains("raw.orders"), "the reason: {message}");
    let fix = e059["suggestion"].as_str().unwrap();
    assert!(fix.contains("cast") && fix.contains("source"), "{fix}");
}

#[test]
fn config_key_is_the_same_as_the_flag() {
    let tmp = project("m", UNKNOWN, "order_id", "Int64", false);
    fs::write(
        tmp.path().join("rocky.toml"),
        "[contracts]\nstrict = true\n",
    )
    .unwrap();
    let (output, report) = compile(tmp.path(), &[]);
    assert!(!output.status.success(), "{report}");
    assert!(codes(&report).contains(&"E059".to_string()), "{report}");
}

#[test]
fn a_cast_to_a_specified_type_clears_it_and_is_checked_against_the_contract() {
    let sql = "SELECT CAST(amount AS DECIMAL(12, 2)) AS amount FROM raw.orders";
    let tmp = project("m", sql, "amount", "Decimal(12,2)", false);
    let (output, report) = compile(tmp.path(), &["--strict-contracts"]);
    assert!(output.status.success(), "{report}");
    assert!(!codes(&report).contains(&"E059".to_string()), "{report}");

    // The cast target is what the contract is compared with.
    let tmp = project("m", sql, "amount", "String", false);
    let (output, report) = compile(tmp.path(), &["--strict-contracts"]);
    assert!(!output.status.success(), "{report}");
    assert!(codes(&report).contains(&"E011".to_string()), "{report}");
}

#[test]
fn a_cast_to_a_bare_decimal_is_still_unchecked() {
    let sql = "SELECT CAST(amount AS DECIMAL) AS amount FROM raw.orders";
    let tmp = project("m", sql, "amount", "Decimal(12,2)", true);
    let (output, report) = compile(tmp.path(), &["--with-seed", "--strict-contracts"]);
    assert!(!output.status.success(), "{report}");
    let message = diagnostic(&report, "E059")["message"].as_str().unwrap();
    assert!(message.contains("DECIMAL"), "{message}");
}

#[test]
fn a_contract_type_that_resolves_from_the_seed_is_not_refused_when_strict() {
    let tmp = project("m", UNKNOWN, "order_id", "Int64", true);
    let (output, report) = compile(tmp.path(), &["--with-seed", "--strict-contracts"]);
    assert!(output.status.success(), "{report}");
    assert!(!codes(&report).contains(&"E059".to_string()), "{report}");
}

#[cfg(feature = "duckdb")]
#[test]
fn ci_refuses_an_unchecked_contract_type_only_when_strict() {
    // `AVG` over a DECIMAL column: the result type depends on the warehouse,
    // so it stays unknown even with the seed's schema.
    let sql = "SELECT AVG(amount) AS avg_amount FROM raw.orders";
    let tmp = project("m", sql, "avg_amount", "Float64", true);
    let args = ["ci", "--models", "models", "--contracts", "contracts"];

    let (output, report) = rocky(tmp.path(), &args);
    assert!(output.status.success(), "{report}");

    let mut strict = args.to_vec();
    strict.push("--strict-contracts");
    let (output, report) = rocky(tmp.path(), &strict);
    assert!(!output.status.success(), "{report}");
    assert!(
        report["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "E059"),
        "{report}"
    );
}
