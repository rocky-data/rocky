//! `rocky ci` reports the per-model-target compile checks of `rocky compile`
//! (E042/E043, E057, ...) and refuses before it executes a model, and a
//! dependency cycle is an E058 diagnostic in the JSON of `rocky compile` and
//! `rocky ci` rather than a bare error.
//!
//! Each case is a throwaway DuckDB project: `rocky.toml` with a DuckDB
//! adapter, a `data/seed.sql` that defines the raw tables, and SQL models.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "\
CREATE SCHEMA raw;
CREATE TABLE raw.orders AS SELECT * FROM (VALUES
  (1::BIGINT, 10::BIGINT, 5.0::DOUBLE, 'completed', DATE '2026-01-01'),
  (2::BIGINT, 11::BIGINT, 7.5::DOUBLE, 'pending', DATE '2026-01-02')
) t(order_id, customer_id, amount, status, order_date);
";

const CONFIG: &str = "\
[adapter]
type = \"duckdb\"
path = \"warehouse.duckdb\"

[pipeline.main]
type = \"transformation\"
models = \"models/**\"

[pipeline.main.target.governance]
auto_create_schemas = true
";

const FCT: &str = "SELECT order_id, customer_id, amount, status, order_date\n\
                   FROM raw.orders\n\
                   WHERE status = 'completed'\n";
const LTV: &str = "SELECT customer_id, SUM(amount) AS lifetime_value, COUNT(*) AS order_count\n\
                   FROM fct_orders\n\
                   GROUP BY customer_id\n";

fn project(models: &[(&str, &str)]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    fs::create_dir(tmp.path().join("data")).expect("create data");
    fs::write(tmp.path().join("data/seed.sql"), SEED).expect("write seed");
    let dir = tmp.path().join("models");
    fs::create_dir(&dir).expect("create models");
    for (name, sql) in models {
        fs::write(dir.join(format!("{name}.sql")), sql).expect("write model sql");
        fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .expect("write model sidecar");
    }
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("--state-path")
        .arg(root.join("state.redb"))
        .arg("--output")
        .arg("json")
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn parse(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON: {error}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// `(code, model)` of every error diagnostic.
fn errors(json: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = json["diagnostics"]
        .as_array()
        .expect("diagnostics array")
        .iter()
        .filter(|d| d["severity"] == "Error")
        .map(|d| {
            (
                d["code"].as_str().unwrap_or_default().to_string(),
                d["model"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn cycle_project() -> tempfile::TempDir {
    project(&[
        (
            "fct_orders",
            "SELECT order_id, customer_id, amount, status, order_date\n\
             FROM raw.orders\n\
             WHERE status = 'completed' AND customer_id IN (SELECT customer_id FROM customer_ltv)\n",
        ),
        ("customer_ltv", LTV),
    ])
}

fn pair(code: &str, model: &str) -> (String, String) {
    (code.to_string(), model.to_string())
}

#[test]
fn compile_reports_a_cycle_as_e058_in_its_json() {
    let tmp = cycle_project();
    let output = rocky(tmp.path(), &["compile", "--with-seed"]);
    assert_eq!(output.status.code(), Some(1));
    let json = parse(&output);
    assert_eq!(json["has_errors"], true);
    assert_eq!(
        errors(&json),
        [pair("E058", "customer_ltv"), pair("E058", "fct_orders")]
    );
    let fct = json["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["model"] == "fct_orders")
        .unwrap();
    let span = &fct["span"];
    assert!(
        span["file"].as_str().unwrap().ends_with("fct_orders.sql"),
        "{span}"
    );
    assert_eq!(span["line"], 3, "{span}");
}

#[test]
fn ci_reports_a_cycle_as_e058_and_executes_nothing() {
    let tmp = cycle_project();
    let output = rocky(tmp.path(), &["ci"]);
    assert_eq!(output.status.code(), Some(1));
    let json = parse(&output);
    assert_eq!(json["compile_ok"], false);
    assert_eq!(json["tests_passed"], 0);
    assert_eq!(
        errors(&json),
        [pair("E058", "customer_ltv"), pair("E058", "fct_orders")]
    );
}

#[test]
fn run_dag_still_refuses_a_cycle() {
    let tmp = cycle_project();
    let output = rocky(tmp.path(), &["run", "--dag"]);
    assert_ne!(output.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("circular dependency detected involving"),
        "{stderr}"
    );
}

/// A per-model-target compile error fails `rocky ci` with its code, before
/// any model executes.
#[test]
fn ci_reports_the_compile_gates_before_executing() {
    for (ltv, code) in [
        (
            LTV.replace("COUNT(*) AS order_count", "SUM(status) AS status_sum"),
            "E042",
        ),
        (
            LTV.replace(
                "SUM(amount) AS lifetime_value",
                "SUMM(amount) AS lifetime_value",
            ),
            "E057",
        ),
    ] {
        let tmp = project(&[("fct_orders", FCT), ("customer_ltv", &ltv)]);
        let output = rocky(tmp.path(), &["ci"]);
        assert_eq!(output.status.code(), Some(1), "{code}");
        let json = parse(&output);
        assert_eq!(errors(&json), [pair(code, "customer_ltv")], "{json}");
        assert_eq!(json["tests_passed"], 0, "{code}: nothing executes: {json}");
    }

    let fct = FCT.replace("WHERE status", "WHERE order_date > 5 AND status");
    let tmp = project(&[("fct_orders", &fct), ("customer_ltv", LTV)]);
    let output = rocky(tmp.path(), &["ci"]);
    assert_eq!(output.status.code(), Some(1));
    let json = parse(&output);
    assert_eq!(errors(&json), [pair("E043", "fct_orders")], "{json}");
    assert_eq!(json["tests_passed"], 0, "{json}");

    // `--models` scope: the same check, judged against the same pipeline.
    let output = rocky(tmp.path(), &["ci", "--models", "models"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(errors(&parse(&output)), [pair("E043", "fct_orders")]);
}

/// Valid SQL the checks must not refuse: an implicit cast DuckDB accepts, a
/// reused `SELECT` alias, a CTE, a correlated sub-query, and a sub-query read
/// of an upstream model that does not close a cycle.
#[test]
fn ci_passes_valid_controls() {
    let controls = [
        FCT.replace("WHERE status = 'completed'", "WHERE order_id >= '1'"),
        "SELECT amount * 2 AS doubled, doubled + 1 AS bumped FROM raw.orders\n".to_string(),
        "WITH c AS (SELECT customer_id, amount FROM raw.orders)\n\
         SELECT customer_id, SUM(amount) AS total FROM c GROUP BY customer_id\n"
            .to_string(),
        "SELECT o.order_id, (SELECT MAX(i.amount) FROM raw.orders i \
         WHERE i.customer_id = o.customer_id) AS max_amount FROM raw.orders o\n"
            .to_string(),
    ];
    for control in controls {
        let tmp = project(&[
            ("fct_orders", FCT),
            ("customer_ltv", LTV),
            ("control", &control),
        ]);
        let output = rocky(tmp.path(), &["ci"]);
        let json = parse(&output);
        assert_eq!(output.status.code(), Some(0), "{control}\n{json}");
        assert!(errors(&json).is_empty(), "{control}\n{json}");
        assert_eq!(json["tests_passed"], 3, "{control}\n{json}");
    }

    let tmp = project(&[
        ("fct_orders", FCT),
        ("customer_ltv", LTV),
        (
            "top_orders",
            "SELECT order_id FROM fct_orders\n\
             WHERE customer_id IN (SELECT customer_id FROM customer_ltv)\n",
        ),
    ]);
    let output = rocky(tmp.path(), &["ci"]);
    let json = parse(&output);
    assert_eq!(output.status.code(), Some(0), "{json}");
    assert!(errors(&json).is_empty(), "{json}");
}
