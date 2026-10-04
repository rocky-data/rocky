//! End-to-end coverage for the aggregate-argument (E042/W042) and
//! comparison-operand (E043/W043) checks in `rocky compile --with-seed`.
//!
//! Each case is a throwaway DuckDB project: `rocky.toml` with a DuckDB
//! adapter, a `data/seed.sql` that defines the raw tables, and SQL models.
//! The adapter type is what selects the DuckDB verdicts.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "\
CREATE SCHEMA raw;
CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, order_date DATE);
CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);
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

const NEW_CODES: [&str; 4] = ["E042", "W042", "E043", "W043"];

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

fn compile(root: &Path, extra: &[&str]) -> (Output, serde_json::Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("--state-path")
        .arg(root.join("state.redb"))
        .arg("--output")
        .arg("json")
        .arg("compile")
        .arg("--models")
        .arg(root.join("models"))
        .arg("--with-seed")
        .args(extra)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky compile");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json = serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON: {error}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, json)
}

/// `(code, severity)` of every diagnostic carrying one of this package's codes.
fn operand_diags(json: &serde_json::Value) -> Vec<(String, String)> {
    json["diagnostics"]
        .as_array()
        .expect("diagnostics array")
        .iter()
        .filter(|d| NEW_CODES.contains(&d["code"].as_str().unwrap_or_default()))
        .map(|d| {
            (
                d["code"].as_str().unwrap().to_string(),
                d["severity"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn d2_sum_over_varchar_fails_compile_on_duckdb() {
    let tmp = project(&[(
        "bad_agg",
        "SELECT customer_id, SUM(customer_name) AS s FROM raw.customers GROUP BY customer_id",
    )]);
    let (output, json) = compile(tmp.path(), &[]);
    assert!(!output.status.success(), "{json}");
    assert_eq!(json["has_errors"], true);
    assert_eq!(
        operand_diags(&json),
        vec![("E042".to_string(), "Error".to_string())],
        "{json}"
    );
}

#[test]
fn d5_varchar_join_key_warns_and_escalates_with_deny_warnings() {
    let tmp = project(&[(
        "bad_join",
        "SELECT o.order_id, c.customer_name FROM raw.orders o \
         JOIN raw.customers c ON o.customer_id = c.customer_name",
    )]);

    let (output, json) = compile(tmp.path(), &[]);
    assert!(output.status.success(), "a warning must not fail: {json}");
    assert_eq!(
        operand_diags(&json),
        vec![("W043".to_string(), "Warning".to_string())],
        "{json}"
    );

    let (output, json) = compile(tmp.path(), &["--deny-warnings", "W042,W043"]);
    assert!(!output.status.success(), "{json}");
    assert_eq!(json["has_errors"], true);
    assert_eq!(
        operand_diags(&json),
        vec![("W043".to_string(), "Error".to_string())],
        "{json}"
    );

    // An explicit target dialect wins over the adapter type.
    let (output, json) = compile(tmp.path(), &["--target-dialect", "bq"]);
    assert!(!output.status.success(), "{json}");
    assert!(
        operand_diags(&json).contains(&("E043".to_string(), "Error".to_string())),
        "{json}"
    );
}

#[test]
fn valid_controls_compile_clean() {
    let tmp = project(&[
        // C2
        (
            "stg_orders",
            "SELECT order_id, customer_id, amount FROM raw.orders",
        ),
        (
            "fct_revenue",
            "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
             JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
        ),
        // V1
        (
            "v1",
            "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
        ),
        // V2
        ("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value"),
        // V3
        (
            "v3",
            "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
        ),
        // V4
        (
            "v4",
            "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
        ),
        // Aggregate overloads and temporal / literal comparisons.
        (
            "aggs",
            "SELECT status, AVG(order_id) AS a, MAX(status) AS b, COUNT(*) AS c, \
             COUNT(DISTINCT customer_id) AS d, SUM(amount) AS e FROM raw.orders \
             WHERE order_date >= '2024-01-01' AND customer_id = '42' GROUP BY status",
        ),
    ]);
    let (output, json) = compile(tmp.path(), &["--deny-warnings", "W042,W043"]);
    assert!(output.status.success(), "{json}");
    assert_eq!(json["has_errors"], false, "{json}");
    assert!(operand_diags(&json).is_empty(), "{json}");
}
