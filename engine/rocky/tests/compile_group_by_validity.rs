//! End-to-end test for the GROUP BY validity check (E044).
//!
//! Spawns the real `rocky` binary with `rocky compile --with-seed` against a
//! DuckDB seed, so source schemas come from the same loader users run. A
//! query that reads an ungrouped column must fail compile with E044 naming
//! the column. The reference controls, and a set of valid grouping shapes,
//! must stay free of E044 and of errors.

use std::fs;
use std::path::Path;
use std::process::Command;

const SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, \
    status VARCHAR, order_date DATE);\n\
    CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n";

/// The G1-S4 seed: `raw.orders` lacks `amount`, which the warehouse has.
const STALE_SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, status VARCHAR, \
    order_date DATE);\n\
    CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n";

fn write_project(root: &Path, seed: &str, models: &[(&str, &str)]) {
    let models_dir = root.join("models");
    fs::create_dir_all(&models_dir).expect("mkdir models");
    fs::create_dir_all(root.join("data")).expect("mkdir data");
    fs::write(root.join("data").join("seed.sql"), seed).expect("write seed");
    for (name, sql) in models {
        fs::write(models_dir.join(format!("{name}.sql")), sql).expect("write sql");
        fs::write(
            models_dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"poc\"\nschema = \"main\"\n"
            ),
        )
        .expect("write toml");
    }
}

/// Run `rocky compile --with-seed --output json`; return (exit code, JSON).
fn compile(seed: &str, models: &[(&str, &str)]) -> (Option<i32>, serde_json::Value) {
    let tmp = tempfile::tempdir().expect("tempdir");
    write_project(tmp.path(), seed, models);
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(tmp.path())
        .args([
            "compile",
            "--models",
            "models",
            "--with-seed",
            "--output",
            "json",
        ])
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky compile");
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    let parsed = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not JSON: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}")
    });
    (out.status.code(), parsed)
}

fn diagnostics(parsed: &serde_json::Value) -> Vec<serde_json::Value> {
    parsed
        .get("diagnostics")
        .and_then(|v| v.as_array())
        .cloned()
        .expect("diagnostics array")
}

fn assert_clean(label: &str, seed: &str, models: &[(&str, &str)]) {
    let (code, parsed) = compile(seed, models);
    let diags = diagnostics(&parsed);
    assert!(
        !diags
            .iter()
            .any(|d| d.get("code").and_then(|c| c.as_str()) == Some("E044")),
        "{label}: false E044 refusal: {diags:?}"
    );
    assert_eq!(
        parsed
            .get("has_errors")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "{label}: expected a clean compile: {diags:?}"
    );
    assert_eq!(code, Some(0), "{label}: expected exit 0: {parsed}");
}

#[test]
fn d6_ungrouped_column_refuses_with_e044() {
    let (code, parsed) = compile(
        SEED,
        &[(
            "bad_group",
            "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY customer_id",
        )],
    );
    assert_eq!(
        parsed
            .get("has_errors")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "D6 must fail compile: {parsed}"
    );
    assert_ne!(code, Some(0), "D6 must exit non-zero");
    let diags = diagnostics(&parsed);
    let e044: Vec<_> = diags
        .iter()
        .filter(|d| d.get("code").and_then(|c| c.as_str()) == Some("E044"))
        .collect();
    assert_eq!(e044.len(), 1, "exactly one E044 expected: {diags:?}");
    let diag = e044[0];
    assert_eq!(diag.get("severity").and_then(|s| s.as_str()), Some("Error"));
    assert_eq!(
        diag.get("model").and_then(|s| s.as_str()),
        Some("bad_group")
    );
    let message = diag.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        message.contains("'status'"),
        "E044 must name status: {message}"
    );
    let suggestion = diag
        .get("suggestion")
        .and_then(|m| m.as_str())
        .unwrap_or("");
    assert!(
        suggestion.contains("GROUP BY") && suggestion.contains("ANY_VALUE(status)"),
        "E044 must suggest grouping or aggregating: {suggestion}"
    );
}

#[test]
fn reference_controls_stay_clean() {
    assert_clean(
        "C2",
        SEED,
        &[
            (
                "stg_orders",
                "SELECT order_id, customer_id, amount FROM raw.orders",
            ),
            (
                "fct_revenue",
                "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
                 JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
            ),
        ],
    );
    assert_clean(
        "V1",
        SEED,
        &[(
            "v1",
            "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
        )],
    );
    assert_clean(
        "V2",
        SEED,
        &[("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value")],
    );
    assert_clean(
        "V3",
        SEED,
        &[(
            "v3",
            "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
        )],
    );
    assert_clean(
        "V4",
        SEED,
        &[(
            "v4",
            "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
        )],
    );
    assert_clean(
        "G1-S2",
        SEED,
        &[(
            "g1_s2",
            "WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) \
             SELECT stg_orders.amount FROM stg_orders",
        )],
    );
    assert_clean(
        "G1-S3",
        SEED,
        &[
            ("stg_orders", "SELECT * FROM raw.orders"),
            ("g1_s3", "SELECT s.amount FROM stg_orders AS s"),
        ],
    );
    assert_clean(
        "G1-S4",
        STALE_SEED,
        &[(
            "stg_orders",
            "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
        )],
    );
}

#[test]
fn valid_grouping_shapes_stay_clean() {
    assert_clean(
        "grouping shapes",
        SEED,
        &[
            (
                "group_all",
                "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY ALL",
            ),
            (
                "group_ordinal",
                "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY 1, 2",
            ),
            (
                "group_alias",
                "SELECT UPPER(status) AS s, COUNT(*) AS n FROM raw.orders GROUP BY s",
            ),
            (
                "group_rollup",
                "SELECT customer_id, status, SUM(amount) AS t FROM raw.orders \
                 GROUP BY ROLLUP (customer_id, status)",
            ),
            (
                "window_over_agg",
                "SELECT customer_id, SUM(SUM(amount)) OVER () AS grand FROM raw.orders \
                 GROUP BY customer_id",
            ),
            (
                "stale_unresolved",
                "SELECT customer_id, region, SUM(amount) AS t FROM raw.orders \
                 GROUP BY customer_id",
            ),
            (
                "global_agg",
                "SELECT COUNT(*) AS n, MAX(order_date) AS last_order FROM raw.orders",
            ),
        ],
    );
}
