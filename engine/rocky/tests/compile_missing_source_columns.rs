//! E041 / W041 end-to-end: a model that reads a column its external source
//! lacks, through the real `rocky` binary.
//!
//! Uses the reference corpus: `raw.orders` / `raw.customers` from a DuckDB
//! seed, case D1 (`order_total` does not exist), the valid controls, and the
//! stale-seed case G1-S4. Covers `rocky compile --with-seed` (W041, exit 0),
//! `--strict-sources` (E041, refused), and `rocky run` refusing an E041 model
//! found against a trusted schema-cache entry before it touches the warehouse.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, \
    status VARCHAR, order_date DATE);\n\
    CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n";

const D1: &str = "SELECT order_id, customer_id, order_total FROM raw.orders";

fn write_model(root: &Path, name: &str, sql: &str) {
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
}

fn seeded(seed: &str, models: &[(&str, &str)]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    fs::create_dir(tmp.path().join("data")).unwrap();
    fs::write(tmp.path().join("data/seed.sql"), seed).unwrap();
    for (name, sql) in models {
        write_model(tmp.path(), name, sql);
    }
    tmp
}

fn compile(root: &Path, extra: &[&str]) -> (Output, serde_json::Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .args(["--output", "json", "compile", "--with-seed"])
        .args(extra)
        .output()
        .expect("spawn rocky compile");
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON: {e}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, report)
}

fn with_code<'a>(report: &'a serde_json::Value, code: &str) -> Vec<&'a serde_json::Value> {
    report["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["code"] == code)
        .collect()
}

#[test]
fn d1_with_seed_warns_w041_and_exits_zero() {
    let tmp = seeded(SEED, &[("stg_orders", D1)]);
    let (output, report) = compile(tmp.path(), &[]);
    assert!(output.status.success(), "W041 must not fail: {report}");
    assert_eq!(report["has_errors"], false);
    let w041 = with_code(&report, "W041");
    assert_eq!(w041.len(), 1, "{report}");
    assert_eq!(w041[0]["severity"], "Warning");
    let message = w041[0]["message"].as_str().unwrap();
    assert!(message.contains("'order_total'"), "{message}");
    assert!(message.contains("'raw.orders'"), "{message}");
    let suggestion = w041[0]["suggestion"].as_str().unwrap();
    assert!(suggestion.contains("refresh the schema"), "{suggestion}");
}

#[test]
fn d1_with_seed_and_strict_sources_refuses_e041() {
    let tmp = seeded(SEED, &[("stg_orders", D1)]);
    let (output, report) = compile(tmp.path(), &["--strict-sources"]);
    assert!(!output.status.success(), "strict D1 must refuse: {report}");
    assert_eq!(report["has_errors"], true);
    let e041 = with_code(&report, "E041");
    assert_eq!(e041.len(), 1, "{report}");
    assert_eq!(e041[0]["model"], "stg_orders");
    assert!(with_code(&report, "W041").is_empty());
}

#[test]
fn reference_valid_controls_stay_clean_under_strict_sources() {
    let tmp = seeded(
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
            (
                "v1",
                "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
            ),
            ("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value"),
            (
                "v3",
                "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
            ),
            (
                "v4",
                "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
            ),
            (
                "g1_s2",
                "WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) \
                 SELECT stg_orders.amount FROM stg_orders",
            ),
            ("stg_star", "SELECT * FROM raw.orders"),
            ("g1_s3", "SELECT s.amount FROM stg_star AS s"),
        ],
    );
    let (output, report) = compile(tmp.path(), &["--strict-sources"]);
    assert!(with_code(&report, "E041").is_empty(), "{report}");
    assert!(with_code(&report, "W041").is_empty(), "{report}");
    assert!(output.status.success(), "controls must compile: {report}");
}

#[test]
fn g1_s4_stale_seed_stays_exit_zero() {
    // The seed lacks `amount`; the warehouse has it.
    let tmp = seeded(
        "CREATE SCHEMA raw;\n\
         CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, status VARCHAR);\n",
        &[(
            "stg_orders",
            "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
        )],
    );
    let (output, report) = compile(tmp.path(), &[]);
    assert!(
        output.status.success(),
        "stale seed must not refuse: {report}"
    );
    assert_eq!(with_code(&report, "W041").len(), 1, "{report}");
}

/// `rocky run` compiles against the schema cache before executing. A cache
/// entry inside `[cache.schemas] trusted_max_age_seconds` is authoritative, so
/// D1 is excluded with E041 and never reaches the warehouse.
#[test]
fn run_refuses_d1_against_trusted_cache_before_touching_the_warehouse() {
    use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use rocky_core::state::StateStore;

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    write_model(root, "stg_orders", D1);
    fs::write(
        root.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\n\
         [pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
         [pipeline.t.target.governance]\nauto_create_schemas = true\n\n\
         [cache.schemas]\ntrusted_max_age_seconds = 3600\n",
    )
    .unwrap();
    let state_path = root.join("state.redb");
    {
        let store = StateStore::open(&state_path).unwrap();
        let columns = ["order_id", "customer_id", "amount", "status", "order_date"]
            .into_iter()
            .map(|name| StoredColumn {
                name: name.into(),
                data_type: "BIGINT".into(),
                nullable: true,
            })
            .collect();
        store
            .write_schema_cache_entry(
                &schema_cache_key("warehouse", "raw", "orders"),
                &SchemaCacheEntry {
                    columns,
                    cached_at: chrono::Utc::now(),
                },
            )
            .unwrap();
    }

    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("--state-path")
        .arg(&state_path)
        .args(["--output", "json", "run", "--pipeline", "t"])
        .output()
        .expect("spawn rocky run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "E041 must fail the run\nstdout: {stdout}\nstderr: {stderr}"
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON: {e}\n{stdout}\nstderr: {stderr}"));
    let errors = report["errors"].as_array().expect("errors array");
    assert!(
        errors
            .iter()
            .any(|e| e["asset_key"] == serde_json::json!(["stg_orders"])
                && e["error"].as_str().is_some_and(|m| m.contains("[E041]"))),
        "run must report E041 for stg_orders: {report}"
    );

    // Nothing was materialized.
    let warehouse = root.join("warehouse.duckdb");
    if warehouse.exists() {
        let conn = duckdb::Connection::open(&warehouse).unwrap();
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'stg_orders'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0, "the refused model must not be materialized");
    }
}
