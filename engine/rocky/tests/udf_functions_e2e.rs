//! User-defined functions (`functions/`) end to end on DuckDB (WP11).
//!
//! A function is a `.sql` body plus a `.toml` signature beside `models/`.
//! These tests drive the real binary: `rocky compile` types a UDF call to the
//! declared return type (so a contract validates it), refuses certain
//! mistakes with E051 and flags unverifiable calls with W051, and `rocky run`
//! creates the function before the model that calls it and materializes the
//! right rows.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE raw.orders (order_id BIGINT, amount_cents BIGINT, status VARCHAR, order_date DATE);
INSERT INTO raw.orders VALUES
    (1, 1250, 'shipped', DATE '2026-01-01'),
    (2, 99, 'pending', DATE '2026-01-02'),
    (3, NULL, 'lost', DATE '2026-01-03');
CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);
";

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
type = "transformation"
models = "models"

[pipeline.probe.target.governance]
auto_create_schemas = true
"#;

const SIDECAR: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "probe"
schema = "main"
"#;

const CENTS_TOML: &str = r#"
description = "Integer cents to dollars"
returns = "DOUBLE"
deterministic = true

[[arguments]]
name = "cents"
type = "BIGINT"
"#;

/// A project with the seed in both places Rocky reads it: `data/seed.sql`
/// for `compile --with-seed`, and the DuckDB file `rocky run` writes to.
fn project(root: &Path) {
    fs::create_dir_all(root.join("models")).unwrap();
    fs::create_dir_all(root.join("functions")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    fs::write(root.join("data/seed.sql"), SEED).unwrap();
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    conn.execute_batch(SEED).unwrap();
    drop(conn);
    function(root, "cents_to_dollars", CENTS_TOML, "cents / 100.0\n");
}

fn function(root: &Path, name: &str, toml: &str, body: &str) {
    fs::write(root.join(format!("functions/{name}.toml")), toml).unwrap();
    fs::write(root.join(format!("functions/{name}.sql")), body).unwrap();
}

fn model(root: &Path, name: &str, sql: &str) {
    fs::write(root.join(format!("models/{name}.sql")), sql).unwrap();
    fs::write(root.join(format!("models/{name}.toml")), SIDECAR).unwrap();
}

fn contract(root: &Path, name: &str, column: &str, ty: &str) {
    fs::write(
        root.join(format!("models/{name}.contract.toml")),
        format!("[[columns]]\nname = \"{column}\"\ntype = \"{ty}\"\n"),
    )
    .unwrap();
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("rocky must launch")
}

fn json(out: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body: String = stdout
        .lines()
        .skip_while(|l| !l.starts_with('{'))
        .collect::<Vec<_>>()
        .join("\n");
    serde_json::from_str(&body).unwrap_or_else(|e| {
        panic!(
            "stdout must be JSON ({e})\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn compile(root: &Path) -> (Output, serde_json::Value) {
    let out = rocky(root, &["compile", "--with-seed", "--output", "json"]);
    let v = json(&out);
    (out, v)
}

fn codes(v: &serde_json::Value) -> Vec<(String, String, String)> {
    v["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["code"].as_str().unwrap().to_string(),
                d["model"].as_str().unwrap().to_string(),
                d["message"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn udf_is_created_before_its_model_and_the_contract_sees_its_return_type() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders\n",
    );
    contract(root, "fct_orders", "amount_usd", "Float64");

    // Compile: clean, the function is listed with its caller, and the
    // contract is checked (no I003 "type unknown, not checked").
    let (out, v) = compile(root);
    assert!(out.status.success(), "compile must pass: {v:#}");
    assert_eq!(codes(&v), vec![], "a valid UDF project compiles clean");
    let functions = v["functions"].as_array().expect("functions listed");
    assert_eq!(functions.len(), 1);
    assert_eq!(functions[0]["name"], "cents_to_dollars");
    assert_eq!(
        functions[0]["signature"],
        "cents_to_dollars(cents BIGINT) RETURNS DOUBLE"
    );
    assert_eq!(functions[0]["called_by"], serde_json::json!(["fct_orders"]));

    // The declared return type reaches the contract: a wrong declared type
    // is now a real E011 rather than an unchecked Unknown.
    contract(root, "fct_orders", "amount_usd", "String");
    let (out, v) = compile(root);
    assert!(
        !out.status.success(),
        "a contract mismatch must fail compile"
    );
    assert!(
        codes(&v).iter().any(|(code, model, message)| code == "E011"
            && model == "fct_orders"
            && message.contains("got Float64")),
        "E011 must name the UDF's declared type: {v:#}"
    );
    contract(root, "fct_orders", "amount_usd", "Float64");

    // Run: the macro exists before the model needs it, and the rows are right.
    let out = rocky(root, &["run", "--output", "json"]);
    let v = json(&out);
    assert!(out.status.success(), "run must succeed: {v:#}");
    assert_eq!(v["tables_failed"], 0, "{v:#}");

    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let mut stmt = conn
        .prepare("SELECT order_id, amount_usd FROM main.fct_orders ORDER BY order_id")
        .unwrap();
    let rows: Vec<(i64, Option<f64>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows, vec![(1, Some(12.5)), (2, Some(0.99)), (3, None)]);
    let landed: String = conn
        .query_row(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 'fct_orders' AND column_name = 'amount_usd'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(landed, "DOUBLE", "the macro casts to the declared type");
    drop(stmt);
    drop(conn);

    // A second run replaces the function in place (CREATE OR REPLACE).
    let out = rocky(root, &["run", "--output", "json"]);
    assert!(out.status.success(), "rerun must succeed: {:#}", json(&out));
}

#[test]
fn a_udf_selected_by_name_is_created_and_its_dependents_stay_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders\n",
    );

    // `rocky compile --model <function>` scopes to the function.
    let out = rocky(
        root,
        &[
            "compile",
            "--with-seed",
            "--model",
            "cents_to_dollars",
            "--output",
            "json",
        ],
    );
    let v = json(&out);
    assert!(out.status.success(), "{v:#}");
    assert_eq!(v["models"], 0);
    assert_eq!(v["functions"][0]["name"], "cents_to_dollars");

    let out = rocky(
        root,
        &["run", "--model", "cents_to_dollars", "--output", "json"],
    );
    assert!(out.status.success(), "{:#}", json(&out));
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let macro_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM duckdb_functions() \
             WHERE function_name = 'cents_to_dollars' AND function_type = 'macro'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(macro_exists, 1, "the selected function is created");
    let model_built: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fct_orders'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(model_built, 0, "selecting a function builds no model");
}

#[test]
fn certain_mistakes_are_e051_and_unverifiable_calls_are_w051() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    function(
        root,
        "py_score",
        "language = \"python\"\nreturns = \"DOUBLE\"\n",
        "return 1.0\n",
    );
    model(
        root,
        "bad_arity",
        "SELECT cents_to_dollars(amount_cents, 2) AS usd FROM raw.orders\n",
    );
    model(
        root,
        "bad_type",
        "SELECT cents_to_dollars(order_date) AS usd FROM raw.orders\n",
    );
    model(
        root,
        "calls_python",
        "SELECT py_score() AS score FROM raw.orders\n",
    );
    model(
        root,
        "coerced",
        "SELECT cents_to_dollars(status) AS usd FROM raw.orders\n",
    );

    let (out, v) = compile(root);
    assert!(!out.status.success(), "E051s must fail compile: {v:#}");
    let diags = codes(&v);
    let has = |code: &str, model: &str, needle: &str| {
        diags
            .iter()
            .any(|(c, m, msg)| c == code && m == model && msg.contains(needle))
    };
    assert!(
        has("E051", "py_score", "Python UDFs are not supported"),
        "{diags:#?}"
    );
    assert!(
        has("E051", "bad_arity", "with 2 argument(s) but it declares 1"),
        "{diags:#?}"
    );
    assert!(
        has(
            "E051",
            "bad_type",
            "argument 1 of `cents_to_dollars` is DATE"
        ),
        "{diags:#?}"
    );
    assert!(
        has("E051", "calls_python", "failed validation"),
        "{diags:#?}"
    );
    assert!(
        has("W051", "coerced", "converting it implicitly"),
        "{diags:#?}"
    );
    assert!(
        !diags.iter().any(|(c, m, _)| c == "E051" && m == "coerced"),
        "an implicit coercion is a warning, never a refusal: {diags:#?}"
    );

    // `rocky run` does not build a model whose UDF call is refused.
    let out = rocky(root, &["run", "--output", "json"]);
    let v = json(&out);
    assert!(!out.status.success(), "{v:#}");
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let built: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_name IN ('bad_arity', 'bad_type', 'calls_python')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(built, 0, "refused models are not built");
}

#[test]
fn a_functions_dir_does_not_disturb_valid_controls() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    // Reference corpus valid controls, plus a UDF caller with an unknown
    // argument type (a W051 at most — never an error).
    model(
        root,
        "stg_orders",
        "SELECT order_id, amount_cents FROM raw.orders\n",
    );
    model(
        root,
        "v1_lateral",
        "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders\n",
    );
    model(
        root,
        "v2_coercion",
        "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value\n",
    );
    model(
        root,
        "v3_scoped",
        "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped\n",
    );
    model(
        root,
        "v4_unknown_fn",
        "SELECT sha256(customer_name) AS customer_hash FROM raw.customers\n",
    );
    model(
        root,
        "downstream",
        "SELECT order_id, cents_to_dollars(amount_cents) AS usd FROM stg_orders\n",
    );
    model(
        root,
        "literal_arg",
        "SELECT cents_to_dollars(NULL) AS a, cents_to_dollars(100) AS b\n",
    );

    let (out, v) = compile(root);
    assert!(out.status.success(), "valid controls must compile: {v:#}");
    let errors: Vec<_> = codes(&v)
        .into_iter()
        .filter(|(code, _, _)| code.starts_with('E'))
        .collect();
    assert!(errors.is_empty(), "no new error on valid SQL: {errors:#?}");
    // Every UDF argument here has a known, assignable type (or is NULL), so
    // not even a warning fires.
    let w051: Vec<_> = codes(&v)
        .into_iter()
        .filter(|(code, _, _)| code == "W051")
        .collect();
    assert!(w051.is_empty(), "typed arguments verify cleanly: {w051:#?}");
    let usage = &v["functions"][0]["called_by"];
    assert_eq!(usage, &serde_json::json!(["downstream", "literal_arg"]));
}
