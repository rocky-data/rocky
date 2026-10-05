//! Integration test for `rocky compile` on a ClickHouse project (E053 /
//! W053), credential-free: compile never connects.
//!
//! Spawns the real `rocky` binary. Proves:
//!
//! - with ClickHouse as the only warehouse, a `merge` model and an
//!   `incremental` model with a `unique_key` are E053 errors;
//! - an invalid `[clickhouse]` block and a `[clickhouse]` block on a `view`
//!   are E053 errors;
//! - an `order_by` column the model does not output is a W053 warning only;
//! - valid controls stay clean: `full_refresh`, `view`, `delete_insert`,
//!   `incremental` append, `time_interval`, and a correct `[clickhouse]`
//!   block;
//! - a project that also configures DuckDB does not refuse `merge` at
//!   compile time.

use std::fs;
use std::path::Path;
use std::process::Command;

const CLICKHOUSE: &str = "[adapter.ch]\ntype = \"clickhouse\"\nhost = \"localhost\"\n";

fn project(adapters: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::create_dir(tmp.path().join("models")).expect("mkdir");
    fs::write(
        tmp.path().join("rocky.toml"),
        format!(
            "{adapters}\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             target = {{ adapter = \"ch\" }}\n"
        ),
    )
    .expect("write rocky.toml");
    fs::write(
        tmp.path().join("models/_defaults.toml"),
        "[target]\ncatalog = \"\"\nschema = \"marts\"\n",
    )
    .expect("write defaults");
    tmp
}

fn model(root: &Path, name: &str, sql: &str, toml: &str) {
    let dir = root.join("models");
    fs::write(dir.join(format!("{name}.sql")), sql).expect("write sql");
    fs::write(dir.join(format!("{name}.toml")), toml).expect("write toml");
}

fn compile_json(root: &Path) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(["compile", "--models", "models", "--output", "json"])
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky compile");
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not one JSON document: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    })
}

fn codes_for<'a>(parsed: &'a serde_json::Value, model: &str) -> Vec<(&'a str, &'a str)> {
    parsed
        .get("diagnostics")
        .and_then(|v| v.as_array())
        .expect("diagnostics array")
        .iter()
        .filter(|d| d.get("model").and_then(|m| m.as_str()) == Some(model))
        .filter_map(|d| Some((d.get("code")?.as_str()?, d.get("severity")?.as_str()?)))
        .collect()
}

fn has_errors(parsed: &serde_json::Value) -> Option<bool> {
    parsed
        .get("has_errors")
        .and_then(serde_json::Value::as_bool)
}

#[test]
fn merge_and_keyed_incremental_are_e053_on_a_clickhouse_project() {
    let tmp = project(CLICKHOUSE);
    let root = tmp.path();
    model(
        root,
        "dim_customers",
        "SELECT 1 AS customer_id, 'a' AS customer_name\n",
        "[strategy]\ntype = \"merge\"\nunique_key = [\"customer_id\"]\n",
    );
    model(
        root,
        "fct_events",
        "SELECT 1 AS id, now() AS ts FROM system.one WHERE @incremental_filter\n",
        "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\nunique_key = [\"id\"]\n",
    );
    let parsed = compile_json(root);
    assert_eq!(has_errors(&parsed), Some(true), "{parsed}");
    for name in ["dim_customers", "fct_events"] {
        assert!(
            codes_for(&parsed, name).contains(&("E053", "Error")),
            "{name}: {parsed}"
        );
    }
}

#[test]
fn bad_table_options_are_e053_and_a_missing_sort_column_is_w053() {
    let tmp = project(CLICKHOUSE);
    let root = tmp.path();
    model(
        root,
        "fct_bad_engine",
        "SELECT 1 AS id\n",
        "[clickhouse]\nengine = \"ReplacingMergeTree(ver)\"\n",
    );
    model(
        root,
        "v_orders",
        "SELECT 1 AS id\n",
        "[strategy]\ntype = \"view\"\n\n[clickhouse]\norder_by = [\"id\"]\n",
    );
    let parsed = compile_json(root);
    assert_eq!(has_errors(&parsed), Some(true), "{parsed}");
    for name in ["fct_bad_engine", "v_orders"] {
        assert!(
            codes_for(&parsed, name).contains(&("E053", "Error")),
            "{name}: {parsed}"
        );
    }

    let tmp = project(CLICKHOUSE);
    let root = tmp.path();
    model(
        root,
        "fct_typo",
        "SELECT 1 AS customer_id, 2 AS amount\n",
        "[clickhouse]\norder_by = [\"region\"]\n",
    );
    let parsed = compile_json(root);
    assert_eq!(
        has_errors(&parsed),
        Some(false),
        "W053 must not fail the build: {parsed}"
    );
    assert!(
        codes_for(&parsed, "fct_typo").contains(&("W053", "Warning")),
        "{parsed}"
    );
}

/// No false refusals: every strategy ClickHouse runs compiles clean.
#[test]
fn supported_strategies_and_valid_options_stay_clean() {
    let tmp = project(CLICKHOUSE);
    let root = tmp.path();
    model(
        root,
        "stg_orders",
        "SELECT 1 AS order_id, 2 AS customer_id, 3.5 AS amount, DATE '2026-01-01' AS order_date\n",
        "[strategy]\ntype = \"view\"\n",
    );
    model(
        root,
        "fct_orders",
        "SELECT order_id, customer_id, amount, order_date FROM stg_orders\n",
        "[clickhouse]\nengine = \"MergeTree\"\norder_by = [\"customer_id\", \"order_date\"]\n\
         partition_by = \"toYYYYMM(order_date)\"\n",
    );
    model(
        root,
        "orders_by_customer",
        "SELECT order_id, customer_id FROM stg_orders\n",
        "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"customer_id\"]\n",
    );
    model(
        root,
        "events_append",
        "SELECT order_id, order_date FROM stg_orders WHERE @incremental_filter\n",
        "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"order_date\"\n",
    );
    model(
        root,
        "daily_revenue",
        "SELECT order_date, SUM(amount) AS revenue FROM stg_orders \
         WHERE order_date >= toDate(@start_date) AND order_date < toDate(@end_date) \
         GROUP BY order_date\n",
        "[strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
         granularity = \"day\"\nfirst_partition = \"2026-01-01\"\n",
    );
    let parsed = compile_json(root);
    assert_eq!(has_errors(&parsed), Some(false), "{parsed}");
    for name in [
        "stg_orders",
        "fct_orders",
        "orders_by_customer",
        "events_append",
        "daily_revenue",
    ] {
        let codes = codes_for(&parsed, name);
        assert!(
            !codes
                .iter()
                .any(|(c, s)| *s == "Error" || *c == "W053" || *c == "E053"),
            "{name} must stay clean: {codes:?}"
        );
    }
}

/// The red-team repro: the pipeline targets ClickHouse, and a DuckDB adapter
/// that no pipeline targets sits beside it. That unused adapter used to hide
/// E053, and `rocky run` then failed with "ClickHouse has no MERGE".
#[test]
fn merge_on_a_clickhouse_target_is_refused_even_with_an_unused_capable_adapter() {
    let tmp = project(&format!(
        "{CLICKHOUSE}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n"
    ));
    let root = tmp.path();
    model(
        root,
        "dim_customers",
        "SELECT 1 AS customer_id\n",
        "[strategy]\ntype = \"merge\"\nunique_key = [\"customer_id\"]\n",
    );
    let parsed = compile_json(root);
    assert!(
        codes_for(&parsed, "dim_customers")
            .iter()
            .any(|(c, _)| *c == "E053"),
        "{parsed}"
    );
}
