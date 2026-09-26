//! `rocky snapshot` executes end to end on DuckDB (#2012).
//!
//! Before the fix, three things stopped it:
//!
//! ```text
//!   initial_load  target schema missing      -> "Schema ... does not exist"
//!   merge_1       INSERT (*) VALUES (source.*, ...)  -> DuckDB parser error
//!   merge_3       UPDATE names an undeclared `target` alias -> binder error
//! ```
//!
//! This test runs the real binary three times against a file-backed DuckDB
//! and reads the SCD2 history back.
//!
//! ```text
//!   run 1   customers 1, 2, 3          -> 3 current rows
//!   change  1 renamed, 3 deleted
//!   run 2                              -> 1 closed + new version, 3 closed
//!   run 3   no change                  -> nothing new
//! ```

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.customers_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.customers_history.source]
adapter = "local"
catalog = "warehouse"
schema = "raw"
table = "customers"

[pipeline.customers_history.target]
adapter = "local"
catalog = "warehouse"
schema = "history"
table = "customers_history"

[pipeline.customers_history.target.governance]
auto_create_schemas = true
"#;

fn db(dir: &Path) -> duckdb::Connection {
    duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb")
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn snapshot(dir: &Path) -> serde_json::Value {
    let out = rocky(dir, &["snapshot"]);
    assert!(
        out.status.success(),
        "rocky snapshot failed; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("snapshot JSON")
}

/// `(customer_id, name, is_current, valid_to IS NULL)` for every history row.
fn history(dir: &Path) -> Vec<(i32, String, bool, bool)> {
    let conn = db(dir);
    let mut stmt = conn
        .prepare(
            "SELECT customer_id, name, is_current, valid_to IS NULL \
             FROM history.customers_history \
             ORDER BY customer_id, valid_from, is_current",
        )
        .expect("prepare history");
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .expect("query history")
        .map(|r| r.expect("row"))
        .collect()
}

#[test]
fn snapshot_records_scd2_history_on_duckdb() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), CONFIG).expect("write config");
    db(dir)
        .execute_batch(
            "CREATE SCHEMA raw;
             CREATE TABLE raw.customers AS SELECT * FROM (VALUES
                 (1, 'Ada',   TIMESTAMP '2026-01-01'),
                 (2, 'Grace', TIMESTAMP '2026-01-01'),
                 (3, 'Linus', TIMESTAMP '2026-01-01')
             ) AS t(customer_id, name, updated_at);",
        )
        .expect("seed source");

    let first = snapshot(dir);
    let steps: Vec<&str> = first["steps"]
        .as_array()
        .expect("steps")
        .iter()
        .map(|s| s["step"].as_str().expect("step name"))
        .collect();
    assert_eq!(
        steps,
        [
            "create_schema",
            "initial_load",
            "merge_1",
            "merge_2",
            "merge_3"
        ]
    );
    assert_eq!(first["steps_ok"], 5, "{first}");
    assert_eq!(
        history(dir),
        [
            (1, "Ada".to_string(), true, true),
            (2, "Grace".to_string(), true, true),
            (3, "Linus".to_string(), true, true),
        ]
    );

    db(dir)
        .execute_batch(
            "UPDATE raw.customers SET name = 'Ada L.', updated_at = TIMESTAMP '2026-02-01'
                 WHERE customer_id = 1;
             DELETE FROM raw.customers WHERE customer_id = 3;",
        )
        .expect("change source");

    snapshot(dir);
    let after_change = vec![
        (1, "Ada".to_string(), false, false),
        (1, "Ada L.".to_string(), true, true),
        (2, "Grace".to_string(), true, true),
        (3, "Linus".to_string(), false, false),
    ];
    assert_eq!(history(dir), after_change);

    // A run with no source change writes nothing.
    snapshot(dir);
    assert_eq!(history(dir), after_change);
}
