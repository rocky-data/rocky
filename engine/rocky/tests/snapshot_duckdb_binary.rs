//! Exercise the public binary against persistent DuckDB across two runs.
#![cfg(feature = "duckdb")]

use std::process::Command;

use duckdb::Connection;

#[test]
fn snapshot_binary_closes_changed_row_and_opens_new_version() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("warehouse.duckdb");
    std::fs::write(
        dir.path().join("rocky.toml"),
        r#"[adapter.local]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.history.source]
adapter = "local"
catalog = "warehouse"
schema = "raw"
table = "customers"

[pipeline.history.target]
adapter = "local"
catalog = "warehouse"
schema = "history"
table = "customers_history"

[pipeline.history.target.governance]
auto_create_schemas = true

[state]
backend = "local"
"#,
    )
    .unwrap();

    let conn = Connection::open(&db).unwrap();
    conn.execute_batch("CREATE SCHEMA raw; CREATE TABLE raw.customers AS SELECT 1 AS customer_id, 'Alice' AS name, TIMESTAMP '2026-01-01' AS updated_at").unwrap();
    drop(conn);

    let run = || {
        let result = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .arg("snapshot")
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    };
    run();

    let conn = Connection::open(&db).unwrap();
    let rows: (String, bool) = conn
        .query_row(
            "SELECT name, is_current FROM history.customers_history",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(rows, ("Alice".into(), true));
    conn.execute_batch("UPDATE raw.customers SET name = 'Alicia', updated_at = TIMESTAMP '2026-02-01' WHERE customer_id = 1").unwrap();
    drop(conn);

    run();
    let conn = Connection::open(&db).unwrap();
    let mut stmt = conn.prepare("SELECT name, is_current, valid_to IS NULL FROM history.customers_history ORDER BY updated_at").unwrap();
    let rows: Vec<(String, bool, bool)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        rows,
        vec![
            ("Alice".into(), false, false),
            ("Alicia".into(), true, true)
        ]
    );
}
