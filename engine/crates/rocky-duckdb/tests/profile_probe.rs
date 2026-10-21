//! Go/no-go probe for the upstream-read metering: what does DuckDB's JSON
//! profile expose for a `TABLE_SCAN` node?
//!
//! Finding (duckdb crate 1.10506, DuckDB 1.5.x): every `TABLE_SCAN` node has
//! `operator_type = "TABLE_SCAN"`, `operator_rows_scanned` (a number) and
//! `extra_info.Table` as `catalog.schema.table`. `extra_info.Projections` is
//! the projected column list, in three shapes: a JSON array for two or more
//! columns, a bare string for one column, and `""` for none (`count(*)`). A
//! Parquet scan or a table function (`range`) has no `Table` key.

use duckdb::Connection;
use serde_json::Value;

fn scans(v: &Value, out: &mut Vec<Value>) {
    if v.get("operator_type").and_then(Value::as_str) == Some("TABLE_SCAN") {
        out.push(v.clone());
    }
    if let Some(children) = v.get("children").and_then(Value::as_array) {
        for child in children {
            scans(child, out);
        }
    }
}

fn profile(conn: &Connection, sql: &str, file: &std::path::Path) -> Vec<Value> {
    conn.execute_batch(&format!(
        "PRAGMA enable_profiling = 'json'; PRAGMA profiling_output = '{}';",
        file.display()
    ))
    .unwrap();
    conn.execute_batch(sql).unwrap();
    conn.execute_batch("PRAGMA disable_profiling;").unwrap();
    let root: Value = serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
    let mut out = Vec::new();
    scans(&root, &mut out);
    out
}

#[test]
fn table_scan_profile_names_table_rows_and_projections() {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    let columns: Vec<String> = (0..10).map(|i| format!("c{i} BIGINT")).collect();
    conn.execute_batch(&format!("CREATE TABLE wide ({});", columns.join(", ")))
        .unwrap();
    let values: Vec<String> = (0..10).map(|i| format!("i + {i}")).collect();
    conn.execute_batch(&format!(
        "INSERT INTO wide SELECT {} FROM range(10000) t(i);",
        values.join(", ")
    ))
    .unwrap();
    let file = dir.path().join("profile.json");

    let two = profile(&conn, "CREATE TABLE two AS SELECT c1, c3 FROM wide", &file);
    assert_eq!(two.len(), 1);
    assert_eq!(two[0]["extra_info"]["Table"], "memory.main.wide");
    assert_eq!(two[0]["operator_rows_scanned"], 10_000);
    assert_eq!(
        two[0]["extra_info"]["Projections"],
        serde_json::json!(["c1", "c3"])
    );

    let all = profile(&conn, "CREATE TABLE every AS SELECT * FROM wide", &file);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["extra_info"]["Table"], "memory.main.wide");
    assert_eq!(all[0]["operator_rows_scanned"], 10_000);
    let projected = all[0]["extra_info"]["Projections"].as_array().unwrap();
    assert_eq!(projected.len(), 10);

    // One column is a bare string, not a one-element array.
    let one = profile(&conn, "CREATE TABLE one AS SELECT c5 FROM wide", &file);
    assert_eq!(one[0]["extra_info"]["Projections"], "c5");

    // A table function has no Table key.
    let range = profile(&conn, "CREATE TABLE r AS SELECT * FROM range(10)", &file);
    assert_eq!(range.len(), 1);
    assert!(range[0]["extra_info"].get("Table").is_none());
}
