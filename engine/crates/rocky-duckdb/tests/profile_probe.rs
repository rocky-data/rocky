//! Probe: what does DuckDB's JSON profile expose for TABLE_SCAN nodes?
use duckdb::Connection;

fn scans(v: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
    if v.get("operator_type").and_then(|t| t.as_str()) == Some("TABLE_SCAN") {
        out.push(v.clone());
    }
    if let Some(c) = v.get("children").and_then(|c| c.as_array()) {
        for x in c {
            scans(x, out);
        }
    }
}

fn profile(conn: &Connection, sql: &str, file: &std::path::Path) -> Vec<serde_json::Value> {
    conn.execute_batch(&format!(
        "PRAGMA enable_profiling = 'json'; PRAGMA profiling_output = '{}';",
        file.display()
    ))
    .unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    let _ = stmt.query_map([], |_| Ok(())).unwrap().count();
    conn.execute_batch("PRAGMA disable_profiling;").unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
    let mut out = vec![];
    scans(&v, &mut out);
    out
}

#[test]
fn probe_table_scan_profile() {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    let cols: Vec<String> = (0..10).map(|i| format!("c{i} BIGINT")).collect();
    conn.execute_batch(&format!("CREATE TABLE wide ({});", cols.join(", ")))
        .unwrap();
    let sel: Vec<String> = (0..10).map(|i| format!("i + {i}")).collect();
    conn.execute_batch(&format!(
        "INSERT INTO wide SELECT {} FROM range(10000) t(i);",
        sel.join(", ")
    ))
    .unwrap();
    let f = dir.path().join("p.json");
    let two = profile(&conn, "SELECT c1, c3 FROM wide", &f);
    let all = profile(&conn, "SELECT * FROM wide", &f);
    eprintln!("TWO: {}", serde_json::to_string_pretty(&two).unwrap());
    eprintln!("ALL: {}", serde_json::to_string_pretty(&all).unwrap());
    assert_eq!(two.len(), 1);
    assert_eq!(all.len(), 1);
}
