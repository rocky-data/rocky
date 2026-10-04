//! A transformation `incremental` model loads only rows past its target's
//! watermark, end to end through the real binary against DuckDB.
//!
//! ```text
//!   run 1  target absent      @incremental_filter = TRUE        3 rows
//!   source: +2 newer rows, 1 row updated (newer updated_at)
//!   run 2  append             updated_at > MAX(target)          +3 rows
//!          merge (unique_key) same filter, MERGE on order_id    5 rows
//!   --full-refresh            TRUE, CREATE OR REPLACE           5 rows
//! ```
//!
//! The append case is the #1990 regression guard: before, `incremental`
//! re-inserted every row on every run, so the three unchanged rows would
//! have been duplicated.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"

[pipeline.transform.target.governance]
auto_create_schemas = true
"#;

const PLACEHOLDER_SQL: &str = "SELECT order_id, customer_id, amount, status, updated_at\n\
                               FROM raw.orders\n\
                               WHERE @incremental_filter\n";

fn project(strategy: &str, sql: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join("rocky.toml"), CONFIG).expect("write config");
    let models = dir.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    fs::write(models.join("fct_orders.sql"), sql).expect("write sql");
    fs::write(
        models.join("fct_orders.toml"),
        format!("[strategy]\n{strategy}\n\n[target]\ncatalog = \"fixture\"\nschema = \"marts\"\n"),
    )
    .expect("write sidecar");
    exec(
        dir.path(),
        "CREATE SCHEMA raw;
         CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE,
                                  status VARCHAR, updated_at TIMESTAMP);
         INSERT INTO raw.orders VALUES
           (1, 10, 5.0, 'new', TIMESTAMP '2026-01-01 00:00:00'),
           (2, 11, 6.0, 'new', TIMESTAMP '2026-01-02 00:00:00'),
           (3, 12, 7.0, 'new', TIMESTAMP '2026-01-03 00:00:00');",
    );
    dir
}

/// Two newer rows, and order 2 updated with a newer `updated_at`.
fn change_source(dir: &Path) {
    exec(
        dir,
        "INSERT INTO raw.orders VALUES
           (4, 13, 8.0, 'new', TIMESTAMP '2026-01-04 00:00:00'),
           (5, 14, 9.0, 'new', TIMESTAMP '2026-01-05 00:00:00');
         UPDATE raw.orders SET status = 'shipped', updated_at = TIMESTAMP '2026-01-06 00:00:00'
         WHERE order_id = 2;",
    );
}

fn exec(dir: &Path, sql: &str) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(sql).expect("execute fixture SQL");
}

/// `(order_id, status)` of every target row, sorted.
fn target_rows(dir: &Path) -> Vec<(i64, String)> {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    let mut stmt = conn
        .prepare("SELECT order_id, status FROM fixture.marts.fct_orders ORDER BY order_id, status")
        .expect("prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect()
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn run_ok(dir: &Path, extra: &[&str]) -> serde_json::Value {
    let mut args = vec!["run", "--pipeline", "transform"];
    args.extend_from_slice(extra);
    let out = rocky(dir, &args);
    assert!(
        out.status.success(),
        "rocky run failed: {}\nstdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stdout).expect("run JSON")
}

fn materialization(run: &serde_json::Value) -> &serde_json::Value {
    let mats = run["materializations"]
        .as_array()
        .expect("materializations");
    assert_eq!(mats.len(), 1, "{run}");
    &mats[0]
}

fn rows(pairs: &[(i64, &str)]) -> Vec<(i64, String)> {
    pairs
        .iter()
        .map(|(id, s)| (*id, (*s).to_string()))
        .collect()
}

#[test]
fn append_loads_only_rows_past_the_target_watermark() {
    let dir = project(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"",
        PLACEHOLDER_SQL,
    );
    let first = run_ok(dir.path(), &[]);
    assert_eq!(
        materialization(&first)["metadata"]["strategy"],
        "incremental"
    );
    assert_eq!(
        materialization(&first)["metadata"]["watermark"],
        "2026-01-03T00:00:00Z"
    );
    assert_eq!(
        target_rows(dir.path()),
        rows(&[(1, "new"), (2, "new"), (3, "new")])
    );

    change_source(dir.path());
    let second = run_ok(dir.path(), &[]);
    let m = materialization(&second);
    assert_eq!(m["metadata"]["watermark"], "2026-01-06T00:00:00Z");
    let notes = m["notes"].to_string();
    assert!(notes.contains("updated_at > 2026-01-03"), "{notes}");
    // Appended: the two new rows and the new version of order 2. Orders 1
    // and 3 are not appended again.
    assert_eq!(
        target_rows(dir.path()),
        rows(&[
            (1, "new"),
            (2, "new"),
            (2, "shipped"),
            (3, "new"),
            (4, "new"),
            (5, "new")
        ])
    );

    // A run with nothing new appends nothing.
    run_ok(dir.path(), &[]);
    assert_eq!(target_rows(dir.path()).len(), 6);

    // --full-refresh rebuilds from the whole source.
    let rebuilt = run_ok(dir.path(), &["--full-refresh"]);
    assert_eq!(
        materialization(&rebuilt)["metadata"]["strategy"],
        "full_refresh"
    );
    assert_eq!(
        target_rows(dir.path()),
        rows(&[
            (1, "new"),
            (2, "shipped"),
            (3, "new"),
            (4, "new"),
            (5, "new")
        ])
    );
}

#[test]
fn unique_key_merges_changed_rows() {
    let dir = project(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nunique_key = [\"order_id\"]",
        PLACEHOLDER_SQL,
    );
    run_ok(dir.path(), &[]);
    change_source(dir.path());
    run_ok(dir.path(), &[]);
    assert_eq!(
        target_rows(dir.path()),
        rows(&[
            (1, "new"),
            (2, "shipped"),
            (3, "new"),
            (4, "new"),
            (5, "new")
        ])
    );
}

/// No placeholder: the watermark is a direct passthrough, so Rocky filters
/// the model's output column instead.
#[test]
fn passthrough_watermark_without_placeholder_is_wrapped() {
    let dir = project(
        "type = \"incremental\"\nwatermark = \"updated_at\"",
        "SELECT order_id, customer_id, amount, status, updated_at FROM raw.orders\n",
    );
    run_ok(dir.path(), &[]);
    change_source(dir.path());
    run_ok(dir.path(), &[]);
    assert_eq!(target_rows(dir.path()).len(), 6);
}

/// A late row (older than the watermark) is picked up by `lookback`.
#[test]
fn lookback_with_unique_key_catches_late_rows() {
    let dir = project(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nunique_key = [\"order_id\"]\n\
         lookback = \"2 days\"",
        PLACEHOLDER_SQL,
    );
    run_ok(dir.path(), &[]);
    exec(
        dir.path(),
        "INSERT INTO raw.orders VALUES (9, 19, 1.0, 'late', TIMESTAMP '2026-01-02 12:00:00');",
    );
    run_ok(dir.path(), &[]);
    assert!(
        target_rows(dir.path()).contains(&(9, "late".to_string())),
        "{:?}",
        target_rows(dir.path())
    );
    assert_eq!(target_rows(dir.path()).len(), 4);
}

/// A new output column fails the run by default and leaves the target alone;
/// `append_new_columns` adds it.
#[test]
fn on_schema_change_fails_then_appends_new_columns() {
    let dir = project(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"",
        PLACEHOLDER_SQL,
    );
    run_ok(dir.path(), &[]);
    change_source(dir.path());
    let models = dir.path().join("models");
    fs::write(
        models.join("fct_orders.sql"),
        "SELECT order_id, amount * 2 AS amount_x2, customer_id, amount, status, updated_at\n\
         FROM raw.orders\nWHERE @incremental_filter\n",
    )
    .unwrap();
    let out = rocky(dir.path(), &["run", "--pipeline", "transform"]);
    assert!(!out.status.success(), "a new column must fail the run");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("amount_x2"), "{stdout}");
    assert_eq!(target_rows(dir.path()).len(), 3, "nothing was written");

    fs::write(
        models.join("fct_orders.toml"),
        "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"updated_at\"\n\
         on_schema_change = \"append_new_columns\"\n\n[target]\ncatalog = \"fixture\"\n\
         schema = \"marts\"\n",
    )
    .unwrap();
    run_ok(dir.path(), &[]);
    let conn = duckdb::Connection::open(dir.path().join("fixture.duckdb")).unwrap();
    // The new column sits in the middle of the model's output but at the end
    // of the target: the named INSERT keeps each value in its own column.
    let (ok_rows, null_rows): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*) FILTER (WHERE amount_x2 = amount * 2), \
                    COUNT(*) FILTER (WHERE amount_x2 IS NULL) \
             FROM fixture.marts.fct_orders",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((ok_rows, null_rows), (3, 3));
}

/// No watermark is still refused (E037), and a watermark Rocky cannot place
/// is refused (E046). Neither model runs.
#[test]
fn compile_refuses_incremental_without_a_safe_watermark() {
    for (strategy, sql, code) in [
        ("type = \"incremental\"", PLACEHOLDER_SQL, "E037"),
        (
            "type = \"incremental\"\ntimestamp_column = \"updated_at\"",
            "SELECT customer_id, MAX(updated_at) AS updated_at FROM raw.orders GROUP BY 1\n",
            "E046",
        ),
    ] {
        let dir = project(strategy, sql);
        let out = rocky(dir.path(), &["compile"]);
        assert!(!out.status.success(), "{code}: compile must fail");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains(code), "{code}: {stdout}");
        let out = rocky(dir.path(), &["run", "--pipeline", "transform"]);
        assert!(!out.status.success(), "{code}: run must fail");
        let conn = duckdb::Connection::open(dir.path().join("fixture.duckdb")).unwrap();
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fct_orders'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exists, 0, "{code}: the refused model must not be built");
    }
}
