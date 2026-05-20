//! Live-execute coverage for the SCD2 NULL-safe change-detection fix.
//!
//! Pins the contract that the dialect's `null_safe_neq` actually
//! captures NULL ↔ value transitions on the watermark column — bare
//! SQL `!=` returns NULL on either side being NULL and silently drops
//! the row from the MERGE's `WHEN MATCHED` branch.
//!
//! Lesson recap: string-content dialect tests can hide warehouse-
//! rejected SQL; the dialect-level fix needs a live-execute test to
//! prove the predicate evaluates correctly, not just generates.

use rocky_core::snapshots::{SnapshotConfig, SnapshotStrategy, generate_snapshot_sql};
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_ir::{SourceRef, TargetRef};

/// Dialect contract: `null_safe_neq` must evaluate truthy for the three
/// NULL ↔ value cases that bare `!=` would silently drop, and falsy for
/// equal / both-null. Live-execute against DuckDB to ensure the emitted
/// predicate is a real boolean, not NULL (which would evaluate as false
/// in a WHEN MATCHED branch).
#[tokio::test]
async fn duckdb_null_safe_neq_captures_null_value_transitions() {
    let adapter = DuckDbWarehouseAdapter::in_memory().expect("in-memory DuckDB");
    let dialect = DuckDbSqlDialect;

    // Two-column probe table mirroring the SCD2 MERGE's
    // `source.updated_at != target.updated_at` shape.
    adapter
        .execute_statement("CREATE TABLE probe (src TIMESTAMP, tgt TIMESTAMP, label VARCHAR)")
        .await
        .expect("create probe table");

    // Five rows exercising every NULL/value combination.
    adapter
        .execute_statement(
            "INSERT INTO probe VALUES \
             (TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-01 00:00:00', 'both_v'), \
             (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00', 'equal'),  \
             (NULL,                            TIMESTAMP '2024-01-01 00:00:00', 'src_null'), \
             (TIMESTAMP '2024-01-01 00:00:00', NULL,                            'tgt_null'), \
             (NULL,                            NULL,                            'both_n')",
        )
        .await
        .expect("seed probe rows");

    let predicate = dialect.null_safe_neq("src", "tgt");
    let sql = format!("SELECT label FROM probe WHERE {predicate} ORDER BY label");
    let result = adapter
        .execute_query(&sql)
        .await
        .expect("execute null-safe-neq probe");

    let matched: Vec<String> = result
        .rows
        .iter()
        .map(|r| {
            r.first()
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_default()
        })
        .collect();

    assert_eq!(
        matched,
        vec![
            "both_v".to_string(),
            "src_null".to_string(),
            "tgt_null".to_string(),
        ],
        "null_safe_neq must capture both-value, src-null, and tgt-null \
         transitions; never equal or both-null. Got: {matched:?}"
    );
}

/// End-to-end contract: a row whose tracked column transitions from
/// NULL → value is captured by the generated SCD2 change predicate.
/// Drives `generate_snapshot_sql` to produce the predicate against the
/// DuckDB dialect, then exercises the predicate directly via a join +
/// UPDATE (DuckDB's MERGE doesn't accept `INSERT (*) VALUES (source.*,
/// ...)` — the SCD2 generator targets Databricks/Snowflake MERGE
/// surfaces, where this lives in production). The UPDATE shape is the
/// part this fix actually touched.
#[tokio::test]
async fn scd2_change_predicate_captures_null_to_value_on_duckdb() {
    let adapter = DuckDbWarehouseAdapter::in_memory().expect("in-memory DuckDB");
    let dialect = DuckDbSqlDialect;

    // Seed a synthetic source / target pair: target holds the
    // pre-transition NULL updated_at; source carries the new value.
    adapter
        .execute_statement(
            "CREATE TABLE source (id INTEGER, updated_at TIMESTAMP); \
             INSERT INTO source VALUES \
                (1, TIMESTAMP '2024-06-01 12:00:00'), \
                (2, NULL),                            \
                (3, TIMESTAMP '2024-06-01 12:00:00')",
        )
        .await
        .expect("create source");
    adapter
        .execute_statement(
            "CREATE TABLE target (id INTEGER, updated_at TIMESTAMP, is_current BOOLEAN); \
             INSERT INTO target VALUES \
                (1, NULL,                            TRUE), \
                (2, TIMESTAMP '2024-05-01 00:00:00', TRUE), \
                (3, TIMESTAMP '2024-06-01 12:00:00', TRUE)",
        )
        .await
        .expect("create target");

    // Build the same change-detection predicate the SCD2 generator
    // would emit on the Timestamp strategy.
    let config = SnapshotConfig {
        source: SourceRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "source".into(),
        },
        target: TargetRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "target".into(),
        },
        unique_key: vec!["id".into()],
        strategy: SnapshotStrategy::Timestamp {
            updated_at: "updated_at".into(),
        },
        invalidate_hard_deletes: false,
    };
    let stmts = generate_snapshot_sql(&config, &dialect).expect("snapshot SQL");
    let merge_sql = &stmts[0];
    assert!(
        merge_sql.contains("source.updated_at IS DISTINCT FROM target.updated_at"),
        "generated MERGE must use IS DISTINCT FROM, got: {merge_sql}"
    );

    // Exercise the predicate against the seeded data: rows 1 and 2 have
    // NULL ↔ value transitions, row 3 is equal — only rows 1 and 2
    // should match. Bare SQL `!=` would only report row 3 (which
    // happens to be equal — so it would report zero rows changed,
    // dropping rows 1 and 2 silently).
    let predicate = dialect.null_safe_neq("source.updated_at", "target.updated_at");
    let result = adapter
        .execute_query(&format!(
            "SELECT target.id FROM source \
             INNER JOIN target ON target.id = source.id \
             WHERE {predicate} ORDER BY target.id"
        ))
        .await
        .expect("scan predicate");
    let matched: Vec<i64> = result
        .rows
        .iter()
        .map(|r| {
            r[0].as_i64()
                .or_else(|| r[0].as_str().and_then(|s| s.parse().ok()))
                .expect("id cell")
        })
        .collect();
    assert_eq!(
        matched,
        vec![1, 2],
        "null-safe change predicate must catch NULL→value (id=1) and \
         value→NULL (id=2); never equal (id=3). bare SQL `!=` would \
         drop both NULL transitions and report empty. Got: {matched:?}"
    );
}
