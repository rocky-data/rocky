//! The DuckDB adapter records what a statement read from each table.

use rocky_core::state::UpstreamReads;
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

async fn adapter() -> DuckDbWarehouseAdapter {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    // Two fixed-width columns, one text column, more rows than one vector.
    adapter
        .execute_statement(
            "CREATE TABLE src AS SELECT i AS id, i * 2 AS amount, i::VARCHAR AS note \
             FROM range(5000) t(i)",
        )
        .await
        .unwrap();
    adapter
}

async fn reads(adapter: &DuckDbWarehouseAdapter, sql: &str) -> UpstreamReads {
    adapter
        .execute_statement_with_upstream_reads(sql)
        .await
        .unwrap()
        .1
        .expect("reads measured")
}

#[tokio::test]
async fn two_consumers_of_one_table_differ_in_columns_and_estimate() {
    let adapter = adapter().await;
    let narrow = reads(
        &adapter,
        "CREATE TABLE narrow AS SELECT id, amount FROM src",
    )
    .await;
    let wide = reads(&adapter, "CREATE TABLE wide AS SELECT * FROM src").await;

    assert_eq!(narrow.reads.len(), 1);
    assert_eq!(narrow.reads[0].table, "memory.main.src");
    assert_eq!(narrow.reads[0].rows_scanned, 5000);
    assert_eq!(narrow.reads[0].projected_columns, ["id", "amount"]);
    assert_eq!(narrow.reads[0].est_bytes, Some(5000 * 16));

    assert_eq!(wide.reads[0].rows_scanned, 5000);
    assert_eq!(wide.reads[0].projected_columns.len(), 3);
    // The text column makes the width unknown, so no estimate.
    assert_eq!(wide.reads[0].est_bytes, None);
}

#[tokio::test]
async fn a_self_join_sums_its_scans() {
    let adapter = adapter().await;
    let joined = reads(
        &adapter,
        "CREATE TABLE j AS SELECT a.id FROM src a JOIN src b ON a.id = b.id",
    )
    .await;
    assert_eq!(joined.reads.len(), 1);
    assert_eq!(joined.reads[0].table, "memory.main.src");
    assert_eq!(joined.reads[0].rows_scanned, 10_000);
    assert_eq!(joined.reads[0].projected_columns, ["id"]);
    assert_eq!(joined.reads[0].est_bytes, Some(10_000 * 8));
}

#[tokio::test]
async fn a_parquet_scan_is_unattributed() {
    let adapter = adapter().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.parquet");
    adapter
        .execute_statement(&format!(
            "COPY src TO '{}' (FORMAT PARQUET)",
            path.display()
        ))
        .await
        .unwrap();
    let got = reads(
        &adapter,
        &format!(
            "CREATE TABLE p AS SELECT s.id FROM src s JOIN read_parquet('{}') f ON s.id = f.id",
            path.display()
        ),
    )
    .await;
    assert_eq!(got.unattributed_scans, 1);
    assert_eq!(got.reads.len(), 1);
    assert_eq!(got.reads[0].table, "memory.main.src");
}

#[tokio::test]
async fn a_count_star_has_no_projection_and_no_estimate() {
    let adapter = adapter().await;
    let got = reads(&adapter, "CREATE TABLE c AS SELECT count(*) AS n FROM src").await;
    assert_eq!(got.reads[0].rows_scanned, 5000);
    assert!(got.reads[0].projected_columns.is_empty());
    assert_eq!(got.reads[0].est_bytes, None);
}

#[tokio::test]
async fn a_failed_statement_is_an_error_and_profiling_is_turned_off() {
    let adapter = adapter().await;
    adapter
        .execute_statement_with_upstream_reads("CREATE TABLE bad AS SELECT nope FROM src")
        .await
        .unwrap_err();
    // The next statement still works and is measured on its own.
    let got = reads(&adapter, "CREATE TABLE ok AS SELECT id FROM src").await;
    assert_eq!(got.reads[0].rows_scanned, 5000);
}

#[tokio::test]
async fn a_batch_is_run_but_not_measured() {
    let adapter = adapter().await;
    let (_, got) = adapter
        .execute_statement_with_upstream_reads(
            "CREATE TABLE b1 AS SELECT id FROM src; CREATE TABLE b2 AS SELECT id FROM src",
        )
        .await
        .unwrap();
    assert_eq!(got, None);
    let rows = adapter
        .execute_query("SELECT count(*) FROM b2")
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
}

/// A pushed-down filter reads columns that `Projections` does not list, so
/// the scan has no byte estimate.
#[tokio::test]
async fn a_filtered_scan_has_no_byte_estimate() {
    let adapter = adapter().await;
    let filtered = reads(
        &adapter,
        "CREATE TABLE f AS SELECT amount FROM src WHERE id % 7 = 0 AND note <> 'x'",
    )
    .await;
    assert_eq!(filtered.reads.len(), 1);
    assert_eq!(filtered.reads[0].projected_columns, ["amount"]);
    assert_eq!(filtered.reads[0].est_bytes, None);
    let plain = reads(&adapter, "CREATE TABLE p AS SELECT amount FROM src").await;
    assert_eq!(plain.reads[0].est_bytes, Some(5000 * 8));
}
