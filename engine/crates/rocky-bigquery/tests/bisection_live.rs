//! Live BigQuery conformance tests for the checksum-bisection diff.
//!
//! `#[ignore]`-gated because they require GCP credentials and a real
//! BigQuery project. Run locally with:
//!
//! ```bash
//! BIGQUERY_TEST_PROJECT=<your-project> \
//! GOOGLE_APPLICATION_CREDENTIALS=<path-to-sa.json> \
//! cargo test -p rocky-bigquery --test bisection_live -- --ignored --nocapture
//! ```
//!
//! Mirrors the DuckDB conformance shape at
//! `rocky-duckdb/tests/bisection_conformance.rs`: noop diff → root-only
//! K chunk checksums, planted change → exactly one row surfaced,
//! determinism → byte-identical `BisectionStats` across reruns.

use std::time::{SystemTime, UNIX_EPOCH};

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_core::compare::bisection::{BisectionConfig, BisectionTarget, bisection_diff};
use rocky_core::ir::TableRef;
use rocky_core::traits::WarehouseAdapter;

fn adapter_from_env() -> Option<(BigQueryAdapter, String)> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    let auth = BigQueryAuth::from_env().ok()?;
    Some((
        BigQueryAdapter::new(project.clone(), location, auth),
        project,
    ))
}

/// Per-test dataset suffix that's monotonic + collision-resistant
/// without needing a UUID dep.
fn dataset_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn drop_dataset(adapter: &BigQueryAdapter, project: &str, dataset: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{dataset}` CASCADE"
        ))
        .await;
}

async fn create_dataset(adapter: &BigQueryAdapter, project: &str, dataset: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{dataset}`"
        ))
        .await
        .expect("create dataset");
}

async fn seed_table(adapter: &BigQueryAdapter, project: &str, dataset: &str, table: &str, n: u64) {
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{project}`.`{dataset}`.`{table}` AS \
             SELECT i AS id, CONCAT('row_', CAST(i AS STRING)) AS name, i * 7 AS value \
             FROM UNNEST(GENERATE_ARRAY(0, {} - 1)) AS i",
            n,
        ))
        .await
        .expect("seed table");
}

async fn override_value(
    adapter: &BigQueryAdapter,
    project: &str,
    dataset: &str,
    table: &str,
    id: u64,
    new_value: i64,
) {
    adapter
        .execute_statement(&format!(
            "UPDATE `{project}`.`{dataset}`.`{table}` SET value = {new_value} WHERE id = {id}"
        ))
        .await
        .expect("override value");
}

/// Verification gate 3 — no-op diff bottoms out at the root level (K
/// root chunks per side, no recursion).
#[tokio::test]
#[ignore]
async fn live_noop_diff_examines_root_chunks_only() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("BIGQUERY_TEST_PROJECT / GOOGLE_APPLICATION_CREDENTIALS not set; skipping");
        return;
    };
    let suffix = dataset_suffix();
    let base_ds = format!("hc_bisection_base_{suffix}");
    let branch_ds = format!("hc_bisection_branch_{suffix}");

    create_dataset(&adapter, &project, &base_ds).await;
    create_dataset(&adapter, &project, &branch_ds).await;
    seed_table(&adapter, &project, &base_ds, "fact", 10_000).await;
    seed_table(&adapter, &project, &branch_ds, "fact", 10_000).await;

    let base = TableRef {
        catalog: project.clone(),
        schema: base_ds.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: project.clone(),
        schema: branch_ds.clone(),
        table: "fact".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(100),
        ..Default::default()
    };
    let target = BisectionTarget {
        base: &base,
        branch: &branch,
        pk_column: "id",
        value_columns: &value_columns,
        pk_lo: 0,
        pk_hi: 10_000,
    };

    let result = bisection_diff(&adapter, &adapter, &target, &config)
        .await
        .expect("noop bisection on BQ should succeed");

    drop_dataset(&adapter, &project, &base_ds).await;
    drop_dataset(&adapter, &project, &branch_ds).await;

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    assert!(result.samples.is_empty());
    assert_eq!(result.stats.depth_max, 0, "no recursion on no-op diff");
    assert_eq!(result.stats.leaves_materialized, 0);
    assert_eq!(
        result.stats.chunks_examined,
        u64::from(config.k) * 2,
        "no-op diff should only checksum K root chunks per side"
    );
}

/// Verification gate 2 — single planted change at id=4242 of a 10k-row
/// table is found by every run.
#[tokio::test]
#[ignore]
async fn live_finds_planted_change() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("BIGQUERY_TEST_PROJECT / GOOGLE_APPLICATION_CREDENTIALS not set; skipping");
        return;
    };
    let suffix = dataset_suffix();
    let base_ds = format!("hc_bisection_base_{suffix}");
    let branch_ds = format!("hc_bisection_branch_{suffix}");

    create_dataset(&adapter, &project, &base_ds).await;
    create_dataset(&adapter, &project, &branch_ds).await;
    seed_table(&adapter, &project, &base_ds, "fact", 10_000).await;
    seed_table(&adapter, &project, &branch_ds, "fact", 10_000).await;
    override_value(&adapter, &project, &branch_ds, "fact", 4_242, -1).await;

    let base = TableRef {
        catalog: project.clone(),
        schema: base_ds.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: project.clone(),
        schema: branch_ds.clone(),
        table: "fact".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(100),
        ..Default::default()
    };
    let target = BisectionTarget {
        base: &base,
        branch: &branch,
        pk_column: "id",
        value_columns: &value_columns,
        pk_lo: 0,
        pk_hi: 10_000,
    };

    let result = bisection_diff(&adapter, &adapter, &target, &config)
        .await
        .expect("planted-change bisection on BQ should succeed");

    drop_dataset(&adapter, &project, &base_ds).await;
    drop_dataset(&adapter, &project, &branch_ds).await;

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 1, "exactly one row should differ");
    assert_eq!(result.samples.len(), 1);
    assert_eq!(result.samples[0].pk, "4242");
    assert!(
        result.stats.depth_max >= 1,
        "single-row change requires recursion"
    );
    assert_eq!(result.stats.leaves_materialized, 1);
}
