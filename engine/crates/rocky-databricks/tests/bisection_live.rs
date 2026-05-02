//! Live Databricks conformance tests for the checksum-bisection diff.
//!
//! `#[ignore]`-gated because they require a real Databricks workspace.
//! Run locally with:
//!
//! ```bash
//! DATABRICKS_HOST=<your-host> \
//! DATABRICKS_HTTP_PATH=<your-http-path> \
//! DATABRICKS_TOKEN=<your-token> \
//! DATABRICKS_TEST_CATALOG=<your-catalog> \
//! cargo test -p rocky-databricks --test bisection_live -- --ignored --nocapture
//! ```
//!
//! Mirrors the DuckDB conformance shape at
//! `rocky-duckdb/tests/bisection_conformance.rs` and the BigQuery live
//! tests at `rocky-bigquery/tests/bisection_live.rs`. Schemas use the
//! `rocky_bisection_*` prefix per the workspace housekeeping convention.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::compare::bisection::{BisectionConfig, BisectionTarget, bisection_diff};
use rocky_core::ir::TableRef;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

fn adapter_from_env() -> Option<(DatabricksWarehouseAdapter, String)> {
    let host = std::env::var("DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
    let catalog = std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .ok()?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        host,
        warehouse_id,
        timeout: Duration::from_secs(180),
        retry: Default::default(),
    };
    let connector = DatabricksConnector::new(config, auth);
    Some((DatabricksWarehouseAdapter::new(connector), catalog))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn drop_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE"
        ))
        .await;
}

async fn create_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
        ))
        .await
        .expect("create schema");
}

async fn seed_table(
    adapter: &DatabricksWarehouseAdapter,
    catalog: &str,
    schema: &str,
    table: &str,
    n: u64,
) {
    // `range(0, n)` materialised via a CTAS — fast enough at the
    // 10k-row scale the bisection tests use.
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{table}` AS \
             SELECT id, CONCAT('row_', CAST(id AS STRING)) AS name, id * 7 AS value \
             FROM range(0, {n})"
        ))
        .await
        .expect("seed table");
}

async fn override_value(
    adapter: &DatabricksWarehouseAdapter,
    catalog: &str,
    schema: &str,
    table: &str,
    id: u64,
    new_value: i64,
) {
    adapter
        .execute_statement(&format!(
            "UPDATE `{catalog}`.`{schema}`.`{table}` SET value = {new_value} WHERE id = {id}"
        ))
        .await
        .expect("override value");
}

/// No-op diff bottoms out at the root level (K root chunks per side,
/// no recursion). Verification gate 3.
#[tokio::test]
#[ignore]
async fn live_noop_diff_examines_root_chunks_only() {
    let Some((adapter, catalog)) = adapter_from_env() else {
        eprintln!("Databricks env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let base_schema = format!("rocky_bisection_base_{suffix}");
    let branch_schema = format!("rocky_bisection_branch_{suffix}");

    create_schema(&adapter, &catalog, &base_schema).await;
    create_schema(&adapter, &catalog, &branch_schema).await;
    seed_table(&adapter, &catalog, &base_schema, "fact", 10_000).await;
    seed_table(&adapter, &catalog, &branch_schema, "fact", 10_000).await;

    let base = TableRef {
        catalog: catalog.clone(),
        schema: base_schema.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: catalog.clone(),
        schema: branch_schema.clone(),
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
        .expect("noop bisection on Databricks should succeed");

    drop_schema(&adapter, &catalog, &base_schema).await;
    drop_schema(&adapter, &catalog, &branch_schema).await;

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

/// Single planted change at id=4242 of a 10k-row table is found by
/// every run. Verification gate 2.
#[tokio::test]
#[ignore]
async fn live_finds_planted_change() {
    let Some((adapter, catalog)) = adapter_from_env() else {
        eprintln!("Databricks env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let base_schema = format!("rocky_bisection_base_{suffix}");
    let branch_schema = format!("rocky_bisection_branch_{suffix}");

    create_schema(&adapter, &catalog, &base_schema).await;
    create_schema(&adapter, &catalog, &branch_schema).await;
    seed_table(&adapter, &catalog, &base_schema, "fact", 10_000).await;
    seed_table(&adapter, &catalog, &branch_schema, "fact", 10_000).await;
    override_value(&adapter, &catalog, &branch_schema, "fact", 4_242, -1).await;

    let base = TableRef {
        catalog: catalog.clone(),
        schema: base_schema.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: catalog.clone(),
        schema: branch_schema.clone(),
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
        .expect("planted-change bisection on Databricks should succeed");

    drop_schema(&adapter, &catalog, &base_schema).await;
    drop_schema(&adapter, &catalog, &branch_schema).await;

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
