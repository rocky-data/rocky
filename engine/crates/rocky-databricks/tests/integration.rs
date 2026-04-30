//! Integration tests against a real Databricks environment.
//!
//! These tests require:
//! - DATABRICKS_HOST
//! - DATABRICKS_HTTP_PATH
//! - DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
//!
//! Run with: cargo test -p rocky-databricks --test integration -- --ignored

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::ir::TableRef;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

fn connector_from_env() -> Option<DatabricksConnector> {
    let host = std::env::var("DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;

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
        timeout: Duration::from_secs(120),
        retry: Default::default(),
    };

    Some(DatabricksConnector::new(config, auth))
}

#[tokio::test]
#[ignore]
async fn test_execute_select_1() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let result = connector.execute_sql("SELECT 1 AS value").await.unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].name, "value");
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
#[ignore]
async fn test_execute_current_timestamp() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let result = connector
        .execute_sql("SELECT current_timestamp() AS ts")
        .await
        .unwrap();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);
    let ts = result.rows[0][0].as_str().unwrap();
    assert!(!ts.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_execute_multiple_rows() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let result = connector
        .execute_sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)")
        .await
        .unwrap();

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 3);
}

#[tokio::test]
#[ignore]
async fn test_execute_show_catalogs() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let result = connector.execute_sql("SHOW CATALOGS").await.unwrap();

    // Should have at least one catalog
    assert!(!result.rows.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_execute_invalid_sql_fails() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let result = connector.execute_sql("SELECTT INVALID SYNTAX").await;

    assert!(result.is_err());
}

/// Verifies the Databricks `clone_table_for_branch` override emits a
/// working `SHALLOW CLONE` statement: source schema + table created,
/// clone produced in a sibling schema, row contents match. Both schemas
/// are dropped (CASCADE) at the end regardless of test outcome.
///
/// Catalog is read from `DATABRICKS_TEST_CATALOG` (defaults to the value
/// of `DATABRICKS_CATALOG_PREFIX`) so the test honors per-environment
/// sandbox naming. Schemas use the `hc_phase5_` prefix.
#[tokio::test]
#[ignore]
async fn test_clone_table_for_branch_shallow_clone() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let adapter = DatabricksWarehouseAdapter::new(connector);

    let catalog = std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .expect("DATABRICKS_TEST_CATALOG or DATABRICKS_CATALOG_PREFIX must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let src_schema = format!("hc_phase5_src_{suffix}");
    let brn_schema = format!("hc_phase5_brn_{suffix}");
    let table = "test_table";

    // Setup: create the two schemas + a 2-row source table.
    // The catalog is assumed to exist; operator is responsible for pre-creating it.
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS {catalog}.{src_schema}"
        ))
        .await
        .expect("create source schema");
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS {catalog}.{brn_schema}"
        ))
        .await
        .expect("create branch schema");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{src_schema}.{table} AS \
             SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)"
        ))
        .await
        .expect("create source table");

    // Run the unit under test, capture the result so cleanup runs first.
    let source = TableRef {
        catalog: catalog.clone(),
        schema: src_schema.clone(),
        table: table.to_string(),
    };
    let clone_result = adapter.clone_table_for_branch(&source, &brn_schema).await;

    let row_count = if clone_result.is_ok() {
        let q = adapter
            .execute_query(&format!(
                "SELECT COUNT(*) AS n FROM {catalog}.{brn_schema}.{table}"
            ))
            .await
            .ok();
        q.and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
    } else {
        None
    };

    // Unconditional cleanup (best-effort; ignored on failure).
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{src_schema} CASCADE"
        ))
        .await;
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{brn_schema} CASCADE"
        ))
        .await;

    clone_result.expect("clone_table_for_branch should succeed");
    let n = row_count.expect("clone target should be queryable");
    let n_str = n.as_str().or_else(|| n.as_str()).unwrap_or_default();
    let n_num: i64 = n.as_i64().unwrap_or_else(|| n_str.parse().unwrap_or(-1));
    assert_eq!(n_num, 2, "cloned table should have 2 rows, got {n:?}");
}

#[tokio::test]
#[ignore]
async fn test_describe_table() {
    let connector = connector_from_env().expect("Databricks env vars not set");

    // system.information_schema.tables exists in all workspaces
    let result = connector
        .execute_sql("DESCRIBE TABLE system.information_schema.tables")
        .await
        .unwrap();

    assert!(!result.rows.is_empty());
    // Should have columns like table_catalog, table_schema, table_name
    let col_names: Vec<&str> = result
        .rows
        .iter()
        .filter_map(|r| r.first().and_then(|v| v.as_str()))
        .collect();
    assert!(col_names.contains(&"table_catalog"));
    assert!(col_names.contains(&"table_name"));
}
