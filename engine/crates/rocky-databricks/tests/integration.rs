//! Integration tests against a real Databricks environment.
//!
//! These tests require:
//! - DATABRICKS_HOST
//! - DATABRICKS_HTTP_PATH
//! - DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
//!
//! Run with: cargo test -p rocky-databricks --test integration -- --ignored

use std::time::Duration;

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
