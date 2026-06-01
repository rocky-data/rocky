//! Standalone cleanup verification: confirm no `hc_dialsweep`-prefixed
//! schemas survived the Databricks dialect-sweep probes. Throwaway audit
//! helper; not a regression test.

use std::time::Duration;

use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

#[tokio::test]
#[ignore]
async fn cleanup_check_no_leftover_schemas() {
    let host = std::env::var("DATABRICKS_HOST").expect("DATABRICKS_HOST");
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").expect("DATABRICKS_HTTP_PATH");
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path).unwrap();
    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
    })
    .unwrap();
    let adapter = DatabricksWarehouseAdapter::new(DatabricksConnector::new(
        ConnectorConfig {
            host,
            warehouse_id,
            timeout: Duration::from_secs(120),
            retry: Default::default(),
        },
        auth,
    ));
    let catalog = std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .unwrap();

    let q = adapter
        .execute_query(&format!("SHOW SCHEMAS IN {catalog} LIKE 'hc_dialsweep*'"))
        .await
        .expect("show schemas");
    println!("Databricks leftover hc_dialsweep schemas: {:?}", q.rows);
    assert!(
        q.rows.is_empty(),
        "leftover Databricks schemas not cleaned up: {:?}",
        q.rows
    );
    println!("Databricks: CLEAN — no hc_dialsweep schemas remain in {catalog}");
}
