//! Live Databricks test for `WarehouseAdapter::source_change_marker` — the
//! `DESCRIBE DETAIL`-derived change-signal behind `prune_unchanged`
//! skip-unchanged replication pruning.
//!
//! Proves the marker advances on a source data write and stays stable when the
//! source is untouched, and exercises the runner's decision (recorded == live
//! ⇒ prune) against a real Delta table. `information_schema.tables.last_altered`
//! was rejected as the signal because it does not advance on data writes (only
//! DDL); `DESCRIBE DETAIL.lastModified` does — this test pins that.
//!
//! `#[ignore]`-gated; reads the same env vars as the other Databricks live
//! tests:
//!   DATABRICKS_HOST, DATABRICKS_HTTP_PATH,
//!   DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET),
//!   DATABRICKS_TEST_CATALOG (or DATABRICKS_CATALOG_PREFIX)
//! Every schema is `hc_`-prefixed and dropped CASCADE on exit.
//!
//! Run with:
//!   cargo test -p rocky-databricks --test source_marker_live -- --ignored --nocapture

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_ir::TableRef;

fn adapter_from_env() -> Option<DatabricksWarehouseAdapter> {
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
    Some(DatabricksWarehouseAdapter::new(DatabricksConnector::new(
        config, auth,
    )))
}

fn catalog_from_env() -> Option<String> {
    std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .ok()
}

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

#[tokio::test]
#[ignore]
async fn source_change_marker_advances_on_write_and_is_stable_when_untouched() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("SKIP: Databricks env not set");
        return;
    };
    let Some(catalog) = catalog_from_env() else {
        eprintln!("SKIP: DATABRICKS_TEST_CATALOG / DATABRICKS_CATALOG_PREFIX not set");
        return;
    };

    let schema = format!("hc_marker_{}", suffix());
    let table = "orders";
    let table_ref = TableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        .await
        .expect("create schema");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.{table} (id BIGINT, v BIGINT)"
        ))
        .await
        .expect("create table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO {catalog}.{schema}.{table} VALUES (1,10),(2,20)"
        ))
        .await
        .expect("seed");

    // Marker after seed.
    let m1 = adapter
        .source_change_marker(&table_ref)
        .await
        .expect("marker query")
        .expect("Delta table yields a marker");

    // Re-read without any write → stable (the prune-eligible case).
    let m1_again = adapter
        .source_change_marker(&table_ref)
        .await
        .expect("marker query")
        .expect("marker present");

    // A data write must advance the marker (the must-copy case).
    adapter
        .execute_statement(&format!(
            "MERGE INTO {catalog}.{schema}.{table} AS t USING (SELECT 1 AS id, 999 AS v) AS s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        ))
        .await
        .expect("data write");
    let m2 = adapter
        .source_change_marker(&table_ref)
        .await
        .expect("marker query")
        .expect("marker present");

    // Marker for a non-existent table is None (→ always copy, never prune).
    let missing = adapter
        .source_change_marker(&TableRef {
            catalog: catalog.clone(),
            schema: schema.clone(),
            table: "does_not_exist".to_string(),
        })
        .await
        .expect("marker query on missing table must not error");

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    println!("markers: seed={m1}  reread={m1_again}  after_write={m2}  missing={missing:?}");

    assert_eq!(
        m1, m1_again,
        "an untouched source must keep the same marker (this is what makes it prune-eligible)"
    );
    assert_ne!(
        m1, m2,
        "a source data write must advance the marker (so the runner re-copies, not prunes)"
    );
    assert!(
        missing.is_none(),
        "a missing table yields no marker so the runner copies rather than falsely pruning"
    );

    // The runner's decision, made concrete: recorded == live ⇒ prune; else copy.
    let would_prune = |recorded: &str, live: &str| recorded == live;
    assert!(would_prune(&m1, &m1_again), "unchanged ⇒ prune");
    assert!(!would_prune(&m1, &m2), "changed ⇒ copy");
}
