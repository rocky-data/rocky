//! Live Databricks confirmation that managed Iceberg rejects the
//! `format_options` combos Rocky now blocks at compile time (FR-044).
//!
//! Rocky validates managed-Iceberg `format_options` so a bad combo surfaces as
//! a clear `rocky compile` diagnostic (E035) — and as a `LakehouseError` in the
//! DDL generator — *before* any warehouse call. This live test pins the
//! ground truth that motivates that validation: the raw `USING ICEBERG` DDL
//! Rocky would otherwise have emitted is, in fact, rejected by the warehouse:
//!
//! 1. `PARTITIONED BY (...) CLUSTER BY (...)` together →
//!    `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`.
//! 2. `TBLPROPERTIES ('write.format.default' = ...)` →
//!    `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`.
//!
//! `#[ignore]`-gated because it requires a real Databricks workspace. Reads the
//! sandbox config from `ROCKY_TEST_DATABRICKS_*` env vars (mirrors
//! `lakehouse_initial_ddl_live.rs`); no workspace identifiers are hardcoded.
//! Any schema it creates uses the `hcv2_` prefix and is dropped on completion.
//!
//! ```bash
//! ROCKY_TEST_DATABRICKS_HOST=<your-host> \
//! ROCKY_TEST_DATABRICKS_HTTP_PATH=<your-http-path> \
//! ROCKY_TEST_DATABRICKS_TOKEN=<your-token> \
//! ROCKY_TEST_DATABRICKS_CATALOG=hcv2_<your-catalog> \
//! cargo test -p rocky-databricks --test managed_iceberg_format_validation_live -- --ignored --nocapture
//! ```

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

/// Build an adapter + target catalog from the `ROCKY_TEST_DATABRICKS_*` sandbox
/// env vars. Returns `None` (test skips) when the host/http-path aren't set.
fn adapter_from_env() -> Option<(DatabricksWarehouseAdapter, String)> {
    let host = std::env::var("ROCKY_TEST_DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("ROCKY_TEST_DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
    let catalog = std::env::var("ROCKY_TEST_DATABRICKS_CATALOG").ok()?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("ROCKY_TEST_DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_SECRET").ok(),
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

async fn create_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
        ))
        .await
        .expect("create schema");
}

async fn drop_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE"
        ))
        .await;
}

/// The warehouse must reject managed-Iceberg DDL with partition+cluster
/// together and with an engine-managed `write.format.default` property — the
/// exact two combos Rocky's E035 / `ManagedIcebergUnsupported` guard blocks at
/// compile/plan time.
#[tokio::test]
#[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
async fn live_managed_iceberg_rejects_blocked_format_options() {
    let Some((adapter, catalog)) = adapter_from_env() else {
        eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("hcv2_mi_fmt_{suffix}");
    create_schema(&adapter, &catalog, &schema).await;

    // (1) PARTITIONED BY + CLUSTER BY together on managed Iceberg.
    let partition_plus_cluster = format!(
        "CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`bad_part_cluster`\n\
         USING ICEBERG\n\
         PARTITIONED BY (region)\n\
         CLUSTER BY (id)\n\
         AS\nSELECT CAST(id % 3 AS STRING) AS region, id FROM range(0, 10)"
    );
    let err1 = adapter
        .execute_statement(&partition_plus_cluster)
        .await
        .expect_err("warehouse must reject partition + cluster on Iceberg");
    let msg1 = err1.to_string();
    assert!(
        msg1.contains("SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED")
            || msg1.to_lowercase().contains("cluster")
            || msg1.to_lowercase().contains("partition"),
        "expected a partition/cluster mutual-exclusion rejection, got: {msg1}"
    );

    // (2) Engine-managed write.format.default property on managed Iceberg.
    let write_format_property = format!(
        "CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`bad_write_format`\n\
         USING ICEBERG\n\
         PARTITIONED BY (region)\n\
         TBLPROPERTIES ('write.format.default' = 'parquet')\n\
         AS\nSELECT CAST(id % 3 AS STRING) AS region, id FROM range(0, 10)"
    );
    let err2 = adapter
        .execute_statement(&write_format_property)
        .await
        .expect_err("warehouse must reject engine-managed write.format.default on Iceberg");
    let msg2 = err2.to_string();
    assert!(
        msg2.contains("MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED")
            || msg2.to_lowercase().contains("not supported")
            || msg2.to_lowercase().contains("write.format"),
        "expected a managed-Iceberg property rejection, got: {msg2}"
    );

    drop_schema(&adapter, &catalog, &schema).await;
}
