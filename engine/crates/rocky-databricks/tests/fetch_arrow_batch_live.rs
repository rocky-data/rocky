//! Live Databricks conformance test for [`WarehouseAdapter::fetch_arrow_batch`].
//!
//! `#[ignore]`-gated because it requires a real Databricks workspace.
//! Mirrors the duckdb shape at
//! `rocky-duckdb/src/adapter.rs::fetch_arrow_batch_returns_workspace_arrow_batch`
//! and the env-var contract used by `bisection_live.rs`.
//!
//! Run locally with:
//!
//! ```bash
//! DATABRICKS_HOST=<your-host> \
//! DATABRICKS_HTTP_PATH=<your-http-path> \
//! DATABRICKS_TOKEN=<your-token> \
//! cargo test -p rocky-databricks --test fetch_arrow_batch_live \
//!     -- --ignored --nocapture
//! ```
//!
//! OAuth M2M is supported transparently by [`Auth::from_config`] when
//! `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` are set instead
//! of `DATABRICKS_TOKEN`.

use std::time::Duration;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::DataType;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

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
    let connector = DatabricksConnector::new(config, auth);
    Some(DatabricksWarehouseAdapter::new(connector))
}

/// Asserts the same shape the duckdb #[ignore] test asserts: 2 fields
/// named `n`/`s` typed `Int32`/`Utf8`, 1 row with values `1` and
/// `"foo"`. The full path exercises EXTERNAL_LINKS + ARROW_STREAM
/// disposition, pre-signed cloud-storage GET, and `StreamReader`
/// decode end-to-end.
#[tokio::test]
#[ignore]
async fn fetch_arrow_batch_returns_workspace_arrow_batch() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("Databricks env vars not set; skipping");
        return;
    };

    let batch = adapter
        .fetch_arrow_batch("SELECT 1 AS n, 'foo' AS s")
        .await
        .expect("fetch_arrow_batch should succeed on rocky-databricks");

    // Schema — 2 columns, named + typed as expected.
    let schema = batch.schema();
    assert_eq!(schema.fields().len(), 2, "expected 2 columns");
    assert_eq!(schema.field(0).name(), "n");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(1).name(), "s");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    // Rows + values.
    assert_eq!(batch.num_rows(), 1);
    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column 0 must downcast to Int32Array");
    assert_eq!(n.value(0), 1);
    let s = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column 1 must downcast to StringArray");
    assert_eq!(s.value(0), "foo");
}
