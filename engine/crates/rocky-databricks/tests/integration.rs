//! Integration tests against a real Databricks environment.
//!
//! These tests require:
//! - DATABRICKS_HOST
//! - DATABRICKS_HTTP_PATH
//! - DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
//!
//! Run with: cargo test -p rocky-databricks --test integration -- --ignored

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::unity_catalog_client::UnityCatalogClient;
use rocky_ir::TableRef;

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

/// Live-infrastructure test for `DatabricksConnector::describe_detail_stats`.
///
/// Uses the Delta table at `dev_hcv2_uniform.spike.uniform_t1` on the
/// sandbox workspace.  Reads connection params from environment variables
/// so workspace identifiers are never committed to the repository.
///
/// Required env vars:
///   DATABRICKS_HOST          — workspace hostname (no https://)
///   DATABRICKS_HTTP_PATH     — SQL warehouse HTTP path
///   DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
///
/// Run with:
///   cargo test -p rocky-databricks --test integration \
///     describe_detail_stats_returns_size_bytes -- --ignored
#[tokio::test]
#[ignore]
async fn describe_detail_stats_returns_size_bytes() {
    let connector = connector_from_env().expect("Databricks env vars not set");

    // The sandbox table used here is the UniForm test table from Exp-4.
    // Replace with any Delta table in your sandbox if this one is dropped.
    let catalog = "dev_hcv2_uniform";
    let schema = "spike";
    let table = "uniform_t1";

    let stats = connector
        .describe_detail_stats(catalog, schema, table)
        .await
        .expect("describe_detail_stats should not fail");

    let stats = stats.expect("expected Some(DescribeDetailStats), got None");

    // Delta's DESCRIBE DETAIL always populates sizeInBytes.
    assert!(
        stats.size_bytes.is_some(),
        "sizeInBytes should be present for a Delta table; got None",
    );
    let size = stats.size_bytes.unwrap();
    // Sanity: the spike table is small but non-zero.
    assert!(
        size > 0,
        "sizeInBytes should be > 0 for a non-empty table; got {size}",
    );
}

/// Verify that `describe_detail_stats` returns `Ok(None)` for a table that
/// does not exist, rather than propagating the error.
#[tokio::test]
#[ignore]
async fn describe_detail_stats_nonexistent_table_returns_none() {
    let connector = connector_from_env().expect("Databricks env vars not set");

    let result = connector
        .describe_detail_stats(
            "dev_hcv2_uniform",
            "spike",
            "definitely_does_not_exist_xxxxxxx",
        )
        .await
        .expect("should not error on missing table");

    assert!(
        result.is_none(),
        "missing table should return None, not an error",
    );
}

// ---------------------------------------------------------------------------
// Catalog-first delegation parity (live)
// ---------------------------------------------------------------------------
//
// These tests run two `DatabricksWarehouseAdapter` instances against the
// same sandbox — one with a `UnityCatalogClient` wired (REST path), one
// without (SQL path) — and assert the results are equal. They're the
// real source of truth for the "REST and SQL paths converge" claim;
// wiremock parity in `catalog_first_delegation.rs` only proves the
// projection shape.
//
// Required env vars (mirrors the existing live-test convention):
//   DATABRICKS_HOST
//   DATABRICKS_HTTP_PATH
//   DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)
//   DATABRICKS_TEST_CATALOG  — must already exist on the sandbox
//   DATABRICKS_TEST_SCHEMA   — optional; defaults to `default`

fn unity_client_from_env() -> Option<UnityCatalogClient> {
    let host = std::env::var("DATABRICKS_HOST").ok()?;
    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;
    Some(UnityCatalogClient::new(host, auth))
}

/// `describe_table` returns identical column-name + data-type sequences
/// regardless of whether the adapter routes through Unity REST or
/// `DESCRIBE TABLE` SQL. Creates an ephemeral `hc_phase1a_` schema
/// scoped to the test, populates a tiny table, then describes it via
/// both adapter instances and asserts parity. Cleans up unconditionally.
#[tokio::test]
#[ignore]
async fn live_describe_table_rest_and_sql_paths_agree() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let unity = unity_client_from_env().expect("Unity client env not set");

    let catalog = std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .expect("DATABRICKS_TEST_CATALOG or DATABRICKS_CATALOG_PREFIX must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let schema = format!("hc_phase1a_desc_{suffix}");
    let table = "hc_probe";

    // Use the SQL-only adapter for setup/teardown so we never depend on
    // REST writes (which we explicitly aren't wiring in this PR).
    let sql_adapter = DatabricksWarehouseAdapter::new(connector);

    sql_adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        .await
        .expect("create schema");
    sql_adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.{table} \
             (id BIGINT, amount DECIMAL(10,2), name STRING)"
        ))
        .await
        .expect("create probe table");

    let table_ref = TableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };

    // Run both paths against the same logical table, capturing each
    // result *before* any assertion so cleanup always runs even if one
    // side errors. Mirrors the pattern in `test_clone_table_for_branch_shallow_clone`.
    let sql_result = sql_adapter.describe_table(&table_ref).await;

    // Rebuild a parallel adapter that wraps the SAME warehouse but with
    // a `UnityCatalogClient` wired. Using a separate connector instance
    // keeps the two adapters independent at the SQL layer.
    let rest_connector = connector_from_env().expect("Databricks env vars not set");
    let rest_adapter = DatabricksWarehouseAdapter::new(rest_connector).with_catalog_client(unity);
    let rest_result = rest_adapter.describe_table(&table_ref).await;

    // Cleanup unconditionally before any assertion fires.
    let _ = sql_adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    let sql_cols = sql_result.expect("SQL describe_table");
    let rest_cols = rest_result.expect("REST describe_table");

    // Compare on (name, data_type). `DESCRIBE TABLE` doesn't reliably
    // surface nullability so the SQL path defaults to `true` — Unity
    // reports the declared nullability faithfully. We intentionally
    // don't assert on `nullable` because the two paths diverge by
    // design (and no caller of `describe_table` reads it for drift
    // detection on Databricks).
    let sql_names: Vec<(String, String)> = sql_cols
        .into_iter()
        .map(|c| (c.name.to_lowercase(), c.data_type.to_lowercase()))
        .collect();
    let rest_names: Vec<(String, String)> = rest_cols
        .into_iter()
        .map(|c| (c.name.to_lowercase(), c.data_type.to_lowercase()))
        .collect();

    assert_eq!(
        sql_names, rest_names,
        "REST and SQL describe_table paths must agree on (name, data_type)"
    );
    assert!(
        sql_names.iter().any(|(n, _)| n == "id" || n.contains("id")),
        "probe table should surface an `id` column; got {sql_names:?}"
    );
}

/// `list_tables` returns the same set (lowercased) regardless of path.
/// Creates an ephemeral schema with a known table set, lists via both
/// paths, asserts the sets are equal. Cleans up unconditionally.
#[tokio::test]
#[ignore]
async fn live_list_tables_rest_and_sql_paths_agree() {
    let connector = connector_from_env().expect("Databricks env vars not set");
    let unity = unity_client_from_env().expect("Unity client env not set");

    let catalog = std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .expect("DATABRICKS_TEST_CATALOG or DATABRICKS_CATALOG_PREFIX must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let schema = format!("hc_phase1a_list_{suffix}");

    let sql_adapter = DatabricksWarehouseAdapter::new(connector);
    sql_adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        .await
        .expect("create schema");
    for t in ["hc_alpha", "hc_beta", "hc_gamma"] {
        sql_adapter
            .execute_statement(&format!(
                "CREATE OR REPLACE TABLE {catalog}.{schema}.{t} (id BIGINT)"
            ))
            .await
            .expect("create probe table");
    }

    let sql_result = sql_adapter.list_tables(&catalog, &schema).await;

    let rest_connector = connector_from_env().expect("Databricks env vars not set");
    let rest_adapter = DatabricksWarehouseAdapter::new(rest_connector).with_catalog_client(unity);
    let rest_result = rest_adapter.list_tables(&catalog, &schema).await;

    // Cleanup unconditionally before any assertion.
    let _ = sql_adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    let mut sql_tables = sql_result.expect("SQL list_tables");
    let mut rest_tables = rest_result.expect("REST list_tables");
    sql_tables.sort();
    rest_tables.sort();

    assert_eq!(
        sql_tables, rest_tables,
        "REST and SQL list_tables must surface the same set"
    );
    // Every probe table we created should appear on both paths.
    for t in ["hc_alpha", "hc_beta", "hc_gamma"] {
        assert!(
            sql_tables.iter().any(|name| name == t),
            "expected {t} in listing; got {sql_tables:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Local-file staging via UC Volume (live merge gate)
// ---------------------------------------------------------------------------
//
// This is the live verification of the local-file → COPY INTO path. It reads
// ALL sandbox config from `ROCKY_TEST_*` env vars (per the scrubbing rule;
// note this diverges from the `DATABRICKS_*` convention the other live tests
// in this file use — the main session must set the `ROCKY_TEST_*` set below to
// run this gate). The catalog/schema MUST be one where the PAT/SP can
// `CREATE VOLUME IF NOT EXISTS`, `PUT` to the Files API, and `COPY INTO` — a
// read-only schema will 403 (a privilege issue, not a bug).
//
// Required env vars:
//   ROCKY_TEST_DATABRICKS_HOST          — workspace hostname (no https://)
//   ROCKY_TEST_DATABRICKS_HTTP_PATH     — SQL warehouse HTTP path
//   ROCKY_TEST_DATABRICKS_TOKEN         — PAT
//     (or ROCKY_TEST_DATABRICKS_CLIENT_ID + ROCKY_TEST_DATABRICKS_CLIENT_SECRET)
//   ROCKY_TEST_DATABRICKS_CATALOG       — catalog to stage + load into
//   ROCKY_TEST_DATABRICKS_SCHEMA        — schema to stage + load into
//   ROCKY_TEST_DATABRICKS_STAGING_VOLUME — optional; defaults to rocky_staging
//
// Run with:
//   cargo test -p rocky-databricks --test integration \
//     live_load_local_csv_via_volume -- --ignored

/// Build a connector from the `ROCKY_TEST_*` sandbox env vars. Returns `None`
/// (test skips via `.expect`) when the host/http-path aren't set.
fn rocky_test_connector_from_env() -> Option<DatabricksConnector> {
    let host = std::env::var("ROCKY_TEST_DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("ROCKY_TEST_DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;

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
        timeout: Duration::from_secs(120),
        retry: Default::default(),
    };
    Some(DatabricksConnector::new(config, auth))
}

#[tokio::test]
#[ignore]
async fn live_load_local_csv_via_volume() {
    use rocky_adapter_sdk::{LoadOptions, LoadSource, LoaderAdapter, TableRef as SdkTableRef};
    use rocky_databricks::loader::DatabricksLoaderAdapter;
    use std::io::Write;
    use std::sync::Arc;

    let connector = rocky_test_connector_from_env().expect("ROCKY_TEST_DATABRICKS_* not set");
    let catalog = std::env::var("ROCKY_TEST_DATABRICKS_CATALOG")
        .expect("ROCKY_TEST_DATABRICKS_CATALOG must be set");
    let schema = std::env::var("ROCKY_TEST_DATABRICKS_SCHEMA")
        .expect("ROCKY_TEST_DATABRICKS_SCHEMA must be set");
    let staging_volume = std::env::var("ROCKY_TEST_DATABRICKS_STAGING_VOLUME")
        .unwrap_or_else(|_| "rocky_staging".into());

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let table = format!("hc_load_local_{suffix}");

    // Write a small CSV to the OS temp dir.
    let csv_path = std::env::temp_dir().join(format!("rocky_live_load_{suffix}.csv"));
    {
        let mut f = std::fs::File::create(&csv_path).expect("create temp csv");
        f.write_all(b"id,name\n1,alice\n2,bob\n3,carol\n")
            .expect("write temp csv");
    }

    // Pre-create the (empty) target table so this gate exercises *staging*, not
    // COPY INTO's create-from-scratch semantics. Databricks COPY INTO loads
    // into an existing table (the canonical pattern is CREATE TABLE; then COPY
    // INTO ... mergeSchema='true'). A separate connector is used for setup /
    // verify / cleanup since the loader consumes its own Arc.
    let setup_connector = rocky_test_connector_from_env().expect("ROCKY_TEST_DATABRICKS_* not set");
    setup_connector
        .execute_statement(&format!(
            "CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (id BIGINT, name STRING)"
        ))
        .await
        .expect("create target table");

    let loader =
        DatabricksLoaderAdapter::new(Arc::new(connector)).with_staging_volume(&staging_volume);
    let target = SdkTableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: table.clone(),
    };

    // Run the load, capturing the result so cleanup always runs.
    let load_result = loader
        .load(
            &LoadSource::LocalFile(csv_path.clone()),
            &target,
            &LoadOptions::default(),
        )
        .await;

    // Verify row count via the setup connector (the loader consumed its Arc).
    let verify_connector = setup_connector;
    let count = if load_result.is_ok() {
        verify_connector
            .execute_sql(&format!(
                "SELECT COUNT(*) AS n FROM {catalog}.{schema}.{table}"
            ))
            .await
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
    } else {
        None
    };

    // Unconditional cleanup (best-effort): drop the loaded table. The staging
    // volume is intentionally left in place (reusable container).
    let _ = verify_connector
        .execute_statement(&format!("DROP TABLE IF EXISTS {catalog}.{schema}.{table}"))
        .await;
    let _ = std::fs::remove_file(&csv_path);

    let result = load_result.expect("local-file load via UC Volume should succeed");
    assert_eq!(result.rows_loaded, 3, "expected 3 rows loaded");
    assert!(result.bytes_read > 0, "should report local byte count");

    let n = count.expect("loaded table should be queryable");
    let n_num: i64 = n
        .as_i64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(-1);
    assert_eq!(n_num, 3, "table should contain 3 rows, got {n:?}");
}
