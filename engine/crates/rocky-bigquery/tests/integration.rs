//! Integration tests against a real BigQuery environment.
//!
//! These tests require:
//! - `BIGQUERY_TEST_PROJECT` (the GCP project ID under which to create
//!   the ephemeral test datasets)
//! - one of:
//!   - `GOOGLE_APPLICATION_CREDENTIALS` (path to a service-account JSON
//!     file with `bigquery.dataEditor` + `bigquery.jobUser` on the
//!     project), or
//!   - `BIGQUERY_TOKEN` (a pre-exchanged OAuth bearer token; useful for
//!     `gcloud auth print-access-token` flows in CI).
//!
//! Optional:
//! - `BIGQUERY_TEST_LOCATION` — the dataset location (defaults to
//!   `"EU"` to match the typical free-trial quota).
//!
//! Run with: `cargo test -p rocky-bigquery --test integration -- --ignored`

use std::time::{SystemTime, UNIX_EPOCH};

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_core::ir::TableRef;
use rocky_core::traits::WarehouseAdapter;

/// Build an adapter from env vars. Returns `None` when the required
/// vars aren't present, so the `#[ignore]`d test silently skips on a
/// developer machine without GCP creds plumbed in.
fn adapter_from_env() -> Option<BigQueryAdapter> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    // `BigQueryAuth::from_env()` honours `BIGQUERY_TOKEN` first, then
    // falls back to `GOOGLE_APPLICATION_CREDENTIALS`.
    let auth = BigQueryAuth::from_env().ok()?;
    Some(BigQueryAdapter::new(project, location, auth))
}

/// Verifies the BigQuery `clone_table_for_branch` override emits a
/// working `CREATE OR REPLACE TABLE ... COPY` statement: source dataset
/// + table created, clone produced in a sibling dataset, row contents
/// match. Both datasets are dropped (CASCADE-equivalent
/// `DROP SCHEMA ... CASCADE`) at the end regardless of test outcome.
///
/// Project is read from `BIGQUERY_TEST_PROJECT`. Datasets use the
/// `hc_phase5_` prefix to match Hugo's Databricks-side convention so
/// sandbox housekeeping uses one regex across warehouses.
#[tokio::test]
#[ignore]
async fn test_clone_table_for_branch_copy() {
    let adapter = adapter_from_env().expect("BigQuery env vars not set");
    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let src_dataset = format!("hc_phase5_src_{suffix}");
    let brn_dataset = format!("hc_phase5_brn_{suffix}");
    let table = "test_table";

    // Setup: create the two datasets + a 2-row source table. BigQuery
    // calls them "datasets" rather than "schemas", but the
    // `CREATE SCHEMA` DDL works either way (it's an alias).
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{src_dataset}`"
        ))
        .await
        .expect("create source dataset");
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{brn_dataset}`"
        ))
        .await
        .expect("create branch dataset");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{project}`.`{src_dataset}`.`{table}` AS \
             SELECT * FROM UNNEST([STRUCT(1 AS id, 'a' AS name), STRUCT(2, 'b')])"
        ))
        .await
        .expect("create source table");

    // Run the unit under test, capture the result so cleanup runs first.
    let source = TableRef {
        catalog: project.clone(),
        schema: src_dataset.clone(),
        table: table.to_string(),
    };
    let clone_result = adapter.clone_table_for_branch(&source, &brn_dataset).await;

    let row_count = if clone_result.is_ok() {
        let q = adapter
            .execute_query(&format!(
                "SELECT COUNT(*) AS n FROM `{project}`.`{brn_dataset}`.`{table}`"
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
            "DROP SCHEMA IF EXISTS `{project}`.`{src_dataset}` CASCADE"
        ))
        .await;
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{brn_dataset}` CASCADE"
        ))
        .await;

    clone_result.expect("clone_table_for_branch should succeed");
    let n = row_count.expect("clone target should be queryable");
    // BigQuery returns COUNT(*) results as JSON strings (`{"v": "2"}`)
    // rather than typed numbers, so handle both shapes defensively.
    let n_num: i64 = n
        .as_i64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(-1);
    assert_eq!(n_num, 2, "cloned table should have 2 rows, got {n:?}");
}

/// Verifies `BigQueryDiscoveryAdapter::discover` lists matching
/// datasets + their tables against the live sandbox. Creates two
/// datasets with the `hc_phase14_disc_<ts>_` prefix, seeds one table
/// in each, runs discover, and asserts both datasets and their tables
/// are returned. Datasets are dropped on test exit (best-effort).
#[tokio::test]
#[ignore]
async fn test_discover_lists_datasets_and_tables() {
    use rocky_bigquery::BigQueryDiscoveryAdapter;
    use rocky_core::traits::DiscoveryAdapter as _;
    use std::sync::Arc;

    let warehouse = adapter_from_env().expect("BigQuery env vars not set");
    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let prefix = format!("hc_phase14_disc_{suffix}_");
    let alpha = format!("{prefix}alpha");
    let beta = format!("{prefix}beta");

    // Setup. Two datasets, one table each.
    warehouse
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{alpha}`"))
        .await
        .expect("create alpha");
    warehouse
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{beta}`"))
        .await
        .expect("create beta");
    warehouse
        .execute_statement(&format!(
            "CREATE TABLE `{project}`.`{alpha}`.`orders` AS SELECT 1 AS id"
        ))
        .await
        .expect("seed alpha.orders");
    warehouse
        .execute_statement(&format!(
            "CREATE TABLE `{project}`.`{beta}`.`customers` AS SELECT 1 AS id"
        ))
        .await
        .expect("seed beta.customers");

    let discovery = BigQueryDiscoveryAdapter::new(Arc::new(warehouse));
    let result = discovery.discover(&prefix).await;

    // Re-construct the warehouse adapter for cleanup; we moved the
    // original into the discovery adapter via Arc.
    let cleanup_adapter = adapter_from_env().expect("BigQuery env vars not set");
    let _ = cleanup_adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{alpha}` CASCADE"
        ))
        .await;
    let _ = cleanup_adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{beta}` CASCADE"
        ))
        .await;

    let result = result.expect("discover should succeed");
    assert_eq!(
        result.connectors.len(),
        2,
        "expected 2 datasets, got {:?}",
        result
            .connectors
            .iter()
            .map(|c| &c.schema)
            .collect::<Vec<_>>()
    );
    assert!(result.failed.is_empty());

    let alpha_conn = result
        .connectors
        .iter()
        .find(|c| c.schema == alpha)
        .expect("alpha dataset present");
    assert_eq!(alpha_conn.source_type, "bigquery");
    assert_eq!(alpha_conn.tables.len(), 1);
    assert_eq!(alpha_conn.tables[0].name, "orders");

    let beta_conn = result
        .connectors
        .iter()
        .find(|c| c.schema == beta)
        .expect("beta dataset present");
    assert_eq!(beta_conn.tables.len(), 1);
    assert_eq!(beta_conn.tables[0].name, "customers");
}
