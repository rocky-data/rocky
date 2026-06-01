//! Live Databricks dialect-sweep probes (audit verification, not a PR).
//!
//! Confirms / refutes a suspected SQL-dialect gap by executing the SQL Rocky
//! *actually emits* (via the real `DatabricksSqlDialect::insert_overwrite_partition`)
//! against a real Databricks workspace.
//!
//! `#[ignore]`-gated; reads from the SAME env vars as the other live tests:
//!   DATABRICKS_HOST, DATABRICKS_HTTP_PATH,
//!   DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET),
//!   DATABRICKS_TEST_CATALOG (or DATABRICKS_CATALOG_PREFIX)
//!
//! Every schema/table is `hc_`-prefixed and dropped CASCADE on exit.
//!
//! Run with:
//!   cargo test -p rocky-databricks --test dialect_sweep_live -- --ignored --nocapture

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::dialect::DatabricksSqlDialect;

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

fn as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// EXECUTE-GAP #6 — Databricks `insert_overwrite_partition` Delta shape
/// `INSERT INTO <t> REPLACE WHERE <filter>\n<select>`. Capability probe:
/// SUCCESS + only the targeted partition replaced (others byte-identical) =
/// REFUTED (works as claimed). SQL generated via the real dialect emitter.
#[tokio::test]
#[ignore]
async fn gap6_insert_overwrite_partition_replaces_only_target() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("SKIP gap6: Databricks env not set");
        return;
    };
    let Some(catalog) = catalog_from_env() else {
        eprintln!("SKIP gap6: DATABRICKS_TEST_CATALOG / DATABRICKS_CATALOG_PREFIX not set");
        return;
    };
    let schema = format!("hc_dialsweep_g6_{}", suffix());
    let table = "hc_part";

    // CONNECTIVITY PROOF: a benign SELECT 1 must succeed first.
    let probe = adapter.execute_query("SELECT 1 AS n").await;

    // Setup. Unity-managed CREATE TABLE defaults to Delta, so REPLACE WHERE
    // is satisfiable.
    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    let setup = async {
        adapter
            .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
            .await?;
        // Two partitions: part=1 (vals 10,11) and part=2 (vals 20,21).
        adapter
            .execute_statement(&format!(
                "CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS \
                 SELECT * FROM (VALUES (1, 10), (1, 11), (2, 20), (2, 21)) AS t(part, val)"
            ))
            .await?;
        Ok::<(), rocky_core::traits::AdapterError>(())
    }
    .await;

    let dialect = DatabricksSqlDialect;
    let target = dialect
        .format_table_ref(&catalog, &schema, table)
        .expect("format target");

    // Real emitter: overwrite partition part=1 with a single new row (99).
    let stmts = dialect
        .insert_overwrite_partition(&target, "part = 1", "SELECT 1 AS part, 99 AS val")
        .expect("insert_overwrite_partition");
    let script = stmts.join(";\n");

    let exec = if setup.is_ok() {
        Some(adapter.execute_statement(&script).await)
    } else {
        None
    };

    // Read back (capture before cleanup).
    let part1 = adapter
        .execute_query(&format!(
            "SELECT val FROM {catalog}.{schema}.{table} WHERE part = 1 ORDER BY val"
        ))
        .await
        .ok();
    let part2 = adapter
        .execute_query(&format!(
            "SELECT val FROM {catalog}.{schema}.{table} WHERE part = 2 ORDER BY val"
        ))
        .await
        .ok();

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    println!("\n=== EXECUTE-GAP #6 Databricks: insert_overwrite_partition ===");
    let p = probe.expect("CONNECTIVITY: SELECT 1 must succeed — proves warehouse reached");
    println!("CONNECTIVITY: SELECT 1 → {:?}", p.rows);
    setup.expect("setup (schema + 2-partition seed) must succeed");
    println!("SQL SENT (REPLACE WHERE shape):\n{script}");
    let exec = exec.expect("exec attempted");
    match &exec {
        Ok(()) => println!("RAW RESPONSE: REPLACE WHERE statement SUCCEEDED"),
        Err(e) => println!("RAW ERROR: {e}"),
    }
    exec.expect("partition-replace must execute — else CONFIRMED bug; see raw error");

    let p1 = part1.expect("part=1 queryable");
    let p2 = part2.expect("part=2 queryable");
    let p1_vals: Vec<i64> = p1.rows.iter().filter_map(|r| as_i64(&r[0])).collect();
    let p2_vals: Vec<i64> = p2.rows.iter().filter_map(|r| as_i64(&r[0])).collect();
    println!(
        "AFTER: part=1 vals={p1_vals:?} (expected [99]) | part=2 vals={p2_vals:?} (expected [20,21] untouched)"
    );

    assert_eq!(
        p1_vals,
        vec![99],
        "targeted partition part=1 should be fully replaced by [99]"
    );
    assert_eq!(
        p2_vals,
        vec![20, 21],
        "untargeted partition part=2 must be byte-identical (untouched)"
    );
    println!("VERDICT: REFUTED — partition-replace works; only part=1 replaced, part=2 intact");
}
