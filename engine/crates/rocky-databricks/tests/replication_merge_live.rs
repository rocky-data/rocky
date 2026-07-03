//! Live Databricks replication-`merge` verification (audit, not a PR by default).
//!
//! Closes the "does `strategy = \"merge\"` actually emit + execute a `MERGE`
//! on Databricks (not just DuckDB)?" gap. It reconstructs the exact dialect
//! call-chain `rocky_core::sql_gen::generate_merge_sql` runs — `format_table_ref`
//! ×2 → `select_clause` → `merge_into` — which is the same SQL
//! `commands/run.rs` executes at materialization (run.rs: `Merge =>
//! generate_merge_sql`). Then it runs that SQL against a real Databricks
//! workspace and asserts upsert semantics + merge-key uniqueness.
//!
//! `#[ignore]`-gated; reads the same env vars as the other Databricks live
//! tests:
//!   DATABRICKS_HOST, DATABRICKS_HTTP_PATH,
//!   DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET),
//!   DATABRICKS_TEST_CATALOG (or DATABRICKS_CATALOG_PREFIX)
//! Every schema is `hc_`-prefixed and dropped CASCADE on exit.
//!
//! Run with:
//!   cargo test -p rocky-databricks --test replication_merge_live -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_ir::ColumnSelection;

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

async fn scalar_i64(adapter: &DatabricksWarehouseAdapter, sql: &str) -> i64 {
    let r = adapter.execute_query(sql).await.expect("scalar query");
    as_i64(&r.rows[0][0]).expect("scalar i64")
}

/// GREEN end-state: the `MERGE INTO … WHEN MATCHED THEN UPDATE SET * WHEN NOT
/// MATCHED THEN INSERT *` that the replication runner emits for `strategy =
/// "merge"` executes on Databricks, upserts correctly (matched rows updated,
/// unmatched rows inserted), keeps the merge key unique (no row
/// multiplication — the failure mode of a NULL/dup key), and is idempotent on
/// re-run against an unchanged source.
#[tokio::test]
#[ignore]
async fn live_databricks_replication_merge_upserts_and_is_idempotent() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("SKIP: Databricks env not set");
        return;
    };
    let Some(catalog) = catalog_from_env() else {
        eprintln!("SKIP: DATABRICKS_TEST_CATALOG / DATABRICKS_CATALOG_PREFIX not set");
        return;
    };

    let s = suffix();
    let src_schema = format!("hc_rmt_src_{s}");
    let tgt_schema = format!("hc_rmt_tgt_{s}");
    let table = "orders";

    // Defensive: clear any prior leak from an aborted run.
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{src_schema} CASCADE"
        ))
        .await;
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{tgt_schema} CASCADE"
        ))
        .await;

    // --- setup: source (3 rows) + empty target, identical schema ---
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS {catalog}.{src_schema}"
        ))
        .await
        .expect("create src schema");
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS {catalog}.{tgt_schema}"
        ))
        .await
        .expect("create tgt schema");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{src_schema}.{table} \
             (order_id BIGINT, customer_id BIGINT, amount DECIMAL(10,2), status STRING)"
        ))
        .await
        .expect("create source table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO {catalog}.{src_schema}.{table} VALUES \
             (1,10,100.00,'completed'),(2,20,200.00,'completed'),(3,30,300.00,'pending')"
        ))
        .await
        .expect("seed source");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{tgt_schema}.{table} \
             (order_id BIGINT, customer_id BIGINT, amount DECIMAL(10,2), status STRING)"
        ))
        .await
        .expect("create empty target");

    // --- build the MERGE via the REAL dialect (mirrors generate_merge_sql) ---
    let dialect = DatabricksSqlDialect;
    let target_ref = dialect
        .format_table_ref(&catalog, &tgt_schema, table)
        .expect("target ref");
    let source_ref = dialect
        .format_table_ref(&catalog, &src_schema, table)
        .expect("source ref");
    let select_clause = dialect
        .select_clause(&ColumnSelection::All, &[])
        .expect("select clause");
    let select = format!("{select_clause}\nFROM {source_ref}");
    let keys = [Arc::<str>::from("order_id")];
    let merge_sql = dialect
        .merge_into(&target_ref, &select, &keys, &ColumnSelection::All)
        .expect("merge sql");
    println!(
        "--- Rocky-emitted Databricks MERGE ---\n{merge_sql}\n--------------------------------------"
    );

    // MERGE #1: empty target -> all 3 source rows inserted (NOT MATCHED).
    adapter
        .execute_statement(&merge_sql)
        .await
        .expect("merge #1");
    let count_1 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM {catalog}.{tgt_schema}.{table}"),
    )
    .await;

    // Mutate source: change order 1 (customer_id 10 -> 111), add order 4.
    adapter
        .execute_statement(&format!(
            "UPDATE {catalog}.{src_schema}.{table} SET customer_id = 111 WHERE order_id = 1"
        ))
        .await
        .expect("update source row");
    adapter
        .execute_statement(&format!(
            "INSERT INTO {catalog}.{src_schema}.{table} VALUES (4,40,400.00,'completed')"
        ))
        .await
        .expect("insert source row");

    // MERGE #2: order 1 UPDATED (MATCHED), order 4 INSERTED (NOT MATCHED).
    adapter
        .execute_statement(&merge_sql)
        .await
        .expect("merge #2");
    let count_2 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM {catalog}.{tgt_schema}.{table}"),
    )
    .await;
    let distinct_2 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(DISTINCT order_id) FROM {catalog}.{tgt_schema}.{table}"),
    )
    .await;
    let cust_1 = scalar_i64(
        &adapter,
        &format!("SELECT customer_id FROM {catalog}.{tgt_schema}.{table} WHERE order_id = 1"),
    )
    .await;
    let has_4 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM {catalog}.{tgt_schema}.{table} WHERE order_id = 4"),
    )
    .await;

    // MERGE #3: idempotent re-run against the unchanged source.
    adapter
        .execute_statement(&merge_sql)
        .await
        .expect("merge #3");
    let count_3 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM {catalog}.{tgt_schema}.{table}"),
    )
    .await;
    let distinct_3 = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(DISTINCT order_id) FROM {catalog}.{tgt_schema}.{table}"),
    )
    .await;

    // --- teardown BEFORE asserts, so a failed assert still cleans up ---
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{src_schema} CASCADE"
        ))
        .await;
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {catalog}.{tgt_schema} CASCADE"
        ))
        .await;

    println!(
        "counts: run1={count_1} run2={count_2}(distinct={distinct_2}) run3={count_3}(distinct={distinct_3}); order1.customer_id={cust_1} has_order4={has_4}"
    );

    assert_eq!(
        count_1, 3,
        "MERGE #1 into empty target inserts all 3 source rows"
    );
    assert_eq!(
        count_2, 4,
        "MERGE #2 inserts order 4 + updates order 1 (no duplication)"
    );
    assert_eq!(distinct_2, 4, "merge key stays unique after upsert");
    assert_eq!(
        count_2, distinct_2,
        "COUNT(*) must equal COUNT(DISTINCT order_id) — no row multiplication"
    );
    assert_eq!(
        cust_1, 111,
        "order 1 reflects the MATCHED UPDATE (10 -> 111)"
    );
    assert_eq!(has_4, 1, "order 4 present via the NOT MATCHED INSERT");
    assert_eq!(
        count_3, 4,
        "MERGE #3 against an unchanged source is idempotent"
    );
    assert_eq!(distinct_3, 4, "idempotent re-run keeps keys unique");
}
