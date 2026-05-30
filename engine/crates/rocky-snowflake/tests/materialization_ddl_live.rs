//! Live Snowflake conformance tests for the proprietary materialization DDL:
//! `DYNAMIC TABLE` and `MATERIALIZED VIEW`.
//!
//! `#[ignore]`-gated; needs a real Snowflake account (same `SNOWFLAKE_*` env
//! vars as the other live tests). These DDL forms are Snowflake-specific —
//! `CREATE DYNAMIC TABLE ... TARGET_LAG = ... WAREHOUSE = ...` and
//! `CREATE MATERIALIZED VIEW` — with syntax and feature constraints a wiremock
//! test can't validate. This pins that the DDL `SnowflakeSqlDialect` emits is
//! actually accepted by Snowflake and produces a queryable object.
//!
//! `MATERIALIZED VIEW` requires Snowflake Enterprise Edition; on a Standard
//! account the CREATE fails with an "unsupported feature" error. That sub-test
//! treats the edition error as a skip (the DDL syntax can't be exercised on
//! that edition) rather than a failure. Dynamic tables are GA on Standard.
//!
//! Schemas use the `hc_mat_*` prefix per Hugo's Snowflake convention; the
//! per-run microsecond suffix isolates parallel runs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};
use rocky_snowflake::dialect::SnowflakeSqlDialect;

/// Returns `(adapter, database, warehouse)` — the warehouse is needed for the
/// dynamic-table DDL.
fn adapter_from_env() -> Option<(SnowflakeWarehouseAdapter, String, String)> {
    let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok()?;
    let database = std::env::var("SNOWFLAKE_TEST_DATABASE")
        .or_else(|_| std::env::var("SNOWFLAKE_DATABASE"))
        .ok()?;

    let auth = Auth::from_config(AuthConfig {
        account: account.clone(),
        username: std::env::var("SNOWFLAKE_USERNAME").ok(),
        password: std::env::var("SNOWFLAKE_PASSWORD").ok(),
        oauth_token: std::env::var("SNOWFLAKE_OAUTH_TOKEN").ok(),
        private_key_path: std::env::var("SNOWFLAKE_PRIVATE_KEY_PATH").ok(),
        pat: std::env::var("SNOWFLAKE_PAT").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        account,
        warehouse: warehouse.clone(),
        database: Some(database.clone()),
        schema: None,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        timeout: Duration::from_secs(180),
        retry: RetryConfig::default(),
    };
    let connector = SnowflakeConnector::new(config, auth);
    Some((
        SnowflakeWarehouseAdapter::new(connector),
        database,
        warehouse,
    ))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn exec(adapter: &SnowflakeWarehouseAdapter, sql: &str) {
    adapter
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {sql}\nerror: {e:?}"));
}

/// Seeds a 3-row source table and returns its fully-qualified quoted ref.
async fn seed_source(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) -> String {
    let source = format!("\"{database}\".\"{schema}\".\"src\"");
    exec(
        adapter,
        &format!("CREATE OR REPLACE TABLE {source} (\"id\" NUMBER(10,0), \"val\" VARCHAR(50))"),
    )
    .await;
    exec(
        adapter,
        &format!("INSERT INTO {source} (\"id\", \"val\") VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
    )
    .await;
    source
}

async fn count_rows(adapter: &SnowflakeWarehouseAdapter, table_ref: &str) -> Option<i64> {
    let result = adapter
        .execute_query(&format!("SELECT COUNT(*) AS \"n\" FROM {table_ref}"))
        .await
        .ok()?;
    let cell = result.rows.first()?.first()?.clone();
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
}

/// `CREATE DYNAMIC TABLE ... TARGET_LAG ... WAREHOUSE ... AS <select>` is
/// accepted by Snowflake and the table refreshes to the source's rows.
#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_dynamic_table_ddl_creates_and_refreshes() {
    let Some((adapter, database, warehouse)) = adapter_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };
    let schema = format!("hc_mat_dt_{}", schema_suffix());
    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;
    let source = seed_source(&adapter, &database, &schema).await;

    let dialect = SnowflakeSqlDialect;
    let dt_ref = format!("\"{database}\".\"{schema}\".\"dt_orders\"");
    let select_sql = format!("SELECT \"id\", \"val\" FROM {source}");
    let ddl = dialect
        .dynamic_table_ddl(&dt_ref, &select_sql, "1 minute", &warehouse)
        .expect("dynamic_table_ddl");
    println!("emitting: {ddl}");

    let create_result = adapter.execute_statement(&ddl).await;

    // A freshly created dynamic table refreshes asynchronously; poll briefly
    // for the initial refresh to reflect the 3 source rows.
    let mut observed = None;
    if create_result.is_ok() {
        for _ in 0..15 {
            if let Some(n) = count_rows(&adapter, &dt_ref).await {
                observed = Some(n);
                if n == 3 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    create_result.expect("CREATE DYNAMIC TABLE should be accepted by Snowflake");
    assert_eq!(
        observed,
        Some(3),
        "dynamic table should refresh to the 3 source rows; observed {observed:?}"
    );
}

/// `CREATE MATERIALIZED VIEW ... AS <select>` is accepted by Snowflake (when
/// the account edition supports MVs) and serves the source's rows.
#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_materialized_view_ddl_creates_and_serves() {
    let Some((adapter, database, _warehouse)) = adapter_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };
    let schema = format!("hc_mat_mv_{}", schema_suffix());
    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;
    let source = seed_source(&adapter, &database, &schema).await;

    let dialect = SnowflakeSqlDialect;
    let mv_ref = format!("\"{database}\".\"{schema}\".\"mv_orders\"");
    // Single-table projection with no joins/aggregates — within Snowflake's MV
    // restrictions.
    let select_sql = format!("SELECT \"id\", \"val\" FROM {source}");
    let ddl = dialect
        .materialized_view_ddl(&mv_ref, &select_sql)
        .expect("materialized_view_ddl");
    println!("emitting: {ddl}");

    let create_result = adapter.execute_statement(&ddl).await;

    // MVs are an Enterprise-edition feature; on Standard the CREATE is rejected
    // with an unsupported-feature error. Treat that as a skip, not a failure.
    if let Err(e) = &create_result {
        let msg = format!("{e:?}");
        if msg.contains("Unsupported feature")
            || msg.to_lowercase().contains("materialized view")
            || msg.contains("Enterprise")
        {
            eprintln!("MATERIALIZED VIEW unsupported on this account edition; skipping: {msg}");
            let _ = adapter
                .execute_statement(&format!(
                    "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"
                ))
                .await;
            return;
        }
    }

    let served = if create_result.is_ok() {
        count_rows(&adapter, &mv_ref).await
    } else {
        None
    };

    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    create_result.expect("CREATE MATERIALIZED VIEW should be accepted (non-edition errors)");
    assert_eq!(
        served,
        Some(3),
        "materialized view should serve the 3 source rows; got {served:?}"
    );
}
