//! Live Snowflake conformance test for the batched schema-describe path.
//!
//! `#[ignore]`-gated because it requires a real Snowflake account.
//! Run locally with:
//!
//! ```bash
//! SNOWFLAKE_ACCOUNT=<your-account> \
//! SNOWFLAKE_WAREHOUSE=<your-warehouse> \
//! SNOWFLAKE_TEST_DATABASE=<your-database> \
//! # one of the supported auth modes (PAT shown here):
//! SNOWFLAKE_PAT=<your-pat> \
//! cargo test -p rocky-snowflake --test batch_describe_live -- --ignored --nocapture
//! ```
//!
//! What this test pins (closes the `feedback_sql_dialect_live_execute_tests`
//! gap for the `INFORMATION_SCHEMA.COLUMNS` batch query):
//!
//! 1. `SnowflakeBatchCheckAdapter::batch_describe_schema` returns one entry
//!    per table in the schema in a single round trip, with every column
//!    keyed by its lowercase table name.
//! 2. The batched column set matches the per-table `DESCRIBE TABLE` path
//!    (`WarehouseAdapter::describe_table`) for the same table — same column
//!    names (lowercased), same nullability. Snowflake's `INFORMATION_SCHEMA`
//!    reports `data_type` families (`NUMBER`, `TEXT`) where `DESCRIBE` reports
//!    precise types (`NUMBER(38,0)`, `VARCHAR(...)`), so the type *string* is
//!    deliberately not asserted equal — only that both paths see the same
//!    columns.
//!
//! Without this test, the inline SQL-shape unit tests in `batch.rs` would
//! pass while a casing or result-shape regression against the real
//! warehouse shipped silently — exactly the class of bug stub tests have
//! repeatedly hidden on the Snowflake adapter.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::traits::{BatchCheckAdapter, WarehouseAdapter};
use rocky_ir::TableRef;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::batch::SnowflakeBatchCheckAdapter;
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};

/// Build a shared connector from env, returning it alongside the resolved
/// database name. Returns `None` when the env isn't plumbed in so the
/// `#[ignore]`d test skips silently rather than panicking on a dev box.
fn connector_from_env() -> Option<(std::sync::Arc<SnowflakeConnector>, String)> {
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
        warehouse,
        database: Some(database.clone()),
        schema: None,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        timeout: Duration::from_secs(180),
        retry: RetryConfig::default(),
    };
    Some((
        std::sync::Arc::new(SnowflakeConnector::new(config, auth)),
        database,
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

#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_batch_describe_matches_per_table_describe() {
    let Some((connector, database)) = connector_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };
    let adapter = SnowflakeWarehouseAdapter::new((*connector).clone());

    let schema = format!("rocky_batch_describe_{}", schema_suffix());

    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;

    // Two tables with mixed types + nullability so the batched query has to
    // group multiple tables and parse `is_nullable` (YES/NO) correctly.
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"orders\" (\
                \"id\" NUMBER(38,0) NOT NULL, \
                \"customer\" VARCHAR(255), \
                \"amount\" FLOAT\
            )"
        ),
    )
    .await;
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"events\" (\
                \"event_id\" NUMBER(38,0), \
                \"payload\" VARIANT, \
                \"created_at\" TIMESTAMP_NTZ NOT NULL\
            )"
        ),
    )
    .await;

    // Run the unit under test, then compare against per-table DESCRIBE.
    let batch = SnowflakeBatchCheckAdapter::new(connector.clone());
    let result = batch.batch_describe_schema(&database, &schema).await;

    // Always drop the schema before asserting so a failure can't leak it.
    let cleanup = format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\"");

    let batched = match result {
        Ok(map) => map,
        Err(e) => {
            exec(&adapter, &cleanup).await;
            panic!("batch_describe_schema failed: {e:?}");
        }
    };

    // The batched map must key both tables by their lowercase name and carry
    // every column. Compare names + nullability against per-table DESCRIBE.
    let mut mismatch: Option<String> = None;
    for table in ["orders", "events"] {
        let per_table = adapter
            .describe_table(&TableRef {
                catalog: database.clone(),
                schema: schema.clone(),
                table: table.to_string(),
            })
            .await;

        let per_table = match per_table {
            Ok(c) => c,
            Err(e) => {
                mismatch = Some(format!("per-table describe of {table} failed: {e:?}"));
                break;
            }
        };

        let Some(batch_cols) = batched.get(table) else {
            mismatch = Some(format!(
                "batched result missing table {table:?}; keys present: {:?}",
                batched.keys().collect::<Vec<_>>()
            ));
            break;
        };

        let batch_names: BTreeSet<String> = batch_cols.iter().map(|c| c.name.clone()).collect();
        let per_table_names: BTreeSet<String> = per_table.iter().map(|c| c.name.clone()).collect();
        if batch_names != per_table_names {
            mismatch = Some(format!(
                "{table}: column-name set mismatch\n  batch     = {batch_names:?}\n  per-table = {per_table_names:?}"
            ));
            break;
        }

        // Nullability must agree column-for-column.
        for col in &per_table {
            let b = batch_cols
                .iter()
                .find(|c| c.name == col.name)
                .expect("name sets already proven equal");
            if b.nullable != col.nullable {
                mismatch = Some(format!(
                    "{table}.{}: nullability mismatch — batch={} per-table={}",
                    col.name, b.nullable, col.nullable
                ));
                break;
            }
        }
        if mismatch.is_some() {
            break;
        }
        eprintln!(
            "OK: {table} — {} columns match per-table DESCRIBE (names + nullability)",
            batch_cols.len()
        );
    }

    exec(&adapter, &cleanup).await;

    if let Some(msg) = mismatch {
        panic!("{msg}");
    }
}
