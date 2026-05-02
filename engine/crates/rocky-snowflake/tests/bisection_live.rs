//! Live Snowflake conformance tests for the checksum-bisection diff.
//!
//! `#[ignore]`-gated because they require a real Snowflake account.
//! Run locally with:
//!
//! ```bash
//! SNOWFLAKE_ACCOUNT=<your-account> \
//! SNOWFLAKE_WAREHOUSE=<your-warehouse> \
//! SNOWFLAKE_TEST_DATABASE=<your-database> \
//! # one of:
//! SNOWFLAKE_OAUTH_TOKEN=<your-oauth-token> \
//! # or:
//! SNOWFLAKE_USERNAME=<user> SNOWFLAKE_PASSWORD=<pwd> \
//! # or:
//! SNOWFLAKE_USERNAME=<user> SNOWFLAKE_PRIVATE_KEY_PATH=<path-to-pem> \
//! cargo test -p rocky-snowflake --test bisection_live -- --ignored --nocapture
//! ```
//!
//! Mirrors the BigQuery (`rocky-bigquery/tests/bisection_live.rs`) and
//! Databricks (`rocky-databricks/tests/bisection_live.rs`) live-test
//! shapes: noop diff bottoms out at the root level (K root chunks per
//! side, no recursion); a single planted change at row 4,242 is found
//! by every run.
//!
//! **Ships UNTESTED by design.** No Snowflake sandbox in the team's
//! account at the time of writing — verification gates merge. See the
//! PR body's "NEEDS SANDBOX" callout.
//!
//! **Snowflake-specific test details (load-bearing).**
//! - Column names in seeded tables are wrapped in **double quotes**
//!   (`"id"`, `"name"`, `"value"`) so they survive Snowflake's
//!   case-fold-on-unquoted-identifiers rule. The bisection kernel
//!   builds its `WHERE "id" >= ...` predicates with double quotes via
//!   `SqlDialect::quote_identifier`; if the seed used unquoted
//!   `id` (which Snowflake folds to `ID`), the kernel's lowercase
//!   reference would resolve to a non-existent column.
//! - Schemas use the `hc_bisection_*` prefix per the standard
//!   per-test schema-isolation convention.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::compare::bisection::{BisectionConfig, BisectionTarget, bisection_diff};
use rocky_core::config::RetryConfig;
use rocky_core::ir::TableRef;
use rocky_core::traits::WarehouseAdapter;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};

fn adapter_from_env() -> Option<(SnowflakeWarehouseAdapter, String)> {
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
    let connector = SnowflakeConnector::new(config, auth);
    Some((SnowflakeWarehouseAdapter::new(connector), database))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn drop_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"
        ))
        .await;
}

async fn create_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""
        ))
        .await
        .expect("create schema");
}

async fn seed_table(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
    n: u64,
) {
    // Snowflake `GENERATOR` materialises N rows; `ROW_NUMBER` over an
    // empty `ORDER BY` provides the 0..N-1 sequence. Column names are
    // double-quoted so they survive Snowflake's case-fold-on-unquoted
    // rule — the bisection kernel's `WHERE "id" >= ...` predicate uses
    // double-quoted lowercase, so the seed has to match.
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE \"{database}\".\"{schema}\".\"{table}\" AS \
             SELECT (ROW_NUMBER() OVER (ORDER BY 1) - 1) AS \"id\", \
                    'row_' || (ROW_NUMBER() OVER (ORDER BY 1) - 1)::STRING AS \"name\", \
                    (ROW_NUMBER() OVER (ORDER BY 1) - 1) * 7 AS \"value\" \
             FROM TABLE(GENERATOR(ROWCOUNT => {n}))"
        ))
        .await
        .expect("seed table");
}

async fn override_value(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
    id: u64,
    new_value: i64,
) {
    adapter
        .execute_statement(&format!(
            "UPDATE \"{database}\".\"{schema}\".\"{table}\" \
             SET \"value\" = {new_value} WHERE \"id\" = {id}"
        ))
        .await
        .expect("override value");
}

/// No-op diff bottoms out at the root level (K root chunks per side,
/// no recursion). Verification gate 3.
#[tokio::test]
#[ignore]
async fn live_noop_diff_examines_root_chunks_only() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let base_schema = format!("hc_bisection_base_{suffix}");
    let branch_schema = format!("hc_bisection_branch_{suffix}");

    create_schema(&adapter, &database, &base_schema).await;
    create_schema(&adapter, &database, &branch_schema).await;
    seed_table(&adapter, &database, &base_schema, "fact", 10_000).await;
    seed_table(&adapter, &database, &branch_schema, "fact", 10_000).await;

    let base = TableRef {
        catalog: database.clone(),
        schema: base_schema.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: database.clone(),
        schema: branch_schema.clone(),
        table: "fact".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(100),
        ..Default::default()
    };
    let target = BisectionTarget {
        base: &base,
        branch: &branch,
        pk_column: "id",
        value_columns: &value_columns,
        pk_lo: 0,
        pk_hi: 10_000,
    };

    let result = bisection_diff(&adapter, &adapter, &target, &config)
        .await
        .expect("noop bisection on Snowflake should succeed");

    drop_schema(&adapter, &database, &base_schema).await;
    drop_schema(&adapter, &database, &branch_schema).await;

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    assert!(result.samples.is_empty());
    assert_eq!(result.stats.depth_max, 0, "no recursion on no-op diff");
    assert_eq!(result.stats.leaves_materialized, 0);
    assert_eq!(
        result.stats.chunks_examined,
        u64::from(config.k) * 2,
        "no-op diff should only checksum K root chunks per side"
    );
}

/// Single planted change at id=4242 of a 10k-row table is found by
/// every run. Verification gate 2.
#[tokio::test]
#[ignore]
async fn live_finds_planted_change() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let base_schema = format!("hc_bisection_base_{suffix}");
    let branch_schema = format!("hc_bisection_branch_{suffix}");

    create_schema(&adapter, &database, &base_schema).await;
    create_schema(&adapter, &database, &branch_schema).await;
    seed_table(&adapter, &database, &base_schema, "fact", 10_000).await;
    seed_table(&adapter, &database, &branch_schema, "fact", 10_000).await;
    override_value(&adapter, &database, &branch_schema, "fact", 4_242, -1).await;

    let base = TableRef {
        catalog: database.clone(),
        schema: base_schema.clone(),
        table: "fact".into(),
    };
    let branch = TableRef {
        catalog: database.clone(),
        schema: branch_schema.clone(),
        table: "fact".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(100),
        ..Default::default()
    };
    let target = BisectionTarget {
        base: &base,
        branch: &branch,
        pk_column: "id",
        value_columns: &value_columns,
        pk_lo: 0,
        pk_hi: 10_000,
    };

    let result = bisection_diff(&adapter, &adapter, &target, &config)
        .await
        .expect("planted-change bisection on Snowflake should succeed");

    drop_schema(&adapter, &database, &base_schema).await;
    drop_schema(&adapter, &database, &branch_schema).await;

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 1, "exactly one row should differ");
    assert_eq!(result.samples.len(), 1);
    assert_eq!(result.samples[0].pk, "4242");
    assert!(
        result.stats.depth_max >= 1,
        "single-row change requires recursion"
    );
    assert_eq!(result.stats.leaves_materialized, 1);
}
