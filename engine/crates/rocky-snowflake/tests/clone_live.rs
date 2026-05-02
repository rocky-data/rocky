//! Live Snowflake conformance tests for the
//! `WarehouseAdapter::clone_table_for_branch` override.
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
//! cargo test -p rocky-snowflake --test clone_live -- --ignored --nocapture
//! ```
//!
//! Two cases:
//! 1. `live_clone_produces_matching_rows` — a 5-row source is cloned
//!    into a sibling schema; the cloned table has the same row count
//!    and the same row content (sampled by `id`).
//! 2. `live_clone_is_independent_of_source` — after cloning, an
//!    `INSERT` into the clone leaves the source row count unchanged.
//!    Exercises Snowflake CLONE's copy-on-write semantics: the clone
//!    is its own table, not a view or alias.
//!
//! Cleanup follows the same imperative drop-after-unit-under-test
//! pattern as the bisection live tests: schemas are dropped CASCADE
//! before assertions so a failed assertion still leaves the sandbox
//! clean.
//!
//! **Snowflake-specific test details (load-bearing).**
//! - Identifier components in seeded SQL are wrapped in double quotes
//!   (`"id"`, `"name"`) to match the override's quoting and avoid
//!   Snowflake's case-fold-on-unquoted-identifiers rule.
//! - Schemas use the `rocky_clone_test_*` prefix with a microsecond
//!   timestamp suffix for per-run isolation.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    let connector = SnowflakeConnector::new(config, auth);
    Some((SnowflakeWarehouseAdapter::new(connector), database))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn create_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""
        ))
        .await
        .expect("create schema");
}

async fn drop_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"
        ))
        .await;
}

/// Seed a 5-row table (id, name) into the given schema. Column names
/// are double-quoted so they survive Snowflake's case-fold rule and
/// match the override's quoting style.
async fn seed_table(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
) {
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"id\" INTEGER, \"name\" STRING)"
        ))
        .await
        .expect("create source table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO \"{database}\".\"{schema}\".\"{table}\" (\"id\", \"name\") VALUES \
             (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta'), (5, 'epsilon')"
        ))
        .await
        .expect("seed rows");
}

async fn count_rows(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
) -> Option<i64> {
    let result = adapter
        .execute_query(&format!(
            "SELECT COUNT(*) AS \"n\" FROM \"{database}\".\"{schema}\".\"{table}\""
        ))
        .await
        .ok()?;
    let cell = result.rows.first()?.first()?.clone();
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
}

async fn fetch_name(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
    id: i64,
) -> Option<String> {
    let result = adapter
        .execute_query(&format!(
            "SELECT \"name\" FROM \"{database}\".\"{schema}\".\"{table}\" \
             WHERE \"id\" = {id}"
        ))
        .await
        .ok()?;
    result.rows.first()?.first()?.as_str().map(str::to_string)
}

/// Case (a): clone produces a table with matching row count + sample
/// row content.
#[tokio::test]
#[ignore]
async fn live_clone_produces_matching_rows() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let src_schema = format!("rocky_clone_test_src_{suffix}");
    let brn_schema = format!("rocky_clone_test_brn_{suffix}");
    let table = "fact";

    create_schema(&adapter, &database, &src_schema).await;
    create_schema(&adapter, &database, &brn_schema).await;
    seed_table(&adapter, &database, &src_schema, table).await;

    let source = TableRef {
        catalog: database.clone(),
        schema: src_schema.clone(),
        table: table.to_string(),
    };
    let clone_result = adapter.clone_table_for_branch(&source, &brn_schema).await;

    let clone_count = if clone_result.is_ok() {
        count_rows(&adapter, &database, &brn_schema, table).await
    } else {
        None
    };
    let sample = if clone_result.is_ok() {
        fetch_name(&adapter, &database, &brn_schema, table, 3).await
    } else {
        None
    };

    drop_schema(&adapter, &database, &src_schema).await;
    drop_schema(&adapter, &database, &brn_schema).await;

    clone_result.expect("clone_table_for_branch should succeed");
    assert_eq!(
        clone_count,
        Some(5),
        "cloned table should have 5 rows, got {clone_count:?}"
    );
    assert_eq!(
        sample.as_deref(),
        Some("gamma"),
        "row id=3 in clone should round-trip the seeded name 'gamma'"
    );
}

/// Case (b): cloned table is independent of source. Inserting into the
/// clone leaves the source untouched. Exercises Snowflake CLONE's
/// copy-on-write semantics — the clone shares micropartitions with
/// the source initially but a write diverges them.
#[tokio::test]
#[ignore]
async fn live_clone_is_independent_of_source() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let src_schema = format!("rocky_clone_test_src_{suffix}");
    let brn_schema = format!("rocky_clone_test_brn_{suffix}");
    let table = "fact";

    create_schema(&adapter, &database, &src_schema).await;
    create_schema(&adapter, &database, &brn_schema).await;
    seed_table(&adapter, &database, &src_schema, table).await;

    let source = TableRef {
        catalog: database.clone(),
        schema: src_schema.clone(),
        table: table.to_string(),
    };
    let clone_result = adapter.clone_table_for_branch(&source, &brn_schema).await;

    // Diverge the clone with one extra row; the source must stay at 5.
    let insert_result = if clone_result.is_ok() {
        adapter
            .execute_statement(&format!(
                "INSERT INTO \"{database}\".\"{brn_schema}\".\"{table}\" (\"id\", \"name\") \
                 VALUES (6, 'zeta')"
            ))
            .await
    } else {
        Ok(())
    };

    let source_count_after = count_rows(&adapter, &database, &src_schema, table).await;
    let clone_count_after = count_rows(&adapter, &database, &brn_schema, table).await;

    drop_schema(&adapter, &database, &src_schema).await;
    drop_schema(&adapter, &database, &brn_schema).await;

    clone_result.expect("clone_table_for_branch should succeed");
    insert_result.expect("insert into clone should succeed");
    assert_eq!(
        source_count_after,
        Some(5),
        "source row count must be unchanged after writing to the clone, got {source_count_after:?}"
    );
    assert_eq!(
        clone_count_after,
        Some(6),
        "clone row count must reflect the inserted row, got {clone_count_after:?}"
    );
}
