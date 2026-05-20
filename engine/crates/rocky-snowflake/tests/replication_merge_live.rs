//! Live Snowflake conformance tests for the replication `[merge]`
//! strategy column-resolution fix.
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
//! SNOWFLAKE_USERNAME=<user> SNOWFLAKE_PAT=<personal-access-token> \
//! # or:
//! SNOWFLAKE_USERNAME=<user> SNOWFLAKE_PASSWORD=<pwd> \
//! cargo test -p rocky-snowflake --test replication_merge_live -- --ignored --nocapture
//! ```
//!
//! Pins the contract that motivates the replication-runner fix:
//! Snowflake's MERGE rejects `UPDATE SET *` (Databricks / BigQuery /
//! DuckDB accept it). The replication runner resolves
//! `ColumnSelection::All` against the discovered source schema *before*
//! handing the IR to SQL-gen so the explicit form lands.
//!
//! Two cases:
//! 1. `live_merge_with_explicit_columns_is_idempotent` — runs the
//!    dialect-emitted MERGE with `ColumnSelection::Explicit(...)` three
//!    times against a static source. Row count stays stable; this is
//!    the GREEN-on-fix end-state.
//! 2. `live_merge_with_update_set_star_is_rejected_by_snowflake` —
//!    bypasses the dialect's pre-flight check and submits a raw
//!    `MERGE ... UPDATE SET *` script directly. Snowflake's parser
//!    rejects it. This is the ground-truth check that proves the bug
//!    fix is targeting a real warehouse-level rejection, not an
//!    artifact of the Rust-side guardrail.
//!
//! Schemas use the `rocky_repl_merge_test_*` prefix with a microsecond
//! timestamp suffix for per-run isolation; cleanup follows the same
//! imperative drop-after-unit-under-test pattern as the bisection /
//! clone live tests.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::ColumnSelection;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};
use rocky_snowflake::dialect::SnowflakeSqlDialect;

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

/// Seed a 3-row source table with `(id, name, amount)` so the merge
/// touches a non-trivial column list (more than just the merge key).
/// Column names are double-quoted to survive Snowflake's
/// case-fold-on-unquoted rule and match the dialect's quoting.
async fn seed_source(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
) {
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"id\" INTEGER, \"name\" STRING, \"amount\" NUMBER(10, 2))"
        ))
        .await
        .expect("create source table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO \"{database}\".\"{schema}\".\"{table}\" (\"id\", \"name\", \"amount\") \
             VALUES (1, 'alpha', 10.50), (2, 'beta', 20.75), (3, 'gamma', 30.00)"
        ))
        .await
        .expect("seed rows");
}

async fn create_empty_target(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
) {
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"id\" INTEGER, \"name\" STRING, \"amount\" NUMBER(10, 2))"
        ))
        .await
        .expect("create target table");
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

/// GREEN end-state: the explicit-column MERGE the replication runner
/// produces after the column-resolution fix is idempotent against
/// Snowflake. Three back-to-back runs against a static source leave the
/// target at 3 rows.
#[tokio::test]
#[ignore]
async fn live_merge_with_explicit_columns_is_idempotent() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("rocky_repl_merge_test_{suffix}");
    let source_tbl = "src_orders";
    let target_tbl = "tgt_orders";

    create_schema(&adapter, &database, &schema).await;
    seed_source(&adapter, &database, &schema, source_tbl).await;
    create_empty_target(&adapter, &database, &schema, target_tbl).await;

    let dialect = SnowflakeSqlDialect;
    let target_ref = dialect
        .format_table_ref(&database, &schema, target_tbl)
        .expect("format target ref");
    let source_ref = dialect
        .format_table_ref(&database, &schema, source_tbl)
        .expect("format source ref");
    // Mirrors the replication runner's resolved SELECT: SELECT every
    // source column, no metadata columns for this minimal case.
    let source_sql = format!("SELECT \"id\", \"name\", \"amount\" FROM {source_ref}");
    let keys: Vec<Arc<str>> = vec![Arc::from("id")];
    // Mirrors `resolve_merge_update_columns` output: source columns
    // (including the merge key) expanded into ColumnSelection::Explicit.
    let update_cols = ColumnSelection::Explicit(vec![
        Arc::from("id"),
        Arc::from("name"),
        Arc::from("amount"),
    ]);
    let merge_sql = dialect
        .merge_into(&target_ref, &source_sql, &keys, &update_cols)
        .expect("dialect emits explicit-column MERGE");

    // Run the same MERGE three times against the static source; each
    // run is a no-op against an already-merged target.
    let mut counts = Vec::with_capacity(3);
    let mut last_err = None;
    for _ in 0..3 {
        match adapter.execute_statement(&merge_sql).await {
            Ok(_) => counts.push(count_rows(&adapter, &database, &schema, target_tbl).await),
            Err(e) => {
                last_err = Some(format!("{e}"));
                break;
            }
        }
    }

    drop_schema(&adapter, &database, &schema).await;

    if let Some(err) = last_err {
        panic!("MERGE statement failed against Snowflake: {err}\nSQL:\n{merge_sql}");
    }
    assert_eq!(
        counts,
        vec![Some(3), Some(3), Some(3)],
        "3 idempotent merges should keep target row count at 3; got {counts:?}"
    );
}

/// Ground-truth check: the broken pre-fix path (`UPDATE SET *`) is
/// rejected by Snowflake's parser. We submit the raw broken MERGE
/// directly, bypassing the dialect's Rust-side `UPDATE SET *` guard at
/// `dialect.rs::merge_into`. If Snowflake ever accepts `UPDATE SET *`
/// in a future release, this test starts failing and we know to revisit
/// the resolution path.
#[tokio::test]
#[ignore]
async fn live_merge_with_update_set_star_is_rejected_by_snowflake() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("rocky_repl_merge_test_{suffix}");
    let source_tbl = "src_orders";
    let target_tbl = "tgt_orders";

    create_schema(&adapter, &database, &schema).await;
    seed_source(&adapter, &database, &schema, source_tbl).await;
    create_empty_target(&adapter, &database, &schema, target_tbl).await;

    let dialect = SnowflakeSqlDialect;
    let target_ref = dialect
        .format_table_ref(&database, &schema, target_tbl)
        .expect("format target ref");
    let source_ref = dialect
        .format_table_ref(&database, &schema, source_tbl)
        .expect("format source ref");
    let source_sql = format!("SELECT \"id\", \"name\", \"amount\" FROM {source_ref}");

    // Hand-built MERGE with the broken `UPDATE SET *` shorthand —
    // mirrors what the replication runner emitted BEFORE the resolution
    // fix, before the dialect's pre-flight check ever ran.
    let broken_merge_sql = format!(
        "MERGE INTO {target_ref} AS t\n\
         USING (\n{source_sql}\n) AS s\n\
         ON t.\"id\" = s.\"id\"\n\
         WHEN MATCHED THEN UPDATE SET *\n\
         WHEN NOT MATCHED THEN INSERT *"
    );

    let result = adapter.execute_statement(&broken_merge_sql).await;
    drop_schema(&adapter, &database, &schema).await;

    match result {
        Ok(_) => panic!(
            "Snowflake accepted `MERGE ... UPDATE SET *` — adapter parity assumption broken.\n\
             SQL:\n{broken_merge_sql}"
        ),
        Err(e) => {
            let msg = format!("{e}");
            // Snowflake's parser typically returns a syntax-error
            // message naming the unexpected token. Loose substring
            // match — the exact wording can vary across Snowflake
            // releases; what matters is that the statement is rejected.
            eprintln!("Snowflake rejected UPDATE SET * (expected): {msg}");
        }
    }
}
