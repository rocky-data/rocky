//! Live Snowflake conformance tests for `insert_overwrite_partition`.
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
//! cargo test -p rocky-snowflake --test insert_overwrite_partition_live \
//!   -- --ignored --nocapture
//! ```
//!
//! **What this pins.** Snowflake's `/api/v2/statements` runs ONE statement per
//! call by default — every call opens an independent session, so emitting
//! BEGIN/DELETE/INSERT/COMMIT as four separate REST calls is silently
//! non-transactional: each call autocommits, `BEGIN` is discarded, the
//! `DELETE` autocommits, and the trailing `COMMIT`/`ROLLBACK` is a no-op.
//!
//! The dialect now returns a single semicolon-joined script, and the
//! connector sets `MULTI_STATEMENT_COUNT` (plus a prepended `ALTER SESSION SET
//! TRANSACTION_ABORT_ON_ERROR = TRUE`) so Snowflake parses and runs all the
//! statements as one transactional unit that aborts on error. The happy-path
//! test (`commits_on_success`) is the load-bearing proof: a replaced target
//! partition with the untouched sibling can only result from DELETE + INSERT +
//! COMMIT all running in one transaction. The partial-failure test plants a
//! SQL-level error at the INSERT (a type-mismatched literal into a numeric
//! column) and asserts the partition's pre-existing rows survive — i.e.
//! partition integrity is preserved on failure. (Row counts are read in
//! separate sessions, which see only committed data, so they confirm
//! integrity without distinguishing error-time rollback from a never-committed
//! transaction; the assertion that the error is the *planted* one guards
//! against false-passing on a pre-execution API rejection.)
//!
//! Schemas use the `hc_iop_*` prefix per Hugo's Databricks/Snowflake
//! convention; the per-run microsecond suffix isolates parallel runs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
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

async fn seed_fact(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str, table: &str) {
    // Two partitions: '2026-04-07' and '2026-04-08'. The
    // insert_overwrite_partition script targets only the '2026-04-08'
    // partition; the other partition's rows must remain untouched
    // regardless of test outcome.
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"order_date\" DATE, \"qty\" NUMBER(10, 0))"
        ))
        .await
        .expect("create fact table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO \"{database}\".\"{schema}\".\"{table}\" \
             (\"order_date\", \"qty\") VALUES \
             (DATE '2026-04-07', 11), \
             (DATE '2026-04-07', 12), \
             (DATE '2026-04-08', 21), \
             (DATE '2026-04-08', 22), \
             (DATE '2026-04-08', 23)"
        ))
        .await
        .expect("seed rows");
}

async fn count_rows_in_partition(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
    date_literal: &str,
) -> Option<i64> {
    let result = adapter
        .execute_query(&format!(
            "SELECT COUNT(*) AS \"n\" FROM \"{database}\".\"{schema}\".\"{table}\" \
             WHERE \"order_date\" = DATE '{date_literal}'"
        ))
        .await
        .ok()?;
    let cell = result.rows.first()?.first()?.clone();
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
}

/// Verification gate for Bug 1: `insert_overwrite_partition` is transactional.
///
/// Builds the four-statement script via the dialect (the production code path)
/// but substitutes the INSERT's SELECT with a SQL-level failure that fires
/// AFTER the DELETE would have committed under the old non-transactional
/// implementation. Asserts the pre-existing partition rows survive AND that the
/// failure is the planted numeric error (not a pre-execution API rejection) —
/// i.e. the script ran as one aborting transaction and partition integrity was
/// preserved on failure.
#[tokio::test]
#[ignore]
async fn live_insert_overwrite_partition_rolls_back_on_partial_failure() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("hc_iop_{suffix}");
    let table = "fct_daily_orders";

    create_schema(&adapter, &database, &schema).await;
    seed_fact(&adapter, &database, &schema, table).await;

    let target = format!("\"{database}\".\"{schema}\".\"{table}\"");
    let partition_filter = "\"order_date\" = DATE '2026-04-08'";
    // Deliberately bad SELECT: projects a string literal into the numeric
    // `qty` column. Snowflake raises a type-conversion error at the INSERT
    // statement, AFTER the script's DELETE has run. Without the
    // MULTI_STATEMENT_COUNT fix, the DELETE would have autocommitted and the
    // partition would be empty.
    let bad_select = "SELECT DATE '2026-04-08' AS \"order_date\", 'not_a_number' AS \"qty\"";

    let stmts = adapter
        .dialect()
        .insert_overwrite_partition(&target, partition_filter, bad_select)
        .expect("dialect should build script");
    assert_eq!(
        stmts.len(),
        1,
        "Snowflake insert_overwrite_partition should emit a single \
         semicolon-joined script (got {} stmts)",
        stmts.len(),
    );
    let script = &stmts[0];
    // Execute the script as the runner does — one REST call.
    let exec_result = adapter.execute_statement(script).await;

    // Capture state BEFORE cleanup so failed assertions still leave a
    // clean sandbox.
    let target_partition_count =
        count_rows_in_partition(&adapter, &database, &schema, table, "2026-04-08").await;
    let other_partition_count =
        count_rows_in_partition(&adapter, &database, &schema, table, "2026-04-07").await;

    drop_schema(&adapter, &database, &schema).await;

    let err = exec_result.expect_err("script with malformed INSERT should fail end-to-end, got Ok");
    // The failure must be the planted numeric type-conversion error raised at
    // the INSERT — proof the script actually executed through DELETE to INSERT
    // in one session. A pre-execution API rejection (e.g. a forbidden session
    // parameter, HTTP 400 / code 391917) ALSO satisfies `is_err()` and leaves
    // the partition untouched, so a bare `is_err()` here false-passes when the
    // request never runs at all. Match on the planted literal / Snowflake's
    // numeric-conversion message so this can only pass when execution genuinely
    // reached the INSERT.
    let err_text = format!("{err:?}");
    assert!(
        err_text.contains("not_a_number") || err_text.contains("Numeric value"),
        "expected the planted numeric type-conversion failure at the INSERT, but got a \
         different error — did the request bounce before executing? {err_text}"
    );
    assert_eq!(
        target_partition_count,
        Some(3),
        "rolled-back DELETE: 2026-04-08 partition must still hold its 3 seeded rows; \
         got {target_partition_count:?}. If this is 0, BEGIN/DELETE/INSERT/COMMIT ran \
         as four independent sessions and the DELETE autocommitted (Bug 1 regression)."
    );
    assert_eq!(
        other_partition_count,
        Some(2),
        "non-targeted partition 2026-04-07 must be unchanged; got {other_partition_count:?}"
    );
}

/// Happy-path gate: a well-formed script commits all 4 statements as one
/// transaction, replacing the target partition while leaving the other
/// partition untouched.
#[tokio::test]
#[ignore]
async fn live_insert_overwrite_partition_commits_on_success() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("hc_iop_ok_{suffix}");
    let table = "fct_daily_orders";

    create_schema(&adapter, &database, &schema).await;
    seed_fact(&adapter, &database, &schema, table).await;

    let target = format!("\"{database}\".\"{schema}\".\"{table}\"");
    let partition_filter = "\"order_date\" = DATE '2026-04-08'";
    // Good SELECT: emits exactly one row into the target partition.
    let good_select = "SELECT DATE '2026-04-08' AS \"order_date\", 999 AS \"qty\"";

    let stmts = adapter
        .dialect()
        .insert_overwrite_partition(&target, partition_filter, good_select)
        .expect("dialect should build script");
    let script = &stmts[0];
    let exec_result = adapter.execute_statement(script).await;

    let target_partition_count =
        count_rows_in_partition(&adapter, &database, &schema, table, "2026-04-08").await;
    let other_partition_count =
        count_rows_in_partition(&adapter, &database, &schema, table, "2026-04-07").await;

    drop_schema(&adapter, &database, &schema).await;

    exec_result.expect("happy-path script should commit");
    assert_eq!(
        target_partition_count,
        Some(1),
        "committed INSERT replaces target partition with one new row, got {target_partition_count:?}"
    );
    assert_eq!(
        other_partition_count,
        Some(2),
        "non-targeted partition 2026-04-07 must be unchanged; got {other_partition_count:?}"
    );
}
