//! Live Snowflake conformance test for the safe-type-widening drift path.
//!
//! `#[ignore]`-gated because it requires a real Snowflake account.
//! Run locally with:
//!
//! ```bash
//! SNOWFLAKE_ACCOUNT=<your-account> \
//! SNOWFLAKE_WAREHOUSE=<your-warehouse> \
//! SNOWFLAKE_TEST_DATABASE=<your-database> \
//! # one of:
//! SNOWFLAKE_PAT=<your-pat> \
//! # or other supported auth modes per clone_live.rs
//! cargo test -p rocky-snowflake --test drift_widening_live -- --ignored --nocapture
//! ```
//!
//! What this test pins:
//!
//! 1. `SnowflakeSqlDialect::is_safe_type_widening` recognizes the
//!    canonical Snowflake widenings, so `detect_drift` returns
//!    `AlterColumnTypes` instead of `DropAndRecreate` on a real
//!    Snowflake table.
//! 2. The ALTER SQL produced by the default `alter_column_type_sql`
//!    impl (`ALTER COLUMN col TYPE new_type`) is accepted by Snowflake
//!    for the widening case. Closes the
//!    `feedback_sql_dialect_live_execute_tests` discipline gap.
//!
//! Without this test, the existing unit tests in `dialect.rs` would
//! pass but the integration would still ship the
//! drift-falls-through-to-DropAndRecreate bug the recon memo flagged.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::drift::detect_drift;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::{ColumnInfo, DriftAction, TableRef};
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

async fn exec(adapter: &SnowflakeWarehouseAdapter, sql: &str) {
    adapter
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {sql}\nerror: {e:?}"));
}

#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_drift_safe_widening_alters_in_place() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };

    let schema = format!("rocky_drift_widening_{}", schema_suffix());
    let table = "widening_target";

    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;

    // Create the table with the narrower types we expect to widen.
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"{table}\" (\
                \"id\" NUMBER(10,0), \
                \"name\" VARCHAR(100)\
            )"
        ),
    )
    .await;

    let table_ref = TableRef {
        catalog: database.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };

    // DESCRIBE returns the canonical Snowflake type names. These are
    // the "target" (current) types in detect_drift's vocabulary.
    let current_columns = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table");

    let mut id_current = String::new();
    let mut name_current = String::new();
    for col in &current_columns {
        match col.name.as_str() {
            "id" => id_current = col.data_type.clone(),
            "name" => name_current = col.data_type.clone(),
            _ => {}
        }
    }
    assert!(
        id_current.starts_with("NUMBER("),
        "expected NUMBER(...) for id, got {id_current}"
    );
    assert!(
        name_current.starts_with("VARCHAR("),
        "expected VARCHAR(...) for name, got {name_current}"
    );

    // Source-side (declared / new) types: widening both columns.
    let desired_columns = vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "NUMBER(38,0)".to_string(),
            nullable: true,
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "VARCHAR(1000)".to_string(),
            nullable: true,
        },
    ];

    let dialect = SnowflakeSqlDialect;
    let result = detect_drift(&table_ref, &desired_columns, &current_columns, &dialect);

    println!(
        "detect_drift -> action={:?} drifted={:?}",
        result.action, result.drifted_columns
    );

    // Drop the schema before asserting so a failed assertion still
    // leaves the sandbox clean.
    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    assert_eq!(
        result.action,
        DriftAction::AlterColumnTypes,
        "Snowflake drift should classify NUMBER(10,0) -> NUMBER(38,0) \
         and VARCHAR(100) -> VARCHAR(1000) as safe widenings, not \
         DropAndRecreate. Drifted: {:?}",
        result.drifted_columns
    );

    // Recreate so we can exercise the ALTER path end-to-end.
    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"{table}\" (\
                \"id\" NUMBER(10,0), \
                \"name\" VARCHAR(100)\
            )"
        ),
    )
    .await;

    // Generate the ALTER SQL via the dialect and execute it against
    // Snowflake. This is the receipt that the SQL form Rocky emits is
    // actually accepted by the warehouse.
    let table_str = dialect
        .format_table_ref(&database, &schema, table)
        .expect("format_table_ref");
    let alter_id = dialect
        .alter_column_type_sql(&table_str, "id", "NUMBER(38,0)")
        .expect("alter_column_type_sql id");
    let alter_name = dialect
        .alter_column_type_sql(&table_str, "name", "VARCHAR(1000)")
        .expect("alter_column_type_sql name");

    println!("emitting: {alter_id}");
    exec(&adapter, &alter_id).await;
    println!("emitting: {alter_name}");
    exec(&adapter, &alter_name).await;

    let widened = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table after alter");

    let mut id_new = String::new();
    let mut name_new = String::new();
    for col in &widened {
        match col.name.as_str() {
            "id" => id_new = col.data_type.clone(),
            "name" => name_new = col.data_type.clone(),
            _ => {}
        }
    }

    // Cleanup before assertions.
    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    assert_eq!(id_new, "NUMBER(38,0)", "id column after ALTER");
    assert_eq!(name_new, "VARCHAR(1000)", "name column after ALTER");

    println!(
        "OK: drift classified safe; ALTER accepted by Snowflake; \
         id {id_current} -> {id_new}, name {name_current} -> {name_new}"
    );
}
