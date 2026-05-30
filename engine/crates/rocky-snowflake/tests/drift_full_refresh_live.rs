//! Live Snowflake conformance tests for the non-widening drift paths.
//!
//! `#[ignore]`-gated; needs a real Snowflake account (same `SNOWFLAKE_*` env
//! vars as the other live tests). Complements `drift_widening_live.rs`, which
//! covers the safe-widening → `AlterColumnTypes` path. Here we pin:
//!
//! 1. **Unsafe narrowing → `DropAndRecreate`.** `NUMBER(38,0) → NUMBER(10,0)`
//!    and `VARCHAR(1000) → VARCHAR(100)` are NOT safe widenings, so
//!    `detect_drift` must return `DropAndRecreate` (an in-place `ALTER` would
//!    lose data or be rejected). The generated `DROP TABLE` SQL is then
//!    executed and the table recreated at the narrowed schema, so the
//!    full-refresh cycle is validated end-to-end against Snowflake.
//! 2. **Added column → `Ignore` action + populated `added_columns`.** A column
//!    present in the source but not the target is surfaced in `added_columns`
//!    (the action stays `Ignore` because no existing column's type drifted),
//!    and the `generate_add_column_sql` `ALTER TABLE ... ADD COLUMN` Rocky
//!    emits is accepted by Snowflake.
//!
//! A wiremock test can't catch a misclassification that ships warehouse-
//! rejected SQL — the `feedback_sql_dialect_live_execute_tests` discipline.
//!
//! Schemas use the `hc_drift_*` prefix per Hugo's Snowflake convention; the
//! per-run microsecond suffix isolates parallel runs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::drift::{detect_drift, generate_add_column_sql, generate_drop_table_sql};
use rocky_core::traits::WarehouseAdapter;
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

/// Unsafe narrowing must classify as `DropAndRecreate`, and the generated drop
/// + recreate cycle must execute against Snowflake.
#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_unsafe_narrowing_is_drop_and_recreate() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };
    let schema = format!("hc_drift_narrow_{}", schema_suffix());
    let table = "narrowing_target";

    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;
    // Start WIDE.
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"id\" NUMBER(38,0), \"name\" VARCHAR(1000))"
        ),
    )
    .await;

    let table_ref = TableRef {
        catalog: database.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };
    let current_columns = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table");

    // Desired = NARROWER types. Narrowing is not a safe widening.
    let desired_columns = vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "NUMBER(10,0)".to_string(),
            nullable: true,
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "VARCHAR(100)".to_string(),
            nullable: true,
        },
    ];

    let dialect = SnowflakeSqlDialect;
    let result = detect_drift(&table_ref, &desired_columns, &current_columns, &dialect);
    println!(
        "detect_drift(narrowing) -> action={:?} drifted={:?}",
        result.action, result.drifted_columns
    );

    // Apply the full-refresh cycle: generated DROP, then recreate at the new
    // (narrow) schema. Plain CREATE (no OR REPLACE) makes the DROP's success
    // load-bearing — if the drop silently failed, this CREATE errors.
    let drop_sql = generate_drop_table_sql(&table_ref, &dialect).expect("generate_drop_table_sql");
    println!("emitting: {drop_sql}");
    exec(&adapter, &drop_sql).await;
    exec(
        &adapter,
        &format!(
            "CREATE TABLE \"{database}\".\"{schema}\".\"{table}\" \
             (\"id\" NUMBER(10,0), \"name\" VARCHAR(100))"
        ),
    )
    .await;
    let recreated = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table after recreate");

    // Cleanup before assertions.
    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    assert_eq!(
        result.action,
        DriftAction::DropAndRecreate,
        "narrowing NUMBER(38,0)->NUMBER(10,0) and VARCHAR(1000)->VARCHAR(100) must be \
         DropAndRecreate, not an in-place ALTER. Drifted: {:?}",
        result.drifted_columns
    );
    let id_new = recreated
        .iter()
        .find(|c| c.name == "id")
        .map(|c| c.data_type.as_str());
    let name_new = recreated
        .iter()
        .find(|c| c.name == "name")
        .map(|c| c.data_type.as_str());
    assert_eq!(id_new, Some("NUMBER(10,0)"), "recreated id type");
    assert_eq!(name_new, Some("VARCHAR(100)"), "recreated name type");
}

/// An added column surfaces in `added_columns` (action `Ignore`), and the
/// generated `ALTER TABLE ... ADD COLUMN` is accepted by Snowflake.
#[tokio::test]
#[ignore = "requires live Snowflake account"]
async fn live_added_column_is_ignore_and_add_column_applies() {
    let Some((adapter, database)) = adapter_from_env() else {
        eprintln!("SNOWFLAKE_* env vars unset; skipping");
        return;
    };
    let schema = format!("hc_drift_add_{}", schema_suffix());
    let table = "add_column_target";

    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS \"{database}\".\"{schema}\""),
    )
    .await;
    exec(
        &adapter,
        &format!("CREATE TABLE \"{database}\".\"{schema}\".\"{table}\" (\"id\" NUMBER(38,0))"),
    )
    .await;

    let table_ref = TableRef {
        catalog: database.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };
    let current_columns = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table");

    // Desired = the existing columns verbatim (so no type drift) + one new
    // nullable column. Reusing the described types guarantees the only
    // difference is the addition.
    let mut desired_columns = current_columns.clone();
    desired_columns.push(ColumnInfo {
        name: "region".to_string(),
        data_type: "VARCHAR(50)".to_string(),
        nullable: true,
    });

    let dialect = SnowflakeSqlDialect;
    let result = detect_drift(&table_ref, &desired_columns, &current_columns, &dialect);
    println!(
        "detect_drift(added) -> action={:?} added={:?}",
        result.action, result.added_columns
    );

    let add_sql = generate_add_column_sql(&table_ref, &result.added_columns, &dialect)
        .expect("add column sql");
    for stmt in &add_sql {
        println!("emitting: {stmt}");
        exec(&adapter, stmt).await;
    }
    let after = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe_table after add");

    // Cleanup before assertions.
    exec(
        &adapter,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\" CASCADE"),
    )
    .await;

    assert_eq!(
        result.action,
        DriftAction::Ignore,
        "a pure column addition (no type drift on existing columns) classifies as Ignore; \
         adds are applied via added_columns, not the action"
    );
    assert!(
        result.added_columns.iter().any(|c| c.name == "region"),
        "the new column must be surfaced in added_columns; got {:?}",
        result.added_columns
    );
    assert!(
        after.iter().any(|c| c.name.eq_ignore_ascii_case("region")),
        "ADD COLUMN must have applied; describe should show the new column. Got {after:?}"
    );
}
