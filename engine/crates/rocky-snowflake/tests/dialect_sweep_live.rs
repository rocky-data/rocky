//! Live Snowflake dialect-sweep regression probes.
//!
//! These are the live execute-coverage regression tests for the dialect
//! adapter-gating & quoting fixes (D1/D3/D4). They run the SQL Rocky
//! *actually emits* (via the real emitter functions, not hand-written)
//! against a real Snowflake account, and pin the POST-FIX behaviour:
//!
//!   - D1 (lakehouse `USING <format>` DDL) and D3 (compact/archive
//!     `OPTIMIZE`/`VACUUM`) now fail fast *in the engine* — the generator
//!     refuses with a known-limitation error BEFORE any SQL is produced,
//!     so the probes assert the in-process `Err` rather than a warehouse
//!     syntax error.
//!   - D4 (watermark quoting): `watermark_where` now double-quotes the
//!     column, so a column STORED lowercase (the quoted-create /
//!     bisection-seed path) resolves; an unquoted-uppercase-stored column
//!     no longer matches the now-quoted reference. The probe pins that
//!     trade-off.
//!
//! `#[ignore]`-gated; reads connection params from the SAME env vars the
//! other live tests use, so workspace identifiers are never committed:
//!   SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
//!   SNOWFLAKE_TEST_DATABASE (or SNOWFLAKE_DATABASE),
//!   SNOWFLAKE_USERNAME, SNOWFLAKE_PAT, SNOWFLAKE_ROLE
//!
//! Run with:
//!   SNOWFLAKE_PAT=<pat> cargo test -p rocky-snowflake --test dialect_sweep_live \
//!     -- --ignored --nocapture

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::lakehouse::{self, LakehouseError, LakehouseFormat, LakehouseOptions};
use rocky_core::sql_gen::{SqlGenError, archive_from_ir, compact_from_ir};
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::{ArchivePlanIr, CompactPlanIr};
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
        // NOTE: the sandbox `.env` mislabels its PAT as SNOWFLAKE_PASSWORD;
        // it must be supplied via SNOWFLAKE_PAT. We deliberately do NOT read
        // SNOWFLAKE_PASSWORD here so a password-mode auth can't be selected
        // by mistake.
        password: None,
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

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn drop_schema(adapter: &SnowflakeWarehouseAdapter, db: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS \"{db}\".\"{schema}\" CASCADE"
        ))
        .await;
}

/// D1 — lakehouse `USING DELTA|ICEBERG` DDL is Spark/Databricks-only.
/// POST-FIX: `generate_lakehouse_ddl` refuses on the Snowflake dialect with
/// `LakehouseError::DialectUnsupported` BEFORE producing any SQL, so the
/// engine never sends Spark-only DDL to the warehouse.
#[tokio::test]
#[ignore]
async fn d1_lakehouse_using_format_ddl_refused_in_engine() {
    let Some((adapter, db)) = adapter_from_env() else {
        eprintln!("SKIP d1: Snowflake env not set");
        return;
    };
    let schema = format!("hc_dialsweep_d1_{}", suffix());
    drop_schema(&adapter, &db, &schema).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA \"{db}\".\"{schema}\""))
        .await
        .expect("create schema");

    let dialect = SnowflakeSqlDialect;
    let target = dialect
        .format_table_ref(&db, &schema, "hc_probe")
        .expect("format target");
    let opts = LakehouseOptions::default();

    // CONTROL: plain CTAS (no USING) MUST succeed — proves connectivity +
    // valid schema/select/identifiers.
    let control_sql = format!("CREATE OR REPLACE TABLE {target} AS SELECT 1 AS id");
    let control = adapter.execute_statement(&control_sql).await;

    // POST-FIX: the engine refuses before producing SQL — no warehouse round
    // trip happens for the format DDL at all.
    let mut results = Vec::new();
    for fmt in [LakehouseFormat::DeltaTable, LakehouseFormat::IcebergTable] {
        let outcome =
            lakehouse::generate_lakehouse_ddl(&fmt, &target, "SELECT 1 AS id", &opts, &dialect);
        results.push((format!("{fmt}"), outcome));
    }

    drop_schema(&adapter, &db, &schema).await;

    control.expect("CONTROL plain CTAS must succeed — proves warehouse reached");
    println!("\n=== D1 Snowflake: lakehouse USING <format> DDL (engine refusal) ===");
    println!("CONTROL (plain CTAS, no USING): SUCCEEDED → warehouse reached, schema/select valid");
    for (fmt, outcome) in results {
        match outcome {
            Ok(sql) => panic!("[{fmt}] expected engine refusal but got SQL: {sql:?}"),
            Err(LakehouseError::DialectUnsupported { format, dialect }) => {
                assert_eq!(dialect, "snowflake");
                println!(
                    "[{fmt}] VERDICT: engine refused before SQL — \
                     DialectUnsupported {{ format: {format:?}, dialect: {dialect:?} }}"
                );
            }
            Err(other) => panic!("[{fmt}] expected DialectUnsupported, got {other:?}"),
        }
    }
}

/// D3a — `OPTIMIZE` (compact) is Databricks/Delta-only.
/// POST-FIX: `compact_from_ir` refuses on the Snowflake dialect with
/// `SqlGenError::UnsupportedMaintenance` BEFORE producing any SQL.
#[tokio::test]
#[ignore]
async fn d3a_compact_refused_in_engine() {
    let Some((adapter, db)) = adapter_from_env() else {
        eprintln!("SKIP d3a: Snowflake env not set");
        return;
    };
    let schema = format!("HC_DIALSWEEP_D3A_{}", suffix());
    drop_schema(&adapter, &db, &schema).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA \"{db}\".\"{schema}\""))
        .await
        .expect("create schema");
    let table_ref = format!("{db}.{schema}.hc_probe");
    let control = adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {table_ref} AS SELECT 1 AS id"
        ))
        .await;

    // POST-FIX: the gated generator refuses before producing any SQL.
    let ir = CompactPlanIr::for_table(&table_ref, 256);
    let outcome = compact_from_ir(&ir, &SnowflakeSqlDialect);

    drop_schema(&adapter, &db, &schema).await;

    control.expect("CONTROL CTAS must succeed — proves warehouse reached + table exists");
    println!("\n=== D3a Snowflake: OPTIMIZE (compact) engine refusal ===");
    println!("CONTROL CTAS: SUCCEEDED → warehouse reached, table exists");
    match outcome {
        Ok(stmts) => panic!("expected engine refusal but compact_from_ir produced SQL: {stmts:?}"),
        Err(SqlGenError::UnsupportedMaintenance { operation, dialect }) => {
            assert_eq!(operation, "OPTIMIZE");
            assert_eq!(dialect, "snowflake");
            println!(
                "VERDICT: engine refused before SQL — UnsupportedMaintenance(OPTIMIZE, snowflake)"
            );
        }
        Err(other) => panic!("expected UnsupportedMaintenance, got {other:?}"),
    }
}

/// D3b/D3c — archive (`DELETE … DATEADD` + `VACUUM`) bundle is
/// Databricks/Delta-only. POST-FIX: `archive_from_ir` refuses on the
/// Snowflake dialect with `SqlGenError::UnsupportedMaintenance` (naming
/// `VACUUM`, the universally-unsupported op) BEFORE producing any SQL —
/// closing the gap where the native-DATEADD DELETE half would run but the
/// trailing VACUUM could not.
#[tokio::test]
#[ignore]
async fn d3_archive_refused_in_engine() {
    let Some((adapter, db)) = adapter_from_env() else {
        eprintln!("SKIP d3 archive: Snowflake env not set");
        return;
    };
    let schema = format!("HC_DIALSWEEP_D3C_{}", suffix());
    drop_schema(&adapter, &db, &schema).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA \"{db}\".\"{schema}\""))
        .await
        .expect("create schema");
    let table_ref = format!("{db}.{schema}.hc_probe");
    let control = adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {table_ref} AS \
             SELECT 1 AS id, CURRENT_TIMESTAMP() AS _fivetran_synced"
        ))
        .await;

    // POST-FIX: the gated generator refuses before producing any SQL.
    let ir = ArchivePlanIr::for_table(&table_ref, 90);
    let outcome = archive_from_ir(&ir, &SnowflakeSqlDialect);

    drop_schema(&adapter, &db, &schema).await;

    control.expect("CONTROL CTAS must succeed — proves warehouse reached + table exists");
    println!("\n=== D3 Snowflake: archive bundle engine refusal ===");
    println!("CONTROL CTAS (with _fivetran_synced col): SUCCEEDED → warehouse reached");
    match outcome {
        Ok(stmts) => panic!("expected engine refusal but archive_from_ir produced SQL: {stmts:?}"),
        Err(SqlGenError::UnsupportedMaintenance { operation, dialect }) => {
            assert_eq!(operation, "VACUUM");
            assert_eq!(dialect, "snowflake");
            println!(
                "VERDICT: engine refused before SQL — UnsupportedMaintenance(VACUUM, snowflake)"
            );
        }
        Err(other) => panic!("expected UnsupportedMaintenance, got {other:?}"),
    }
}

/// D4 — Snowflake watermark quoting.
/// POST-FIX: `watermark_where` double-quotes the column, matching the
/// `format_table_ref` / `merge_into` path. The fix's payload is that a
/// column STORED lowercase (the quoted-create / bisection-seed path) now
/// RESOLVES. The deliberate trade-off: an unquoted-UPPERCASE-stored column
/// no longer matches the now-quoted reference.
#[tokio::test]
#[ignore]
async fn d4_watermark_quoted_resolves_lowercase_stored() {
    let Some((adapter, db)) = adapter_from_env() else {
        eprintln!("SKIP d4: Snowflake env not set");
        return;
    };
    let schema = format!("HC_DIALSWEEP_D4_{}", suffix());
    drop_schema(&adapter, &db, &schema).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA \"{db}\".\"{schema}\""))
        .await
        .expect("create schema");

    let dialect = SnowflakeSqlDialect;
    // POST-FIX the emitter produces `WHERE "ts" > '1970-...'::TIMESTAMP_NTZ`
    // with `ts` DOUBLE-QUOTED.
    let where_clause = dialect
        .watermark_where("ts", None)
        .expect("watermark_where");
    assert!(
        where_clause.contains("\"ts\""),
        "post-fix watermark_where must double-quote the column, got: {where_clause}"
    );

    // CASE B (the fix's payload + connectivity proof): column created
    // QUOTED-lowercase → stored `ts` → the now-quoted ref matches → RESOLVES.
    let tbl_b = format!("{db}.{schema}.hc_quoted");
    let create_b = format!("CREATE OR REPLACE TABLE {tbl_b} (\"ts\" TIMESTAMP)");
    adapter
        .execute_statement(&create_b)
        .await
        .expect("create quoted-col table");
    let select_b = format!("SELECT COUNT(*) AS n FROM {tbl_b}\n{where_clause}");
    let res_b = adapter.execute_query(&select_b).await;

    // CASE A (the deliberate trade-off): column created UNQUOTED → stored
    // `TS` → the now-quoted (case-sensitive) ref does NOT match → fails.
    let tbl_a = format!("{db}.{schema}.hc_unquoted");
    let create_a = format!("CREATE OR REPLACE TABLE {tbl_a} (ts TIMESTAMP)");
    adapter
        .execute_statement(&create_a)
        .await
        .expect("create unquoted-col table");
    let select_a = format!("SELECT COUNT(*) AS n FROM {tbl_a}\n{where_clause}");
    let res_a = adapter.execute_query(&select_a).await;

    drop_schema(&adapter, &db, &schema).await;

    println!("\n=== D4 Snowflake: quoted watermark resolves lowercase-stored ===");
    println!("GENERATED WHERE (emitter, col QUOTED): {where_clause}");

    // Connectivity proof + the fix's payload: CASE B must resolve & run.
    let b = res_b.expect(
        "CASE B quoted-lowercase-col SELECT must succeed — this is the fix's payload AND the \
         connectivity proof; if it failed the warehouse may be unreachable → INCONCLUSIVE",
    );
    println!(
        "[CASE B: col created QUOTED-lowercase `\"ts\"` → stored ts] RESOLVED, rows={:?} \
         → FIXED (quoted ref matches lowercase-stored column)",
        b.rows
    );

    // Characterise the deliberate trade-off: CASE A (stored UPPER) no longer
    // matches the quoted reference.
    match &res_a {
        Ok(q) => println!(
            "[CASE A: col created UNQUOTED `ts` → stored TS] resolved unexpectedly, rows={:?}",
            q.rows
        ),
        Err(e) => {
            println!(
                "[CASE A: col created UNQUOTED `ts` → stored TS] errored as expected: {e} \
                 → deliberate trade-off (a fixed-case quoted ident can't match both casings)"
            );
            assert!(
                e.to_string().to_uppercase().contains("INVALID IDENTIFIER"),
                "CASE A should fail with invalid identifier (case-sensitive quoted ref): {e}"
            );
        }
    }
}
