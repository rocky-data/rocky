//! Live Snowflake conformance test for the loader's
//! create-the-target-table-from-a-local-CSV path
//! ([`SnowflakeLoaderAdapter`] + `LoadOptions::create_table`).
//!
//! `#[ignore]`-gated because it requires a real Snowflake account. Run
//! locally with:
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
//! # optional:
//! SNOWFLAKE_ROLE=<role> \
//! cargo test -p rocky-snowflake --test load_create_live -- --ignored --nocapture
//! ```
//!
//! The env-var convention mirrors `clone_live::adapter_from_env` /
//! `bisection_live` verbatim; nothing is hardcoded.
//!
//! What it proves: loading a local CSV into a table that does NOT exist yet,
//! with `create_table` set, (1) creates the table with column TYPES inferred
//! from the CSV (id -> NUMBER(38,0), name -> VARCHAR, score -> FLOAT, active
//! -> BOOLEAN, joined_at -> TIMESTAMP_NTZ) and (2) lands the rows.
//!
//! **Snowflake-specific test details (load-bearing).**
//! - The loader's `format_target` emits an UNQUOTED `db.schema.table` ref, so
//!   Snowflake folds the schema and table names to UPPER. Every reference in
//!   this test (schema create/drop, the load target, and the verification
//!   queries) therefore stays unquoted too, so they resolve the same folded
//!   objects the loader's CREATE TABLE produced. The data *columns*, by
//!   contrast, the loader creates double-quoted lowercase (`"id"`, `"name"`,
//!   …) — so verification queries reference columns double-quoted.
//! - The staging schema uses the `rocky_load_create_test_*` prefix with a
//!   microsecond suffix for per-run isolation, and is dropped CASCADE before
//!   assertions so a failed assertion still leaves the sandbox clean.

use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_adapter_sdk::{LoadOptions, LoadSource, LoaderAdapter, TableRef as SdkTableRef};
use rocky_core::config::RetryConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};
use rocky_snowflake::loader::SnowflakeLoaderAdapter;

/// Build a connector from env. Returns `None` (and the test self-skips) when
/// the required vars are absent.
fn connector_from_env() -> Option<(SnowflakeConnector, String)> {
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
    Some((SnowflakeConnector::new(config, auth), database))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn create_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    // Unquoted, matching `format_target`: Snowflake folds the schema name to
    // UPPER, so the loader's later unquoted CREATE TABLE resolves the same
    // schema object.
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {database}.{schema}"))
        .await
        .expect("create schema");
}

async fn drop_schema(adapter: &SnowflakeWarehouseAdapter, database: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {database}.{schema} CASCADE"
        ))
        .await;
}

/// Read the warehouse data type for `column` of `table` from
/// `information_schema.columns`. Column/table identifiers were created
/// unquoted-table / quoted-lowercase-columns by the loader, so they live
/// upper-cased (table) / lowercase (columns) — match that here.
async fn column_type(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
    column: &str,
) -> Option<String> {
    // `format_target` leaves schema + table unquoted, so Snowflake folds both
    // to UPPER. information_schema stores names case-sensitively as resolved,
    // so the SCHEMA / TABLE_NAME literals must be uppercased to match. The data
    // columns were created double-quoted lowercase, so COLUMN_NAME stays
    // lowercase.
    let result = adapter
        .execute_query(&format!(
            "SELECT DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = '{schema_upper}' \
             AND TABLE_NAME = '{table_upper}' \
             AND COLUMN_NAME = '{column}'",
            schema_upper = schema.to_uppercase(),
            table_upper = table.to_uppercase(),
        ))
        .await
        .ok()?;
    result.rows.first()?.first()?.as_str().map(str::to_string)
}

async fn count_rows(
    adapter: &SnowflakeWarehouseAdapter,
    database: &str,
    schema: &str,
    table: &str,
) -> Option<i64> {
    // Schema + table unquoted (UPPER-folded, matching the loader); the COUNT
    // alias is quoted only to fix the output column case for the reader.
    let result = adapter
        .execute_query(&format!(
            "SELECT COUNT(*) AS \"n\" FROM {database}.{schema}.{table}"
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
    // Schema + table unquoted (UPPER-folded); data columns quoted lowercase.
    let result = adapter
        .execute_query(&format!(
            "SELECT \"name\" FROM {database}.{schema}.{table} WHERE \"id\" = {id}"
        ))
        .await
        .ok()?;
    result.rows.first()?.first()?.as_str().map(str::to_string)
}

#[tokio::test]
#[ignore]
async fn live_load_local_csv_creates_typed_table_and_lands_rows() {
    let Some((connector, database)) = connector_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };

    // Share one connector: a clone (Arc-wrapped) drives the loader; the
    // other backs a warehouse adapter for setup + verification queries.
    let loader = SnowflakeLoaderAdapter::new(Arc::new(connector.clone()));
    let adapter = SnowflakeWarehouseAdapter::new(connector);

    let suffix = schema_suffix();
    let schema = format!("rocky_load_create_test_{suffix}");
    let table = "people"; // created unquoted -> folds to PEOPLE.

    create_schema(&adapter, &database, &schema).await;

    // Write a local CSV with one column per inferred type.
    let mut csv = tempfile::NamedTempFile::with_suffix(".csv").expect("temp csv");
    csv.write_all(
        b"id,name,score,active,joined_at\n\
          1,alice,1.5,true,2026-01-02 03:04:05\n\
          2,bob,2.5,false,2026-02-03 04:05:06\n",
    )
    .expect("write csv");
    csv.flush().expect("flush csv");

    let source = LoadSource::LocalFile(csv.path().to_path_buf());
    let target = SdkTableRef {
        catalog: database.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };
    let options = LoadOptions {
        create_table: true,
        ..Default::default()
    };

    let load_result = loader.load(&source, &target, &options).await;

    // Gather verification BEFORE the cleanup drop so assertions reflect the
    // post-load state; drop CASCADE regardless so the sandbox stays clean.
    let (rows, sample, ty_id, ty_name, ty_score, ty_active, ty_joined) = if load_result.is_ok() {
        (
            count_rows(&adapter, &database, &schema, table).await,
            fetch_name(&adapter, &database, &schema, table, 2).await,
            column_type(&adapter, &database, &schema, table, "id").await,
            column_type(&adapter, &database, &schema, table, "name").await,
            column_type(&adapter, &database, &schema, table, "score").await,
            column_type(&adapter, &database, &schema, table, "active").await,
            column_type(&adapter, &database, &schema, table, "joined_at").await,
        )
    } else {
        (None, None, None, None, None, None, None)
    };

    drop_schema(&adapter, &database, &schema).await;

    let result = load_result.expect("local CSV load into a fresh table should succeed");
    assert_eq!(result.rows_loaded, 2, "both CSV rows should land");

    assert_eq!(rows, Some(2), "target row count after load");
    assert_eq!(
        sample.as_deref(),
        Some("bob"),
        "sampled row content should round-trip"
    );

    // Inferred + mapped types, as reported by information_schema.
    // (DATA_TYPE is the family name — NUMBER / FLOAT / TEXT / BOOLEAN /
    // TIMESTAMP_NTZ — without precision; assert on the family.)
    assert_eq!(ty_id.as_deref(), Some("NUMBER"), "id -> NUMBER(38,0)");
    assert_eq!(ty_name.as_deref(), Some("TEXT"), "name -> VARCHAR (TEXT)");
    assert_eq!(ty_score.as_deref(), Some("FLOAT"), "score -> FLOAT");
    assert_eq!(ty_active.as_deref(), Some("BOOLEAN"), "active -> BOOLEAN");
    assert_eq!(
        ty_joined.as_deref(),
        Some("TIMESTAMP_NTZ"),
        "joined_at -> TIMESTAMP_NTZ"
    );
}
