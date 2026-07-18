//! Live Databricks conformance test for the safe-type-widening drift path.
//!
//! `#[ignore]`-gated because it requires a real Databricks workspace on
//! Databricks Runtime 15.4 LTS or above (the floor for the Delta
//! `delta.enableTypeWidening` table feature). Reads the SAME env vars as
//! the other live tests:
//!   DATABRICKS_HOST, DATABRICKS_HTTP_PATH,
//!   DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET),
//!   DATABRICKS_TEST_CATALOG (or DATABRICKS_CATALOG_PREFIX)
//!
//! Every schema/table is `hc_`-prefixed and dropped CASCADE on exit.
//!
//! Run with:
//!   cargo test -p rocky-databricks --test drift_widening_live -- --ignored --nocapture
//!
//! What this pins (mirrors `rocky-snowflake/tests/drift_widening_live.rs`):
//!
//! 1. Delta type widening actually accepts the widenings the scoped
//!    `DatabricksSqlDialect::is_safe_type_widening` allowlist advertises, when
//!    `delta.enableTypeWidening` is set — so `detect_drift` returning
//!    `AlterColumnTypes` does not ship an `ALTER` Databricks rejects (#1115 /
//!    #1157).
//! 2. The `SET TBLPROPERTIES('delta.enableTypeWidening' = 'true')` +
//!    `ALTER COLUMN … TYPE …` SQL Rocky emits is accepted, both at CREATE
//!    time and on a pre-existing table created without the feature.
//! 3. Existing rows survive the in-place widening (no data loss).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_ir::TableRef;

fn adapter_from_env() -> Option<DatabricksWarehouseAdapter> {
    let host = std::env::var("DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        host,
        warehouse_id,
        timeout: Duration::from_secs(180),
        retry: Default::default(),
    };
    Some(DatabricksWarehouseAdapter::new(DatabricksConnector::new(
        config, auth,
    )))
}

fn catalog_from_env() -> Option<String> {
    std::env::var("DATABRICKS_TEST_CATALOG")
        .or_else(|_| std::env::var("DATABRICKS_CATALOG_PREFIX"))
        .ok()
}

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn exec(adapter: &DatabricksWarehouseAdapter, sql: &str) {
    adapter
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {sql}\nerror: {e:?}"));
}

fn col_type(cols: &[rocky_ir::ColumnInfo], name: &str) -> String {
    cols.iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .map(|c| c.data_type.clone())
        .unwrap_or_default()
}

/// CAPABILITY PROBE — raw SQL only, no dependency on the scoped allowlist.
///
/// Confirms the Delta feature + ALTER forms Rocky needs actually work on the
/// target warehouse before the allowlist re-classifies anything as ALTER-safe.
/// Prints the canonical DESCRIBE type spellings so the allowlist tuples can be
/// pinned to what the warehouse actually reports.
#[tokio::test]
#[ignore = "requires live Databricks workspace on DBR 15.4 LTS+"]
async fn live_delta_type_widening_capability_probe() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("SKIP: Databricks env not set");
        return;
    };
    let Some(catalog) = catalog_from_env() else {
        eprintln!("SKIP: DATABRICKS_TEST_CATALOG / DATABRICKS_CATALOG_PREFIX not set");
        return;
    };

    // CONNECTIVITY PROOF first.
    let probe = adapter.execute_query("SELECT 1 AS n").await;
    let dialect = DatabricksSqlDialect;
    let schema = format!("hc_drift_widen_{}", suffix());

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"),
    )
    .await;

    // ---- Probe A: enableTypeWidening at CREATE, then widen each type ----
    let a = "hc_widen_create";
    exec(
        &adapter,
        &format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.{a} \
             (n INT, f FLOAT, d DECIMAL(10,2), i2 INT, s SMALLINT) \
             TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"
        ),
    )
    .await;
    exec(
        &adapter,
        &format!(
            "INSERT INTO {catalog}.{schema}.{a} VALUES \
             (2147483647, CAST(1.5 AS FLOAT), CAST(123.45 AS DECIMAL(10,2)), 100, CAST(7 AS SMALLINT))"
        ),
    )
    .await;

    let a_ref = dialect
        .format_table_ref(&catalog, &schema, a)
        .expect("format_table_ref a");
    // Each widening via the dialect emitter (current ANSI `ALTER COLUMN … TYPE`).
    for (col, ty) in [
        ("n", "BIGINT"),
        ("f", "DOUBLE"),
        ("d", "DECIMAL(20,2)"),
        ("i2", "DOUBLE"),
        ("s", "INT"),
    ] {
        let sql = dialect
            .alter_column_type_sql(&a_ref, col, ty)
            .unwrap_or_else(|e| panic!("alter_column_type_sql {col}->{ty}: {e:?}"));
        adapter
            .execute_statement(&sql)
            .await
            .unwrap_or_else(|e| panic!("WIDEN FAILED {col}->{ty}\nsql: {sql}\nerr: {e:?}"));
    }

    let a_table = TableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: a.to_string(),
    };
    let a_desc = adapter.describe_table(&a_table).await.expect("describe a");
    let a_rows = adapter
        .execute_query(&format!(
            "SELECT n, f, d, i2, s FROM {catalog}.{schema}.{a}"
        ))
        .await
        .ok();

    // ---- Probe B: pre-existing table (no feature), SET then widen ----
    let b = "hc_widen_set";
    exec(
        &adapter,
        &format!("CREATE OR REPLACE TABLE {catalog}.{schema}.{b} (n INT)"),
    )
    .await;
    exec(
        &adapter,
        &format!("INSERT INTO {catalog}.{schema}.{b} VALUES (5)"),
    )
    .await;
    let b_ref = dialect
        .format_table_ref(&catalog, &schema, b)
        .expect("format_table_ref b");
    let set_sql =
        format!("ALTER TABLE {b_ref} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')");
    let set_res = adapter.execute_statement(&set_sql).await;
    let widen_b = dialect
        .alter_column_type_sql(&b_ref, "n", "BIGINT")
        .unwrap();
    let widen_b_res = if set_res.is_ok() {
        Some(adapter.execute_statement(&widen_b).await)
    } else {
        None
    };
    let b_table = TableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: b.to_string(),
    };
    let b_desc = adapter.describe_table(&b_table).await.ok();

    // Cleanup before assertions so a failure still leaves the sandbox clean.
    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    // ---- Report ----
    println!("\n=== #1157 Databricks Delta type-widening capability probe ===");
    let p = probe.expect("CONNECTIVITY: SELECT 1 must succeed");
    println!("CONNECTIVITY: SELECT 1 -> {:?}", p.rows);
    println!("PROBE A canonical DESCRIBE spellings (post-widen):");
    for c in &["n", "f", "d", "i2", "s"] {
        println!("   {c:>3} -> {}", col_type(&a_desc, c));
    }
    println!("PROBE A rows preserved: {a_rows:?}");
    println!("PROBE B SET TBLPROPERTIES on existing table -> {set_res:?}");
    println!("PROBE B widen result -> {widen_b_res:?}");
    if let Some(bd) = &b_desc {
        println!("PROBE B n -> {}", col_type(bd, "n"));
    }

    // ---- Assertions: every widening Rocky needs must have taken ----
    assert_eq!(
        col_type(&a_desc, "n").to_lowercase(),
        "bigint",
        "INT->BIGINT"
    );
    assert_eq!(
        col_type(&a_desc, "f").to_lowercase(),
        "double",
        "FLOAT->DOUBLE"
    );
    assert_eq!(
        col_type(&a_desc, "d").to_lowercase(),
        "decimal(20,2)",
        "DECIMAL(10,2)->DECIMAL(20,2)"
    );
    assert_eq!(
        col_type(&a_desc, "i2").to_lowercase(),
        "double",
        "INT->DOUBLE"
    );
    assert_eq!(
        col_type(&a_desc, "s").to_lowercase(),
        "int",
        "SMALLINT->INT"
    );

    set_res.expect("SET TBLPROPERTIES enableTypeWidening on existing table must succeed");
    widen_b_res
        .expect("widen attempted")
        .expect("ALTER COLUMN n TYPE BIGINT must succeed after SET TBLPROPERTIES");
    assert_eq!(
        col_type(b_desc.as_deref().unwrap_or(&[]), "n").to_lowercase(),
        "bigint",
        "pre-existing table widened after SET TBLPROPERTIES"
    );

    println!("VERDICT: Delta type widening CONFIRMED for the scoped allowlist set.");
}

/// END-TO-END — the SQL Rocky actually emits, driven by the real
/// `detect_drift` + `generate_alter_column_sql` + `DatabricksSqlDialect`.
///
/// Creates a table WITHOUT `enableTypeWidening` (so the `pre_alter` prelude is
/// exercised), drives every allowlist widening through `detect_drift`, executes
/// the emitted batch, and asserts the columns widened and the seeded row
/// survived. This is the receipt that `AlterColumnTypes` no longer ships an
/// `ALTER` Databricks rejects (#1115 / #1157).
#[tokio::test]
#[ignore = "requires live Databricks workspace on DBR 15.4 LTS+"]
async fn live_drift_safe_widening_alters_in_place() {
    use rocky_core::drift::{detect_drift, generate_alter_column_sql};
    use rocky_ir::{ColumnInfo, DriftAction};

    let Some(adapter) = adapter_from_env() else {
        eprintln!("SKIP: Databricks env not set");
        return;
    };
    let Some(catalog) = catalog_from_env() else {
        eprintln!("SKIP: DATABRICKS_TEST_CATALOG / DATABRICKS_CATALOG_PREFIX not set");
        return;
    };

    let dialect = DatabricksSqlDialect;
    let schema = format!("hc_drift_e2e_{}", suffix());
    let table = "hc_widen_e2e";

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"),
    )
    .await;
    // Deliberately created WITHOUT the type-widening feature — the emitted
    // batch's prelude must turn it on.
    exec(
        &adapter,
        &format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.{table} \
             (id INT, amt FLOAT, price DECIMAL(10,2), qty SMALLINT, big INT)"
        ),
    )
    .await;
    exec(
        &adapter,
        &format!(
            "INSERT INTO {catalog}.{schema}.{table} VALUES \
             (2147483647, CAST(2.5 AS FLOAT), CAST(999.99 AS DECIMAL(10,2)), CAST(12 AS SMALLINT), 42)"
        ),
    )
    .await;

    let table_ref = TableRef {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };
    let current = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe current");

    // Desired (new) types — every one an allowlist widening.
    let desired = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: true,
        },
        ColumnInfo {
            name: "amt".into(),
            data_type: "DOUBLE".into(),
            nullable: true,
        },
        ColumnInfo {
            name: "price".into(),
            data_type: "DECIMAL(20,2)".into(),
            nullable: true,
        },
        ColumnInfo {
            name: "qty".into(),
            data_type: "INT".into(),
            nullable: true,
        },
        ColumnInfo {
            name: "big".into(),
            data_type: "DOUBLE".into(),
            nullable: true,
        },
    ];

    let drift = detect_drift(&table_ref, &desired, &current, &dialect);
    let stmts = generate_alter_column_sql(&table_ref, &drift.drifted_columns, &dialect)
        .expect("generate_alter_column_sql");
    // Execute the emitted batch exactly as the run path would.
    let mut exec_err = None;
    for s in &stmts {
        if let Err(e) = adapter.execute_statement(s).await {
            exec_err = Some(format!("stmt failed: {s}\nerr: {e:?}"));
            break;
        }
    }

    let widened = adapter
        .describe_table(&table_ref)
        .await
        .expect("describe after");
    let row = adapter
        .execute_query(&format!(
            "SELECT id, amt, price, qty, big FROM {catalog}.{schema}.{table}"
        ))
        .await
        .ok();

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;

    println!("\n=== #1157 Databricks end-to-end drift widening ===");
    println!("detect_drift action = {:?}", drift.action);
    println!("emitted batch ({} stmts):", stmts.len());
    for s in &stmts {
        println!("   {s}");
    }
    println!("post-widen: {widened:?}");
    println!("row preserved: {row:?}");

    assert_eq!(
        drift.action,
        DriftAction::AlterColumnTypes,
        "every drifted column is a Delta lossless widening"
    );
    assert!(
        stmts
            .first()
            .map(|s| s.contains("enableTypeWidening"))
            .unwrap_or(false),
        "batch must enable the type-widening feature first"
    );
    if let Some(e) = exec_err {
        panic!("emitted widening batch failed: {e}");
    }
    assert_eq!(col_type(&widened, "id").to_lowercase(), "bigint");
    assert_eq!(col_type(&widened, "amt").to_lowercase(), "double");
    assert_eq!(col_type(&widened, "price").to_lowercase(), "decimal(20,2)");
    assert_eq!(col_type(&widened, "qty").to_lowercase(), "int");
    assert_eq!(col_type(&widened, "big").to_lowercase(), "double");

    let row = row.expect("row queryable");
    assert_eq!(row.rows.len(), 1, "the seeded row survives the widening");

    println!("VERDICT: detect_drift → AlterColumnTypes; emitted batch accepted; rows preserved.");
}
