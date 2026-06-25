//! Live Databricks smoke test for format-aware first-run initial DDL.
//!
//! A transformation model declaring a managed-Iceberg `format` must
//! materialize `USING ICEBERG` (plus its declared `format_options`) on the
//! *first* run, not fall back to the warehouse default. This mirrors the
//! runtime bootstrap path: when the target table is missing, the runner calls
//! `rocky_core::sql_gen::generate_transformation_initial_ddl`, which routes
//! through the dialect-aware lakehouse DDL generator. Subsequent runs find the
//! target present and take the normal incremental `INSERT INTO` path, so the
//! format DDL never re-runs.
//!
//! `#[ignore]`-gated because it requires a real Databricks workspace. Gated on
//! the `ROCKY_TEST_DATABRICKS_*` sandbox env vars, mirroring the live-test
//! shape at `rocky-databricks/tests/integration.rs`. Run with:
//!
//! ```bash
//! ROCKY_TEST_DATABRICKS_HOST=<your-host> \
//! ROCKY_TEST_DATABRICKS_HTTP_PATH=<your-http-path> \
//! ROCKY_TEST_DATABRICKS_TOKEN=<your-token> \
//! ROCKY_TEST_DATABRICKS_CATALOG=hcv2_<your-catalog> \
//! cargo test -p rocky-databricks --test lakehouse_initial_ddl_live -- --ignored --nocapture
//! ```
//!
//! Any schema/table this test creates uses the `hcv2_` prefix and is dropped on
//! completion, per the sandbox housekeeping convention. No workspace
//! identifiers are hardcoded — everything comes from env.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::sql_gen;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_ir::{
    GovernanceConfig, LakehouseFormat, LakehouseOptions, MaterializationStrategy, ModelIr,
    SourceRef, TableRef, TargetRef,
};

/// Build an adapter + target catalog from the `ROCKY_TEST_DATABRICKS_*` sandbox
/// env vars. Returns `None` (test skips) when the host/http-path aren't set.
fn adapter_from_env() -> Option<(DatabricksWarehouseAdapter, String)> {
    let host = std::env::var("ROCKY_TEST_DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("ROCKY_TEST_DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
    let catalog = std::env::var("ROCKY_TEST_DATABRICKS_CATALOG").ok()?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("ROCKY_TEST_DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("ROCKY_TEST_DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        host,
        warehouse_id,
        timeout: Duration::from_secs(180),
        retry: Default::default(),
    };
    let connector = DatabricksConnector::new(config, auth);
    Some((DatabricksWarehouseAdapter::new(connector), catalog))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn create_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
        ))
        .await
        .expect("create schema");
}

async fn drop_schema(adapter: &DatabricksWarehouseAdapter, catalog: &str, schema: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE"
        ))
        .await;
}

/// Seed a small source table with `n` rows the model selects from.
async fn seed_source(
    adapter: &DatabricksWarehouseAdapter,
    catalog: &str,
    schema: &str,
    table: &str,
    n: u64,
) {
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{table}` AS \
             SELECT id AS id, \
                    CAST(id % 3 AS STRING) AS region, \
                    TIMESTAMP '2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, id) AS updated_at \
             FROM range(0, {n})"
        ))
        .await
        .expect("seed source table");
}

/// Single-column scalar query → first cell as i64.
async fn scalar_i64(adapter: &DatabricksWarehouseAdapter, sql: &str) -> i64 {
    let result = adapter
        .connector()
        .execute_sql(sql)
        .await
        .expect("scalar query");
    let row = result.rows.first().expect("at least one row");
    let cell = row.first().expect("at least one column");
    cell.as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| cell.as_i64())
        .expect("cell parses as i64")
}

/// Read a managed table's storage format from `information_schema.tables`.
/// `SHOW CREATE TABLE` is not supported on managed Iceberg, so the format is
/// confirmed via the catalog's information schema (e.g. `"ICEBERG"`/`"DELTA"`).
async fn table_format(
    adapter: &DatabricksWarehouseAdapter,
    catalog: &str,
    schema: &str,
    table: &str,
) -> String {
    let result = adapter
        .connector()
        .execute_sql(&format!(
            "SELECT data_source_format FROM `{catalog}`.information_schema.tables \
             WHERE table_schema = '{schema}' AND table_name = '{table}'"
        ))
        .await
        .expect("query information_schema.tables");
    result
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string()
}

/// Build an Incremental model declaring managed-Iceberg format with
/// representative `format_options` (partitioning + table properties; Iceberg
/// disallows clustering alongside partitioning). The SELECT mirrors the seeded
/// source columns.
fn incremental_iceberg_model(
    catalog: &str,
    src_schema: &str,
    tgt_schema: &str,
    table: &str,
) -> ModelIr {
    let select = format!("SELECT id, region, updated_at FROM `{catalog}`.`{src_schema}`.`{table}`");
    ModelIr::transformation(
        TargetRef {
            catalog: catalog.to_string(),
            schema: tgt_schema.to_string(),
            table: table.to_string(),
        },
        MaterializationStrategy::Incremental {
            timestamp_column: "updated_at".into(),
        },
        vec![SourceRef {
            catalog: catalog.to_string(),
            schema: src_schema.to_string(),
            table: table.to_string(),
        }],
        select,
        GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        Some(LakehouseFormat::IcebergTable),
        Some(LakehouseOptions {
            partition_by: vec!["region".into()],
            // Databricks rejects PARTITIONED BY + CLUSTER BY together on Iceberg
            // (SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED) — they are
            // mutually exclusive, so this fixture exercises partitioning only.
            cluster_by: vec![],
            // Managed Iceberg rejects engine-managed write properties like
            // write.format.default (MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED), so
            // this fixture asserts the format + partitioning, not raw TBLPROPERTIES.
            table_properties: vec![],
            comment: Some("incremental iceberg mart".into()),
        }),
    )
}

/// First run of an incremental Iceberg model must materialize `USING ICEBERG`
/// with its declared `format_options`, and load the source exactly once (no
/// double-load from a populate-then-INSERT). A second run must leave the
/// declared format intact and append through the normal incremental path.
#[tokio::test]
#[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
async fn live_incremental_iceberg_initial_ddl_uses_iceberg_format() {
    let Some((adapter, catalog)) = adapter_from_env() else {
        eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
        return;
    };
    let suffix = schema_suffix();
    let src_schema = format!("hcv2_lh_src_{suffix}");
    let tgt_schema = format!("hcv2_lh_tgt_{suffix}");
    let table = "fct_events";
    let dialect = DatabricksSqlDialect;

    create_schema(&adapter, &catalog, &src_schema).await;
    create_schema(&adapter, &catalog, &tgt_schema).await;
    let source_rows: i64 = 50;
    seed_source(&adapter, &catalog, &src_schema, table, source_rows as u64).await;

    let model = incremental_iceberg_model(&catalog, &src_schema, &tgt_schema, table);

    // --- First run: target is missing → format-aware initial DDL. ---
    let target_ref = TableRef {
        catalog: catalog.clone(),
        schema: tgt_schema.clone(),
        table: table.into(),
    };
    assert!(
        adapter.describe_table(&target_ref).await.is_err(),
        "target should not exist before the first run"
    );

    let initial_ddls = sql_gen::generate_transformation_initial_ddl(&model, &dialect)
        .expect("initial DDL generation");
    assert_eq!(
        initial_ddls.len(),
        1,
        "iceberg initial DDL is a single CTAS: {initial_ddls:?}"
    );
    for ddl in &initial_ddls {
        adapter
            .execute_statement(ddl)
            .await
            .expect("execute initial DDL");
    }

    // The declared format landed on the physical table. (The partitioned
    // `USING ICEBERG` CTAS executing without error already proves partitioning
    // was accepted; managed Iceberg has no SHOW CREATE TABLE, so confirm the
    // format via information_schema.)
    let fmt = table_format(&adapter, &catalog, &tgt_schema, table).await;
    assert!(
        fmt.eq_ignore_ascii_case("ICEBERG"),
        "first-run create must materialize as Iceberg (not the default format); \
         information_schema reports data_source_format = {fmt:?}"
    );

    // The CTAS *is* the first load — the source is materialized exactly once.
    // A double-load (populate + subsequent INSERT) would report 2x rows.
    let after_first = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM `{catalog}`.`{tgt_schema}`.`{table}`"),
    )
    .await;
    assert_eq!(
        after_first, source_rows,
        "first run must load the source exactly once (no double-load)"
    );

    // --- Second run: target exists → normal incremental append, format intact. ---
    assert!(
        adapter.describe_table(&target_ref).await.is_ok(),
        "target should exist after the first run"
    );
    let exec_stmts =
        sql_gen::generate_transformation_sql(&model, &dialect).expect("incremental exec SQL");
    for stmt in &exec_stmts {
        adapter
            .execute_statement(stmt)
            .await
            .expect("execute incremental append");
    }

    let fmt_after = table_format(&adapter, &catalog, &tgt_schema, table).await;
    assert!(
        fmt_after.eq_ignore_ascii_case("ICEBERG"),
        "second run must not change the declared format; \
         information_schema reports data_source_format = {fmt_after:?}"
    );

    // This model's SQL has no watermark filter, so the second run appends the
    // full source again — proving the table is reused (not recreated) and the
    // append path runs on top of it.
    let after_second = scalar_i64(
        &adapter,
        &format!("SELECT COUNT(*) FROM `{catalog}`.`{tgt_schema}`.`{table}`"),
    )
    .await;
    assert_eq!(
        after_second,
        source_rows * 2,
        "second run appends onto the existing table without recreating it"
    );

    drop_schema(&adapter, &catalog, &src_schema).await;
    drop_schema(&adapter, &catalog, &tgt_schema).await;
}
