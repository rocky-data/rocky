#![cfg(feature = "duckdb")]

//! Snapshot-model SQL per warehouse dialect.
//!
//! DuckDB runs end to end in `rocky/tests/snapshot_model_run.rs`; this file
//! pins the statements the other dialects receive, built by the real dialect
//! implementations (quoting, metadata-column case, hash expression, UPDATE
//! alias form).

use chrono::{TimeZone, Utc};
use rocky_bigquery::dialect::BigQueryDialect;
use rocky_core::snapshot_model::{
    generate_snapshot_bootstrap_select, generate_snapshot_model_sql, snapshot_source_columns,
};
use rocky_core::traits::SqlDialect;
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_ir::{
    SnapshotChangeStrategy, SnapshotCheckColumns, SnapshotHardDeletes, SnapshotMetaColumns,
    SnapshotSpec,
};
use rocky_snowflake::dialect::SnowflakeSqlDialect;

fn spec(change: SnapshotChangeStrategy, hard_deletes: SnapshotHardDeletes) -> SnapshotSpec {
    SnapshotSpec {
        unique_key: vec!["id".into(), "region".into()],
        change,
        hard_deletes,
        meta_columns: SnapshotMetaColumns::default(),
        valid_to_current: None,
    }
}

fn timestamp() -> SnapshotChangeStrategy {
    SnapshotChangeStrategy::Timestamp {
        updated_at: "updated_at".into(),
    }
}

fn statements(dialect: &dyn SqlDialect, spec: &SnapshotSpec, columns: &[&str]) -> Vec<String> {
    // BigQuery validates the catalog as a GCP project id.
    let catalog = if dialect.name() == "bigquery" {
        "my-project"
    } else {
        "cat"
    };
    let target = dialect.format_table_ref(catalog, "sch", "snap").unwrap();
    let columns: Vec<String> = columns.iter().map(|c| (*c).to_string()).collect();
    generate_snapshot_model_sql(
        spec,
        &target,
        "SELECT id, region, name, updated_at FROM raw.customers",
        dialect,
        &columns,
        Utc.with_ymd_and_hms(2026, 10, 4, 12, 0, 0).unwrap(),
    )
    .unwrap()
}

#[test]
fn databricks_uses_backticks_and_aliased_update() {
    let stmts = statements(
        &DatabricksSqlDialect,
        &spec(timestamp(), SnapshotHardDeletes::Invalidate),
        &["id", "region", "name", "updated_at"],
    );
    assert_eq!(stmts.len(), 3);
    assert!(
        stmts[0].starts_with("MERGE INTO cat.sch.snap AS target"),
        "{}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("ON target.`id` = source.`id` AND target.`region` = source.`region` AND target.`is_current` = TRUE"),
        "{}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("source.`updated_at` > target.`updated_at`"),
        "{}",
        stmts[0]
    );
    assert!(
        stmts[1].contains("md5(cast(coalesce(cast(source.`id` as STRING)"),
        "{}",
        stmts[1]
    );
    assert!(
        stmts[2].starts_with("UPDATE cat.sch.snap AS target SET `valid_to` = CAST('2026-10-04 12:00:00.000000' AS TIMESTAMP), `is_current` = FALSE"),
        "{}",
        stmts[2]
    );
}

#[test]
fn snowflake_folds_metadata_columns_to_upper_case() {
    // Snowflake returns unquoted columns upper-cased from DESCRIBE.
    let described: Vec<String> = [
        "ID",
        "REGION",
        "NAME",
        "UPDATED_AT",
        "VALID_FROM",
        "VALID_TO",
        "IS_CURRENT",
        "SNAPSHOT_ID",
    ]
    .iter()
    .map(|c| (*c).to_string())
    .collect();
    let s = spec(
        SnapshotChangeStrategy::Check {
            check_cols: SnapshotCheckColumns::All,
            updated_at: None,
        },
        SnapshotHardDeletes::NewRecord,
    );
    let source = snapshot_source_columns(&s, &described).unwrap();
    assert_eq!(source, vec!["ID", "REGION", "NAME", "UPDATED_AT"]);
    let source: Vec<&str> = source.iter().map(String::as_str).collect();
    let stmts = statements(&SnowflakeSqlDialect, &s, &source);
    assert_eq!(stmts.len(), 4);
    assert!(
        stmts[0].contains("target.\"IS_CURRENT\" = TRUE"),
        "{}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("source.\"NAME\" IS DISTINCT FROM target.\"NAME\" OR source.\"UPDATED_AT\" IS DISTINCT FROM target.\"UPDATED_AT\""),
        "check = all compares every non-key column: {}",
        stmts[0]
    );
    assert!(
        stmts[0].contains("COALESCE(target.\"IS_DELETED\", FALSE) = TRUE"),
        "{}",
        stmts[0]
    );
    assert!(stmts[2].contains("\"IS_DELETED\")"), "{}", stmts[2]);

    let bootstrap = generate_snapshot_bootstrap_select(
        &s,
        "SELECT 1 AS id",
        &SnowflakeSqlDialect,
        Utc.with_ymd_and_hms(2026, 10, 4, 12, 0, 0).unwrap(),
    )
    .unwrap();
    assert!(bootstrap.contains("TRUE AS \"IS_CURRENT\""), "{bootstrap}");
    assert!(bootstrap.contains("FALSE AS \"IS_DELETED\""), "{bootstrap}");
}

#[test]
fn bigquery_quotes_with_backticks_and_hashes_with_to_hex() {
    let stmts = statements(
        &BigQueryDialect,
        &spec(timestamp(), SnapshotHardDeletes::Ignore),
        &["id", "region", "name", "updated_at"],
    );
    assert_eq!(stmts.len(), 2);
    assert!(
        stmts[0].contains(
            "UPDATE SET `valid_to` = CAST(source.`updated_at` AS TIMESTAMP), `is_current` = FALSE"
        ),
        "{}",
        stmts[0]
    );
    assert!(stmts[1].contains("to_hex(md5("), "{}", stmts[1]);
    assert!(
        stmts[1].contains(
            "IS NOT NULL AND NOT EXISTS (SELECT 1 FROM `my-project`.`sch`.`snap` AS existing"
        ),
        "{}",
        stmts[1]
    );
}

#[test]
fn duckdb_statement_shapes_match_the_e2e_path() {
    let stmts = statements(
        &DuckDbSqlDialect,
        &spec(timestamp(), SnapshotHardDeletes::NewRecord),
        &["id", "region", "name", "updated_at"],
    );
    let kinds: Vec<&str> = stmts
        .iter()
        .map(|s| s.split_whitespace().next().unwrap_or(""))
        .collect();
    assert_eq!(kinds, vec!["MERGE", "INSERT", "INSERT", "UPDATE"]);
}

/// Postgres under `merge_mode = "on_conflict"` and Redshift cannot run the
/// SCD2 MERGE. Every snapshot-model entry point refuses before it emits any
/// statement: the bootstrap CTAS, the `is_deleted` ALTER, the steady-state
/// statements and the plan preview.
#[test]
fn snapshot_models_refuse_dialects_without_snapshot_support() {
    use rocky_core::snapshot_model::{
        ExistingMarkers, add_is_deleted_column_sql, generate_snapshot_model_sql_with,
        preview_snapshot_model_sql,
    };
    let on_conflict =
        rocky_postgres::PostgresDialect::with_merge_mode(rocky_postgres::MergeMode::OnConflict);
    let redshift = rocky_postgres::RedshiftDialect::with_late_binding_views(false);
    let now = Utc.with_ymd_and_hms(2026, 10, 4, 12, 0, 0).unwrap();
    let s = spec(timestamp(), SnapshotHardDeletes::NewRecord);
    let cols: Vec<String> = ["id", "region", "name", "updated_at"]
        .iter()
        .map(|c| (*c).to_string())
        .collect();
    let model = "SELECT id, region, name, updated_at FROM raw.customers";
    for dialect in [&on_conflict as &dyn SqlDialect, &redshift] {
        let errors = [
            generate_snapshot_bootstrap_select(&s, model, dialect, now).unwrap_err(),
            add_is_deleted_column_sql(&s, "cat.sch.snap", dialect).unwrap_err(),
            generate_snapshot_model_sql_with(
                &s,
                "cat.sch.snap",
                model,
                dialect,
                &cols,
                now,
                ExistingMarkers::FromMode,
            )
            .unwrap_err(),
            preview_snapshot_model_sql(&s, "cat.sch.snap", model, dialect, &cols, now).unwrap_err(),
        ];
        for err in errors {
            let msg = err.to_string();
            assert!(
                msg.contains("snapshot models cannot run on"),
                "{}: {msg}",
                dialect.name()
            );
        }
    }
    // PostgreSQL 15+ (`merge_mode = "merge"`, the default) still runs them.
    let pg = rocky_postgres::PostgresDialect::with_merge_mode(rocky_postgres::MergeMode::Merge);
    assert!(generate_snapshot_bootstrap_select(&s, model, &pg, now).is_ok());
}
