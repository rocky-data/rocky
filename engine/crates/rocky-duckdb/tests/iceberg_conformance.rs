//! Live conformance for Iceberg tables written through DuckDB and a REST
//! catalog (`docs/adr/ADR-ICEBERG-DUCKDB.md`).
//!
//! Needs a Lakekeeper catalog with a warehouse named `demo` over an S3
//! bucket named `warehouse` (the setup in
//! `.github/workflows/iceberg-conformance.yml`). Run with:
//!
//! ```text
//! cargo test -p rocky-duckdb --features iceberg-conformance \
//!     --test iceberg_conformance -- --ignored --test-threads 1
//! ```
//!
//! Endpoints come from `ICEBERG_ENDPOINT` (default
//! `http://localhost:8181/catalog`) and `S3_ENDPOINT` (default
//! `localhost:8333`).
#![cfg(feature = "iceberg-conformance")]

use rocky_core::config::{
    DuckDbAttach, DuckDbAttachAuthorization, DuckDbAttachS3, DuckDbAttachType, S3UrlStyle,
};
use rocky_core::env_string::EnvString;
use rocky_core::redacted::RedactedString;
use rocky_core::traits::{TableReplace, WarehouseAdapter};
use rocky_duckdb::ExtensionOptions;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn attachment() -> DuckDbAttach {
    DuckDbAttach {
        alias: "lk".into(),
        attach_type: DuckDbAttachType::Iceberg,
        warehouse: EnvString::from_resolved("demo"),
        endpoint: EnvString::from_resolved(env_or(
            "ICEBERG_ENDPOINT",
            "http://localhost:8181/catalog",
        )),
        authorization: DuckDbAttachAuthorization::None,
        s3: Some(DuckDbAttachS3 {
            key_id: Some(EnvString::from_resolved("rocky-conformance")),
            secret: Some(RedactedString::new("rocky-conformance".to_string())),
            endpoint: Some(EnvString::from_resolved(env_or("S3_ENDPOINT", "localhost:8333"))),
            url_style: Some(S3UrlStyle::Path),
            use_ssl: Some(false),
            region: Some(EnvString::from_resolved("us-east-1")),
            scope: Some(EnvString::from_resolved("s3://warehouse")),
        }),
    }
}

fn adapter() -> DuckDbWarehouseAdapter {
    DuckDbWarehouseAdapter::in_memory()
        .expect("in-memory DuckDB")
        .with_attachments(&[attachment()], &ExtensionOptions::default())
        .expect("attach the Lakekeeper catalog")
}

/// A schema name no other run of this test uses.
fn fresh_schema() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("conf_{nanos}")
}

async fn rows(a: &DuckDbWarehouseAdapter, sql: &str) -> Vec<Vec<String>> {
    a.execute_query(sql)
        .await
        .expect(sql)
        .rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
                .collect()
        })
        .collect()
}

async fn columns(a: &DuckDbWarehouseAdapter, table: &str) -> Vec<(String, String)> {
    rows(a, &format!("DESCRIBE {table}"))
        .await
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect()
}

#[tokio::test]
#[ignore = "needs a live Lakekeeper catalog and S3 store"]
async fn full_refresh_overwrites_in_place_and_reconciles_columns() {
    let a = adapter();
    assert_eq!(a.dialect().table_replace("lk"), TableReplace::InPlaceOverwrite);
    assert_eq!(a.dialect().table_replace("memory"), TableReplace::Atomic);
    let schema = fresh_schema();
    let create_schema = a.dialect().create_schema_sql("lk", &schema).unwrap().unwrap();
    a.execute_statement(&create_schema).await.unwrap();
    let t = format!("lk.{schema}.t");

    // First build: a plain CREATE TABLE … AS.
    a.execute_statement(&a.dialect().create_table_as_new(
        &t,
        "SELECT 1::INTEGER AS id, 'a' AS name, 1::UTINYINT AS small",
    ))
    .await
    .unwrap();
    assert_eq!(a.list_tables("lk", &schema).await.unwrap(), vec!["t"]);

    // A re-run with the same query (UTINYINT comes back INTEGER) changes no
    // column.
    a.overwrite_in_place(&t, "SELECT 2::INTEGER AS id, 'b' AS name, 2::UTINYINT AS small")
        .await
        .unwrap()
        .expect("supported");
    assert_eq!(rows(&a, &format!("SELECT id, name FROM {t}")).await, vec![vec!["2", "b"]]);
    assert_eq!(
        columns(&a, &t).await,
        vec![
            ("id".into(), "INTEGER".into()),
            ("name".into(), "VARCHAR".into()),
            ("small".into(), "INTEGER".into())
        ]
    );

    // Add a nested column, drop one, widen one.
    a.overwrite_in_place(&t, "SELECT 3::BIGINT AS id, [1, 2] AS tags, 'c' AS name")
        .await
        .unwrap()
        .expect("supported");
    assert_eq!(
        columns(&a, &t).await,
        vec![
            ("id".into(), "BIGINT".into()),
            ("name".into(), "VARCHAR".into()),
            ("tags".into(), "INTEGER[]".into())
        ]
    );
    assert_eq!(rows(&a, &format!("SELECT id, name FROM {t}")).await, vec![vec!["3", "c"]]);
}

#[tokio::test]
#[ignore = "needs a live Lakekeeper catalog and S3 store"]
async fn a_failed_overwrite_keeps_the_old_table_and_rows() {
    let a = adapter();
    let schema = fresh_schema();
    a.execute_statement(&format!("CREATE SCHEMA lk.{schema}"))
        .await
        .unwrap();
    let t = format!("lk.{schema}.t");
    a.execute_statement(&format!("CREATE TABLE {t} AS SELECT 1::INTEGER AS id"))
        .await
        .unwrap();

    // The INSERT fails at run time (a value out of INTEGER range), after
    // the ADD COLUMN and DELETE ran inside the transaction.
    let err = a
        .overwrite_in_place(&t, "SELECT 5000000000::BIGINT::INTEGER AS id, 'x' AS extra")
        .await
        .expect_err("the insert overflows");
    assert!(err.to_string().contains("out of range"), "{err}");
    assert_eq!(rows(&a, &format!("SELECT * FROM {t}")).await, vec![vec!["1"]]);
    assert_eq!(columns(&a, &t).await, vec![("id".into(), "INTEGER".into())]);

    // A narrowing is refused before any write.
    a.overwrite_in_place(&t, "SELECT 1::BIGINT AS id")
        .await
        .unwrap()
        .expect("widen");
    let err = a
        .overwrite_in_place(&t, "SELECT 1::INTEGER AS id")
        .await
        .expect_err("narrowing");
    assert!(err.to_string().contains("BIGINT to INTEGER"), "{err}");
}

#[tokio::test]
#[ignore = "needs a live Lakekeeper catalog and S3 store"]
async fn dropping_a_partition_column_is_refused_and_rolled_back() {
    let a = adapter();
    let schema = fresh_schema();
    a.execute_statement(&format!("CREATE SCHEMA lk.{schema}"))
        .await
        .unwrap();
    let t = format!("lk.{schema}.p");
    a.execute_statement(&format!(
        "CREATE TABLE {t} (id INTEGER, d DATE) PARTITIONED BY (d)"
    ))
    .await
    .unwrap();
    a.execute_statement(&format!("INSERT INTO {t} VALUES (1, DATE '2026-01-01')"))
        .await
        .unwrap();
    let err = a
        .overwrite_in_place(&t, "SELECT 2::INTEGER AS id")
        .await
        .expect_err("the partition column cannot be dropped");
    assert!(err.to_string().contains("partition"), "{err}");
    assert_eq!(rows(&a, &format!("SELECT id FROM {t}")).await, vec![vec!["1"]]);
}
