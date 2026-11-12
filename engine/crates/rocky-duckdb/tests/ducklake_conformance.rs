//! Live conformance for a DuckLake catalog attached to DuckDB
//! (`docs/adr/ADR-DUCKLAKE.md`). It runs the SQL Rocky's DuckDB dialect
//! renders against a real lake: a local DuckDB-file catalog in a temporary
//! directory, no Docker.
//!
//! The tests download the `ducklake` extension (about 55 MB), so they are
//! `#[ignore]` behind the `ducklake-conformance` feature:
//!
//! ```text
//! cargo test -p rocky-duckdb --features ducklake-conformance \
//!   --test ducklake_conformance -- --ignored --test-threads 1
//! ```
//!
//! `--test-threads 1` matters: one test changes the working directory.
#![cfg(feature = "ducklake-conformance")]

use std::path::Path;

use rocky_core::config::{DuckDbAttach, DuckDbDuckLakeAttach};
use rocky_core::env_string::EnvString;
use rocky_core::traits::{CatalogKind, TableReplace, WarehouseAdapter};
use rocky_duckdb::ExtensionOptions;
use rocky_duckdb::adapter::{DuckDbWarehouseAdapter, classify_duckdb_failure};
use rocky_ir::ColumnSelection;

fn lake(metadata: &Path, data: Option<&str>) -> DuckDbAttach {
    DuckDbAttach::DuckLake(DuckDbDuckLakeAttach {
        alias: "dl".into(),
        metadata_path: EnvString::from_resolved(metadata.to_string_lossy()),
        data_path: data.map(EnvString::from_resolved),
        s3: None,
    })
}

fn adapter(dir: &Path, data: Option<&str>) -> DuckDbWarehouseAdapter {
    let opts = ExtensionOptions::default();
    DuckDbWarehouseAdapter::open_with(&dir.join("main.duckdb"), &opts)
        .expect("open DuckDB")
        .with_attachments(&[lake(&dir.join("meta.ducklake"), data)], &opts)
        .expect("attach the lake")
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

async fn scalar(a: &DuckDbWarehouseAdapter, sql: &str) -> String {
    rows(a, sql).await.remove(0).remove(0)
}

fn parquet_files(dir: &Path) -> usize {
    fn walk(dir: &Path, n: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, n);
            } else if p.extension().is_some_and(|x| x == "parquet") {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

async fn setup_schema(a: &DuckDbWarehouseAdapter) {
    let sql = a.dialect().create_schema_sql("dl", "s").unwrap().unwrap();
    assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS dl.s");
    a.execute_statement(&sql).await.unwrap();
}

/// A full refresh is Rocky's `CREATE OR REPLACE TABLE`: one atomic
/// snapshot that may change the columns, sees the old rows when it reads its
/// own target, and keeps the old table when it fails.
#[tokio::test]
#[ignore = "downloads the ducklake extension"]
async fn full_refresh_replaces_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let data = format!("{}/data/", dir.path().display());
    let a = adapter(dir.path(), Some(&data));
    let d = a.dialect();
    assert_eq!(d.catalog_kind("dl"), CatalogKind::DuckLake);
    assert_eq!(d.table_replace("dl"), TableReplace::Atomic);
    setup_schema(&a).await;
    let t = d.format_table_ref("dl", "s", "t").unwrap();

    a.execute_statement(&d.create_table_as(
        &t,
        "SELECT range::INTEGER AS id, 'a' AS name FROM range(100)",
    ))
    .await
    .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {t}")).await,
        "100"
    );
    assert!(
        parquet_files(Path::new(&data)) > 0,
        "Parquet files under {data}"
    );
    assert_eq!(a.list_tables("dl", "s").await.unwrap(), vec!["t"]);

    // New columns, and a self-read that sees the old rows.
    a.execute_statement(&d.create_table_as(
        &t,
        &format!("SELECT id::BIGINT AS id, 'b' AS tag FROM {t} UNION ALL SELECT 1000, 'c'"),
    ))
    .await
    .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {t}")).await,
        "101"
    );
    assert_eq!(
        rows(
            &a,
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_catalog = 'dl' AND table_name = 't' ORDER BY ordinal_position"
        )
        .await,
        vec![vec!["id", "BIGINT"], vec!["tag", "VARCHAR"]]
    );

    // A replace that fails at run time keeps the old table.
    let err = a
        .execute_statement(&d.create_table_as(
            &t,
            "SELECT 5000000000::BIGINT::INTEGER AS id FROM range(20)",
        ))
        .await
        .expect_err("overflow");
    assert!(err.to_string().contains("out of range"), "{err}");
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {t}")).await,
        "101"
    );
}

/// Rocky's merge, partition overwrite and drift SQL run on a lake table.
#[tokio::test]
#[ignore = "downloads the ducklake extension"]
async fn merge_partitions_and_drift_run_on_the_lake() {
    let dir = tempfile::tempdir().unwrap();
    let a = adapter(dir.path(), Some(&format!("{}/data/", dir.path().display())));
    let d = a.dialect();
    setup_schema(&a).await;
    let t = d.format_table_ref("dl", "s", "m").unwrap();
    a.execute_statement(&d.create_table_as_new(
        &t,
        "SELECT range::INTEGER AS id, 'old' AS v, range::DECIMAL(10,2) AS amt, \
         DATE '2026-01-01' + (range % 3)::INTEGER AS day FROM range(30)",
    ))
    .await
    .unwrap();

    // Merge on a unique key.
    let merge = d
        .merge_into(
            &t,
            "SELECT range::INTEGER AS id, 'new' AS v, 0::DECIMAL(10,2) AS amt, \
             DATE '2026-01-05' AS day FROM range(25, 40)",
            &["id".into()],
            &ColumnSelection::Explicit(vec!["v".into(), "amt".into(), "day".into()]),
        )
        .unwrap();
    a.execute_statement(&merge).await.unwrap();
    assert_eq!(scalar(&a, &format!("SELECT count(*) FROM {t}")).await, "40");
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {t} WHERE v = 'new'")).await,
        "15"
    );

    // A time_interval partition overwrite.
    let stmts = d
        .insert_overwrite_partition(
            &t,
            "day = DATE '2026-01-01'",
            "SELECT 999::INTEGER AS id, 'part' AS v, 1::DECIMAL(10,2) AS amt, \
             DATE '2026-01-01' AS day",
        )
        .unwrap();
    for s in &stmts {
        a.execute_statement(s).await.unwrap();
    }
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT count(*) FROM {t} WHERE day = DATE '2026-01-01'")
        )
        .await,
        "1"
    );

    // Drift: a widening and an added column.
    for sql in [
        d.alter_column_type_sql(&t, "id", "BIGINT").unwrap(),
        d.alter_column_type_sql(&t, "amt", "DECIMAL(18,2)").unwrap(),
        d.add_column_sql(&t, "region", "VARCHAR"),
    ] {
        a.execute_statement(&sql).await.expect(&sql);
    }
    // A narrowing is refused by DuckLake itself.
    let narrow = d.alter_column_type_sql(&t, "id", "INTEGER").unwrap();
    assert!(a.execute_statement(&narrow).await.is_err());
}

/// One transaction may not write the lake and the main database. The error
/// is permanent: a retry would fail the same way.
#[tokio::test]
#[ignore = "downloads the ducklake extension"]
async fn a_transaction_across_the_lake_and_main_is_permanent() {
    let dir = tempfile::tempdir().unwrap();
    let a = adapter(dir.path(), None);
    setup_schema(&a).await;
    a.execute_statement("CREATE TABLE main.local AS SELECT 1 AS id")
        .await
        .unwrap();
    a.execute_statement("CREATE TABLE dl.s.lake AS SELECT 1 AS id")
        .await
        .unwrap();
    a.execute_statement("BEGIN TRANSACTION").await.unwrap();
    a.execute_statement("INSERT INTO main.local VALUES (2)")
        .await
        .unwrap();
    let err = a
        .execute_statement("INSERT INTO dl.s.lake VALUES (2)")
        .await
        .expect_err("two databases in one transaction");
    let _ = a.execute_statement("ROLLBACK").await;
    assert!(
        !classify_duckdb_failure(&err).is_retryable(),
        "must not retry: {err}"
    );
}

/// A relative `data_path` is stored absolute, so a later attach from another
/// working directory finds the same files. A lake made elsewhere with a
/// relative stored path is refused.
#[tokio::test]
#[ignore = "downloads the ducklake extension"]
async fn relative_paths_are_stored_absolute() {
    let dir = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    // Restores the working directory even when an assertion fails.
    struct Cwd(std::path::PathBuf);
    impl Drop for Cwd {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }
    let _restore = Cwd(std::env::current_dir().unwrap());

    std::env::set_current_dir(dir.path()).unwrap();
    let opts = ExtensionOptions::default();
    let rel = DuckDbAttach::DuckLake(DuckDbDuckLakeAttach {
        alias: "dl".into(),
        metadata_path: EnvString::from_resolved("meta.ducklake"),
        data_path: Some(EnvString::from_resolved("data/")),
        s3: None,
    });
    let first = DuckDbWarehouseAdapter::in_memory()
        .unwrap()
        .with_attachments(std::slice::from_ref(&rel), &opts);
    {
        let a = first.expect("attach");
        setup_schema(&a).await;
        a.execute_statement("CREATE TABLE dl.s.t AS SELECT range AS id FROM range(50)")
            .await
            .unwrap();
        let stored = scalar(
            &a,
            "SELECT value FROM __ducklake_metadata_dl.main.ducklake_metadata \
             WHERE key = 'data_path'",
        )
        .await;
        assert!(Path::new(&stored).is_absolute(), "stored: {stored}");
        drop(a);

        // Another working directory: the absolute metadata path finds the
        // same lake and the same files.
        std::env::set_current_dir(elsewhere.path()).unwrap();
        let abs = lake(&dir.path().join("meta.ducklake"), None);
        let b = DuckDbWarehouseAdapter::in_memory()
            .unwrap()
            .with_attachments(&[abs], &opts)
            .expect("re-attach");
        assert_eq!(scalar(&b, "SELECT count(*) FROM dl.s.t").await, "50");
        assert!(parquet_files(&dir.path().join("data")) > 0);
        assert_eq!(parquet_files(elsewhere.path()), 0);
    }

    // A lake created outside Rocky with a relative DATA_PATH.
    let foreign = tempfile::tempdir().unwrap();
    let meta = foreign.path().join("foreign.ducklake");
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "LOAD ducklake; ATTACH 'ducklake:{}' AS f (DATA_PATH 'rel/'); DETACH f",
            meta.display()
        ))
        .unwrap();
    }
    let err = DuckDbWarehouseAdapter::in_memory()
        .unwrap()
        .with_attachments(&[lake(&meta, None)], &opts)
        .err()
        .expect("a relative stored data path is refused");
    assert!(err.to_string().contains("relative data path"), "{err}");
}

/// Discovery never reads a lake schema as a source.
#[tokio::test]
#[ignore = "downloads the ducklake extension"]
async fn discovery_skips_the_lake() {
    use rocky_core::traits::DiscoveryAdapter;
    let dir = tempfile::tempdir().unwrap();
    let a = adapter(dir.path(), None);
    a.execute_statement("CREATE SCHEMA dl.src__lake")
        .await
        .unwrap();
    a.execute_statement("CREATE TABLE dl.src__lake.t AS SELECT 1 AS id")
        .await
        .unwrap();
    a.execute_statement("CREATE SCHEMA src__own").await.unwrap();
    a.execute_statement("CREATE TABLE src__own.t AS SELECT 1 AS id")
        .await
        .unwrap();
    let discovery = rocky_duckdb::discovery::DuckDbDiscoveryAdapter::new(a.shared_connector());
    let found = discovery.discover("src__").await.unwrap();
    let schemas: Vec<&str> = found.connectors.iter().map(|c| c.schema.as_str()).collect();
    assert_eq!(schemas, ["src__own"]);
}
