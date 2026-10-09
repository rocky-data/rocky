//! Live conformance harness for the Spark adapter.
//!
//! Gated behind the `spark-conformance` feature so the default
//! `cargo test -p rocky-spark` stays network-free. It needs a Spark Connect
//! server with Delta Lake at
//! `${SPARK_CONNECT_HOST:-localhost}:${SPARK_CONNECT_PORT:-15002}`:
//!
//! ```bash
//! docker run -d --rm --name rocky-spark-conformance -p 15002:15002 \
//!   -e SPARK_NO_DAEMONIZE=1 apache/spark:4.0.1 \
//!   /opt/spark/sbin/start-connect-server.sh \
//!   --packages io.delta:delta-spark_2.13:4.0.0 \
//!   --conf spark.jars.ivy=/tmp/.ivy2 \
//!   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
//!   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
//!   --conf spark.connect.grpc.binding.address=0.0.0.0
//! # wait for "Spark Connect server started" in `docker logs`
//! cargo test -p rocky-spark --features spark-conformance -- --ignored
//! ```
//!
//! Every statement below is rendered by [`SparkDialect`], so the test proves
//! the SQL Rocky generates runs, not hand-written SQL. Each test uses its own
//! schema, so the tests run in parallel and re-run against a long-lived
//! server.

#![cfg(feature = "spark-conformance")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::{CaseSignificance, ObjectKind, WarehouseAdapter};
use rocky_ir::{ColumnSelection, TableRef};
use rocky_spark::{SparkConfig, SparkWarehouseAdapter, TableFormat};

const CATALOG: &str = "spark_catalog";

fn adapter() -> SparkWarehouseAdapter {
    let host = std::env::var("SPARK_CONNECT_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("SPARK_CONNECT_PORT").unwrap_or_else(|_| "15002".into());
    let cfg = SparkConfig::new(
        Some(&format!("{host}:{port}")),
        None,
        Some("rocky-conformance"),
        Duration::from_secs(300),
    )
    .expect("config");
    SparkWarehouseAdapter::new(cfg, TableFormat::Delta).expect("adapter")
}

/// A fresh schema, created, unique per call.
async fn schema(a: &SparkWarehouseAdapter, tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let name = format!("rocky_{tag}_{nanos}");
    let sql = a
        .dialect()
        .create_schema_sql(CATALOG, &name)
        .expect("spark renders CREATE SCHEMA")
        .expect("valid schema name");
    a.execute_statement(&sql).await.expect("create schema");
    name
}

fn table(schema: &str, name: &str) -> TableRef {
    TableRef {
        catalog: CATALOG.into(),
        schema: schema.into(),
        table: name.into(),
    }
}

fn target(a: &SparkWarehouseAdapter, t: &TableRef) -> String {
    a.dialect()
        .format_table_ref(&t.catalog, &t.schema, &t.table)
        .expect("valid ref")
}

async fn rows(a: &SparkWarehouseAdapter, sql: &str) -> Vec<Vec<String>> {
    a.execute_query(sql)
        .await
        .expect("query")
        .rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| {
                    v.as_str()
                        .map_or_else(|| "NULL".to_string(), str::to_string)
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn literal_escape_round_trips() {
    let a = adapter();
    let value = "it's a \\ back\\slash 'quoted' \\' end";
    let lit = rocky_core::sql_gen::string_literal(a.dialect(), value);
    let got = rows(&a, &format!("SELECT {lit} AS v")).await;
    assert_eq!(got, vec![vec![value.to_string()]]);
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn full_refresh_view_describe_kind_and_list() {
    let a = adapter();
    let s = schema(&a, "fr").await;
    let t = table(&s, "orders");
    let v = table(&s, "orders_v");
    let d = a.dialect();

    // Twice: a full refresh must be re-runnable.
    for amount in ["1.50", "2.50"] {
        a.execute_statement(&d.create_table_as(
            &target(&a, &t),
            &format!(
                "SELECT 1 AS id, CAST({amount} AS DECIMAL(10,2)) AS amount, \
                 TIMESTAMP '2024-05-01 10:00:00.123456' AS ts, 'x' AS s"
            ),
        ))
        .await
        .expect("CTAS");
    }
    assert_eq!(
        rows(&a, &format!("SELECT amount FROM {}", target(&a, &t))).await,
        vec![vec!["2.50".to_string()]]
    );
    a.execute_statement(
        &d.view_ddl(
            &target(&a, &v),
            &format!("SELECT id FROM {}", target(&a, &t)),
        )
        .unwrap(),
    )
    .await
    .expect("view");

    let cols = a.describe_table(&t).await.expect("describe");
    let shape: Vec<_> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();
    assert_eq!(
        shape,
        [
            ("id", "int"),
            ("amount", "decimal(10,2)"),
            ("ts", "timestamp"),
            ("s", "string")
        ]
    );
    assert_eq!(a.object_kind(&t).await.unwrap(), ObjectKind::Table);
    assert_eq!(a.object_kind(&v).await.unwrap(), ObjectKind::View);
    assert_eq!(
        a.object_kind(&table(&s, "absent")).await.unwrap(),
        ObjectKind::Unknown
    );
    let mut listed = a.list_tables(CATALOG, &s).await.expect("list");
    listed.sort();
    assert_eq!(listed, ["orders", "orders_v"]);

    // The session default makes Rocky's plain CTAS a Delta table.
    let detail = rows(&a, &format!("DESCRIBE DETAIL {}", target(&a, &t))).await;
    assert_eq!(detail[0][0], "delta");

    // A timestamp reads back in the RFC 3339 shape Rocky's watermark parser
    // reads.
    let ts = rows(&a, &format!("SELECT MAX(ts) FROM {}", target(&a, &t))).await;
    let parsed: chrono::DateTime<chrono::Utc> = ts[0][0].parse().expect("rfc3339");
    assert_eq!(parsed.timestamp_micros() % 1_000_000, 123_456);
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn missing_objects_are_typed() {
    let a = adapter();
    let s = schema(&a, "miss").await;
    let err = a
        .describe_table(&table(&s, "absent"))
        .await
        .expect_err("absent table");
    assert!(a.is_missing_object_error(&err), "{err}");
    let err = a
        .execute_query("SELECT * FROM spark_catalog.rocky_no_such_schema_x.t")
        .await
        .expect_err("absent schema");
    assert!(a.is_missing_object_error(&err), "{err}");
    let err = a.execute_query("SELEC 1").await.expect_err("syntax");
    assert!(!a.is_missing_object_error(&err), "{err}");
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn incremental_append_with_watermark_and_merge() {
    let a = adapter();
    let s = schema(&a, "inc").await;
    let t = table(&s, "events");
    let d = a.dialect();
    let tgt = target(&a, &t);

    a.execute_statement(&d.create_table_as(
        &tgt,
        "SELECT 1 AS id, 'a' AS status, TIMESTAMP '2024-01-01 00:00:00' AS ts",
    ))
    .await
    .unwrap();

    let since = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let filter = d.watermark_where("ts", Some(&since)).unwrap();
    let incoming = format!(
        "SELECT * FROM (SELECT 2 AS id, 'b' AS status, TIMESTAMP '2024-01-02 00:00:00' AS ts \
         UNION ALL SELECT 9, 'old', TIMESTAMP '2023-12-31 00:00:00') AS src {filter}"
    );
    a.execute_statement(&d.insert_into(&tgt, &incoming))
        .await
        .unwrap();
    assert_eq!(
        rows(&a, &format!("SELECT id FROM {tgt} ORDER BY id")).await,
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );

    let merge = d
        .merge_into(
            &tgt,
            "SELECT 2 AS id, 'B' AS status, TIMESTAMP '2024-01-03 00:00:00' AS ts \
             UNION ALL SELECT 3, 'c', TIMESTAMP '2024-01-03 00:00:00'",
            &[Arc::from("id")],
            &ColumnSelection::All,
        )
        .unwrap();
    let stats = a.execute_statement_with_stats(&merge).await.unwrap();
    assert_eq!(stats.rows_affected, Some(2));
    let explicit = d
        .merge_into(
            &tgt,
            "SELECT 1 AS id, 'A' AS status, TIMESTAMP '2030-01-01 00:00:00' AS ts",
            &[Arc::from("id")],
            &ColumnSelection::Explicit(vec![Arc::from("status")]),
        )
        .unwrap();
    a.execute_statement(&explicit).await.unwrap();
    assert_eq!(
        rows(
            &a,
            &format!("SELECT id, status, CAST(ts AS DATE) FROM {tgt} ORDER BY id")
        )
        .await,
        vec![
            vec!["1".to_string(), "A".to_string(), "2024-01-01".to_string()],
            vec!["2".to_string(), "B".to_string(), "2024-01-03".to_string()],
            vec!["3".to_string(), "c".to_string(), "2024-01-03".to_string()],
        ]
    );
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn time_interval_replace_where_and_delete_insert() {
    let a = adapter();
    let s = schema(&a, "ti").await;
    let t = table(&s, "daily");
    let d = a.dialect();
    let tgt = target(&a, &t);

    a.execute_statement(&d.create_table_as(
        &tgt,
        "SELECT DATE '2024-01-01' AS day, 1 AS n UNION ALL SELECT DATE '2024-01-02', 2",
    ))
    .await
    .unwrap();
    let window = "day >= DATE '2024-01-02' AND day < DATE '2024-01-03'";
    for stmt in d
        .insert_overwrite_partition(&tgt, window, "SELECT DATE '2024-01-02' AS day, 20 AS n")
        .unwrap()
    {
        a.execute_statement(&stmt).await.unwrap();
    }
    assert_eq!(
        rows(&a, &format!("SELECT day, n FROM {tgt} ORDER BY day")).await,
        vec![
            vec!["2024-01-01".to_string(), "1".to_string()],
            vec!["2024-01-02".to_string(), "20".to_string()],
        ]
    );

    let delete = d.delete_partitions_sql(
        &tgt,
        &[Arc::from("day")],
        "SELECT DATE '2024-01-01' AS day, 10 AS n",
    );
    for stmt in d.delete_insert_statements(
        delete,
        d.insert_into(&tgt, "SELECT DATE '2024-01-01' AS day, 10 AS n"),
    ) {
        a.execute_statement(&stmt).await.unwrap();
    }
    assert_eq!(
        rows(&a, &format!("SELECT n FROM {tgt} ORDER BY day")).await,
        vec![vec!["10".to_string()], vec!["20".to_string()]]
    );
}

#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn arrow_fetch_case_branch_clone_and_checksums() {
    let a = adapter();
    let s = schema(&a, "misc").await;
    let t = table(&s, "src");
    let d = a.dialect();
    let tgt = target(&a, &t);
    a.execute_statement(&d.create_table_as(
        &tgt,
        "SELECT id, CAST(id * 10 AS BIGINT) AS v FROM range(1, 101)",
    ))
    .await
    .unwrap();

    let batch = a
        .fetch_arrow_batch(&format!("SELECT * FROM {tgt}"))
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 100);

    assert_eq!(
        a.identifier_case_significance().await.unwrap(),
        CaseSignificance::Insignificant
    );

    let branch = schema(&a, "branch").await;
    a.clone_table_for_branch(&t, &branch).await.unwrap();
    assert_eq!(
        rows(&a, &format!("SELECT COUNT(*) FROM {CATALOG}.{branch}.src")).await,
        vec![vec!["100".to_string()]]
    );

    let ranges: Vec<_> = (0..4)
        .map(|i| rocky_core::traits::PkRange::IntRange {
            lo: 1 + i * 25,
            hi: 1 + (i + 1) * 25,
        })
        .collect();
    let chunks = a
        .checksum_chunks(&t, "id", &["v".to_string()], &ranges)
        .await
        .unwrap();
    assert_eq!(chunks.len(), 4);
    assert!(chunks.iter().all(|c| c.row_count == 25));

    let plan = a.explain(&format!("SELECT * FROM {tgt}")).await.unwrap();
    assert!(
        plan.raw_explain.contains("Optimized Logical Plan"),
        "{}",
        plan.raw_explain
    );
}

const SNAPSHOT_SOURCE: [&str; 3] = [
    "SELECT 1 AS id, 'alice' AS name, TIMESTAMP '2024-01-01 00:00:00' AS updated_at \
     UNION ALL SELECT 2, 'bob', TIMESTAMP '2024-01-01 00:00:00' \
     UNION ALL SELECT 3, 'carol', TIMESTAMP '2024-01-01 00:00:00'",
    // id 1 updated, id 3 deleted at the source.
    "SELECT 1 AS id, 'alice2' AS name, TIMESTAMP '2024-02-01 00:00:00' AS updated_at \
     UNION ALL SELECT 2, 'bob', TIMESTAMP '2024-01-01 00:00:00'",
    // id 3 comes back.
    "SELECT 1 AS id, 'alice2' AS name, TIMESTAMP '2024-02-01 00:00:00' AS updated_at \
     UNION ALL SELECT 2, 'bob', TIMESTAMP '2024-01-01 00:00:00' \
     UNION ALL SELECT 3, 'carol', TIMESTAMP '2024-03-01 00:00:00'",
];

/// Replace `tgt` with the rows of `select_sql`.
async fn set_source(a: &SparkWarehouseAdapter, tgt: &str, select_sql: &str) {
    a.execute_statement(&format!("DROP TABLE IF EXISTS {tgt}"))
        .await
        .unwrap();
    a.execute_statement(&a.dialect().create_table_as(tgt, select_sql))
        .await
        .unwrap();
}

/// `id:name:is_current` of every version, oldest first per key.
async fn history(a: &SparkWarehouseAdapter, tgt: &str) -> Vec<String> {
    rows(
        a,
        &format!("SELECT id, name, is_current FROM {tgt} ORDER BY id, valid_from, valid_to"),
    )
    .await
    .into_iter()
    .map(|r| r.join(":"))
    .collect()
}

/// A snapshot model with hard deletes runs on Delta: the close step is a
/// `MERGE … WHEN NOT MATCHED BY SOURCE`, not the `UPDATE … WHERE NOT EXISTS`
/// that open-source Delta refuses (`DELTA_UNSUPPORTED_SUBQUERY`).
#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn snapshot_model_hard_deletes_invalidate_and_new_record() {
    use rocky_core::snapshot_model::{
        generate_snapshot_bootstrap_select, generate_snapshot_model_sql,
    };
    use rocky_ir::{
        SnapshotChangeStrategy, SnapshotHardDeletes, SnapshotMetaColumns, SnapshotSpec,
    };

    let a = adapter();
    let d = a.dialect();
    let s = schema(&a, "snapm").await;
    let cols: Vec<String> = ["id", "name", "updated_at"].map(String::from).to_vec();

    for (mode, hard_deletes) in [
        ("inv", SnapshotHardDeletes::Invalidate),
        ("rec", SnapshotHardDeletes::NewRecord),
    ] {
        let new_record = hard_deletes == SnapshotHardDeletes::NewRecord;
        let src = target(&a, &table(&s, &format!("src_{mode}")));
        let tgt = target(&a, &table(&s, &format!("snap_{mode}")));
        let spec = SnapshotSpec {
            unique_key: vec![Arc::from("id")],
            change: SnapshotChangeStrategy::Timestamp {
                updated_at: Arc::from("updated_at"),
            },
            hard_deletes,
            meta_columns: SnapshotMetaColumns::default(),
            valid_to_current: None,
        };
        let model_sql = format!("SELECT id, name, updated_at FROM {src}");

        set_source(&a, &src, SNAPSHOT_SOURCE[0]).await;
        let boot =
            generate_snapshot_bootstrap_select(&spec, &model_sql, d, chrono::Utc::now()).unwrap();
        a.execute_statement(&d.create_table_as(&tgt, &boot))
            .await
            .unwrap();
        assert_eq!(
            history(&a, &tgt).await,
            ["1:alice:true", "2:bob:true", "3:carol:true"],
            "{mode} initial load"
        );

        // A source update (id 1) and a source delete (id 3).
        set_source(&a, &src, SNAPSHOT_SOURCE[1]).await;
        for stmt in
            generate_snapshot_model_sql(&spec, &tgt, &model_sql, d, &cols, chrono::Utc::now())
                .unwrap()
        {
            a.execute_statement(&stmt).await.unwrap();
        }
        let expected: &[&str] = if new_record {
            &[
                "1:alice:false",
                "1:alice2:true",
                "2:bob:true",
                "3:carol:false",
                "3:carol:true",
            ]
        } else {
            &[
                "1:alice:false",
                "1:alice2:true",
                "2:bob:true",
                "3:carol:false",
            ]
        };
        assert_eq!(
            history(&a, &tgt).await,
            expected,
            "{mode} after update and delete"
        );

        // A rerun over the same source writes nothing.
        for stmt in
            generate_snapshot_model_sql(&spec, &tgt, &model_sql, d, &cols, chrono::Utc::now())
                .unwrap()
        {
            a.execute_statement(&stmt).await.unwrap();
        }
        assert_eq!(history(&a, &tgt).await, expected, "{mode} idempotent rerun");

        // The deleted key comes back and is current again.
        set_source(&a, &src, SNAPSHOT_SOURCE[2]).await;
        for stmt in
            generate_snapshot_model_sql(&spec, &tgt, &model_sql, d, &cols, chrono::Utc::now())
                .unwrap()
        {
            a.execute_statement(&stmt).await.unwrap();
        }
        let current = rows(
            &a,
            &format!("SELECT id FROM {tgt} WHERE is_current = TRUE ORDER BY id"),
        )
        .await;
        let ids: Vec<&str> = current.iter().map(|r| r[0].as_str()).collect();
        assert_eq!(ids, ["1", "2", "3"], "{mode} current keys after re-insert");
        let versions_of_3 = history(&a, &tgt)
            .await
            .iter()
            .filter(|r| r.starts_with("3:"))
            .count();
        assert_eq!(
            versions_of_3,
            if new_record { 3 } else { 2 },
            "{mode} versions of the re-inserted key"
        );
    }
}

/// The `snapshot` pipeline generator with `invalidate_hard_deletes`.
#[tokio::test]
#[ignore = "requires a live Spark Connect server at SPARK_CONNECT_HOST:SPARK_CONNECT_PORT (default localhost:15002); run with `--ignored`"]
async fn snapshot_pipeline_invalidates_hard_deletes() {
    use rocky_core::snapshots::{
        SnapshotConfig, SnapshotStrategy, generate_initial_load_sql, generate_snapshot_sql,
    };
    use rocky_ir::{SourceRef, TargetRef};

    let a = adapter();
    let d = a.dialect();
    let s = schema(&a, "snapp").await;
    let src_t = table(&s, "src");
    let tgt_t = table(&s, "hist");
    let (src, tgt) = (target(&a, &src_t), target(&a, &tgt_t));
    let cfg = SnapshotConfig {
        source: SourceRef {
            catalog: src_t.catalog.clone(),
            schema: src_t.schema.clone(),
            table: src_t.table.clone(),
        },
        target: TargetRef {
            catalog: tgt_t.catalog.clone(),
            schema: tgt_t.schema.clone(),
            table: tgt_t.table.clone(),
        },
        unique_key: vec!["id".into()],
        strategy: SnapshotStrategy::Timestamp {
            updated_at: "updated_at".into(),
        },
        invalidate_hard_deletes: true,
    };
    let cols: Vec<String> = ["id", "name", "updated_at"].map(String::from).to_vec();

    set_source(&a, &src, SNAPSHOT_SOURCE[0]).await;
    a.execute_statement(&generate_initial_load_sql(&cfg, d).unwrap())
        .await
        .unwrap();
    for stmt in generate_snapshot_sql(&cfg, d, &cols).unwrap() {
        a.execute_statement(&stmt).await.unwrap();
    }
    assert_eq!(
        history(&a, &tgt).await,
        ["1:alice:true", "2:bob:true", "3:carol:true"]
    );

    set_source(&a, &src, SNAPSHOT_SOURCE[1]).await;
    for stmt in generate_snapshot_sql(&cfg, d, &cols).unwrap() {
        a.execute_statement(&stmt).await.unwrap();
    }
    assert_eq!(
        history(&a, &tgt).await,
        [
            "1:alice:false",
            "1:alice2:true",
            "2:bob:true",
            "3:carol:false"
        ]
    );
}
