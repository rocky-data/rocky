//! Live tests against a real SQL Server.
//!
//! Skipped (each test returns early) unless `ROCKY_SQLSERVER_TEST_HOST` is
//! set. Connection settings:
//!
//! | Env var | Default |
//! |---|---|
//! | `ROCKY_SQLSERVER_TEST_HOST` | — (required; `host` or `host,port`) |
//! | `ROCKY_SQLSERVER_TEST_DB` | `rocky_test` (must exist) |
//! | `ROCKY_SQLSERVER_TEST_USER` | `sa` |
//! | `ROCKY_SQLSERVER_TEST_PASSWORD` | — |
//! | `ROCKY_SQLSERVER_TEST_TRUST_CERT` | `true` (a local container's self-signed certificate) |
//!
//! A local server: `docker run -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=... -p
//! 1433:1433 mcr.microsoft.com/mssql/server:2022-latest`, then `CREATE
//! DATABASE rocky_test`.
//!
//! Each test works in its own schema (`rocky_live_<name>`), emptied first,
//! so tests run in parallel and re-run cleanly. They drive the adapter and
//! the generators `rocky run` uses (`rocky_core::sql_gen`,
//! `rocky_core::drift`): the SQL each strategy emits is executed for real
//! and its effect read back.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rocky_core::traits::{ObjectKind, WarehouseAdapter};
use rocky_ir::{
    ColumnSelection, GovernanceConfig, MaterializationStrategy, ModelIr, TableRef, TargetRef,
};
use rocky_sqlserver::{Credentials, SqlServerConfig, SqlServerWarehouseAdapter};

fn config() -> Option<SqlServerConfig> {
    let host = std::env::var("ROCKY_SQLSERVER_TEST_HOST").ok()?;
    let db = std::env::var("ROCKY_SQLSERVER_TEST_DB").unwrap_or_else(|_| "rocky_test".into());
    let user = std::env::var("ROCKY_SQLSERVER_TEST_USER").unwrap_or_else(|_| "sa".into());
    let password = std::env::var("ROCKY_SQLSERVER_TEST_PASSWORD").ok();
    let trust = std::env::var("ROCKY_SQLSERVER_TEST_TRUST_CERT").unwrap_or_else(|_| "true".into());
    let mut extra = BTreeMap::new();
    extra.insert("trust_server_certificate".into(), serde_json::json!(trust));
    let creds = Credentials {
        username: Some(&user),
        password: password.as_deref(),
        ..Credentials::default()
    };
    Some(
        SqlServerConfig::new(
            Some(&host),
            Some(&db),
            &creds,
            Duration::from_secs(60),
            &extra,
        )
        .expect("valid test config"),
    )
}

async fn setup(schema: &str) -> Option<SqlServerWarehouseAdapter> {
    let adapter = SqlServerWarehouseAdapter::new(config()?).expect("adapter");
    // Drop every view and table in the schema, then (re)create it.
    adapter
        .execute_statement(&format!(
            "DECLARE @sql NVARCHAR(MAX) = N''; \
             SELECT @sql += N'DROP ' + CASE o.type WHEN 'V' THEN N'VIEW ' ELSE N'TABLE ' END \
               + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N'; ' \
             FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id \
             WHERE s.name = N'{schema}' AND o.type IN ('U', 'V') ORDER BY o.type DESC; \
             EXEC(@sql); \
             IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');"
        ))
        .await
        .expect("reset schema");
    Some(adapter)
}

fn tref(schema: &str, table: &str) -> TableRef {
    TableRef {
        catalog: String::new(),
        schema: schema.into(),
        table: table.into(),
    }
}

async fn scalar(adapter: &SqlServerWarehouseAdapter, sql: &str) -> Option<String> {
    let r = adapter.execute_query(sql).await.expect("query");
    r.rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_str().map(str::to_string))
}

async fn count(adapter: &SqlServerWarehouseAdapter, table: &str) -> String {
    scalar(adapter, &format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
}

fn model(schema: &str, table: &str, strategy: MaterializationStrategy, sql: &str) -> ModelIr {
    ModelIr::transformation(
        TargetRef {
            catalog: String::new(),
            schema: schema.into(),
            table: table.into(),
        },
        strategy,
        vec![],
        sql.into(),
        GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        None,
        None,
    )
}

#[tokio::test]
async fn ping_typed_cells_and_session_settings() {
    let Some(a) = setup("rocky_live_ping").await else {
        return;
    };
    a.ping().await.unwrap();
    let r = a
        .execute_query(
            "SELECT CAST(1 AS BIT) AS b, CAST(12.50 AS DECIMAL(10,2)) AS d, \
             CAST('2026-01-02 03:04:05.1234567' AS DATETIME2(7)) AS ts, \
             CAST('2026-01-02 03:04:05 +05:30' AS DATETIMEOFFSET) AS tz, \
             CAST(NULL AS INT) AS n, N'héllo' AS s, CAST(42 AS BIGINT) AS i, \
             CASE WHEN @@OPTIONS & 16384 > 0 THEN 1 ELSE 0 END AS xa, SESSIONPROPERTY('QUOTED_IDENTIFIER') AS qi",
        )
        .await
        .unwrap();
    assert_eq!(
        r.columns,
        vec!["b", "d", "ts", "tz", "n", "s", "i", "xa", "qi"]
    );
    let row = &r.rows[0];
    assert_eq!(row[0], serde_json::json!("true"));
    assert_eq!(row[1], serde_json::json!("12.50"));
    assert_eq!(row[2], serde_json::json!("2026-01-02 03:04:05.123456700"));
    assert_eq!(row[3], serde_json::json!("2026-01-01T21:34:05Z"));
    assert_eq!(row[4], serde_json::Value::Null);
    assert_eq!(row[5], serde_json::json!("héllo"));
    assert_eq!(row[6], serde_json::json!("42"));
    // The session setup pinned XACT_ABORT and QUOTED_IDENTIFIER on.
    assert_eq!(row[7], serde_json::json!("1"));
    assert_eq!(row[8], serde_json::json!("1"));
}

/// The literal rule (`LiteralEscape::Standard`) proven by a round trip: a
/// value holding a quote AND a backslash reads back byte-identical.
#[tokio::test]
async fn literal_escape_round_trips() {
    let Some(a) = setup("rocky_live_lit").await else {
        return;
    };
    let value = r"it's a \path\ with 'quotes' and \\ two";
    let lit = rocky_core::sql_gen::string_literal(a.dialect(), value);
    assert_eq!(
        scalar(&a, &format!("SELECT {lit}")).await.as_deref(),
        Some(value)
    );
}

#[tokio::test]
async fn describe_table_canonical_types_and_missing_table() {
    let s = "rocky_live_desc";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[t] (id INT NOT NULL, big BIGINT, f FLOAT, r REAL, \
         amt DECIMAL(12,2), name NVARCHAR(40), notes NVARCHAR(MAX), flag BIT, \
         ts DATETIME2(3), d DATE, g UNIQUEIDENTIFIER)"
    ))
    .await
    .unwrap();
    let cols = a.describe_table(&tref(s, "t")).await.unwrap();
    let got: Vec<(&str, &str, bool)> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str(), c.nullable))
        .collect();
    assert_eq!(
        got,
        vec![
            ("id", "INT", false),
            ("big", "BIGINT", true),
            ("f", "DOUBLE PRECISION", true),
            ("r", "REAL", true),
            ("amt", "DECIMAL(12,2)", true),
            ("name", "NVARCHAR(40)", true),
            ("notes", "NVARCHAR(MAX)", true),
            ("flag", "BIT", true),
            ("ts", "DATETIME2(3)", true),
            ("d", "DATE", true),
            ("g", "UNIQUEIDENTIFIER", true),
        ]
    );
    // Every canonical name is valid DDL: drift replays them.
    let ddl = cols
        .iter()
        .map(|c| format!("[{}] {}", c.name, c.data_type))
        .collect::<Vec<_>>()
        .join(", ");
    a.execute_statement(&format!("CREATE TABLE [{s}].[t2] ({ddl})"))
        .await
        .unwrap();

    let err = a.describe_table(&tref(s, "missing")).await.unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    let err = a
        .execute_query(&format!("SELECT * FROM [{s}].[missing]"))
        .await
        .unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    assert_eq!(
        a.identifier_case_significance().await.unwrap(),
        rocky_core::traits::CaseSignificance::Insignificant
    );
}

#[tokio::test]
async fn full_refresh_swaps_atomically_and_keeps_old_table_on_failure() {
    let s = "rocky_live_full";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let target = d.format_table_ref("", s, "t").unwrap();
    // A reserved word as a column name works because every name is
    // bracketed in the generated SQL around it.
    let ir = model(
        s,
        "t",
        MaterializationStrategy::FullRefresh,
        "WITH base AS (SELECT 1 AS id UNION ALL SELECT 2)\nSELECT id, 'x' AS [order] FROM base",
    );
    for _ in 0..2 {
        for stmt in rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap() {
            a.execute_statement(&stmt).await.unwrap();
        }
    }
    assert_eq!(count(&a, &target).await, "2");
    // The staging table is gone after the swap.
    assert_eq!(
        scalar(&a, &format!("SELECT COUNT(*) FROM sys.tables WHERE name = N't__rocky_new' AND schema_id = SCHEMA_ID(N'{s}')")).await.as_deref(),
        Some("0")
    );
    // A failing body leaves the old table and its rows.
    let err = a
        .execute_statement(&d.create_table_as(&target, "SELECT 1/0 AS id"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Divide by zero"), "{err}");
    assert_eq!(count(&a, &target).await, "2");
    // The pool still works after an error (the failed connection was dropped).
    a.ping().await.unwrap();
    // A first-run create refuses to replace an existing table.
    assert!(
        a.execute_statement(&d.create_table_as_new(&target, "SELECT 1 AS id"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn incremental_model_with_cte_lookback_and_new_columns() {
    let s = "rocky_live_incr";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[src] (id INT, amount INT, ts DATETIME2(7)); \
         INSERT INTO [{s}].[src] VALUES (1, 10, '2026-01-01 00:00:00.25'), (2, 20, '2026-01-02')"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let sql = format!(
        "WITH base AS (SELECT id, amount, ts FROM [{s}].[src])\n\
         SELECT id, amount, ts FROM base WHERE @incremental_filter"
    );
    let ir = model(
        s,
        "tgt",
        MaterializationStrategy::Incremental {
            timestamp_column: "ts".into(),
            unique_key: Vec::new(),
            lookback: Some(rocky_ir::IncrementalLookback {
                amount: 1,
                unit: rocky_ir::LookbackUnit::Hour,
            }),
            filter_column: None,
        },
        &sql,
    );
    // First run: the bootstrap create (every @incremental_filter → (1 = 1)).
    for stmt in rocky_core::sql_gen::generate_transformation_initial_ddl(&ir, d).unwrap() {
        a.execute_statement(&stmt).await.unwrap();
    }
    let tgt = d.format_table_ref("", s, "tgt").unwrap();
    assert_eq!(count(&a, &tgt).await, "2");
    // A later row and a row inside the 1-hour lookback window.
    a.execute_statement(&format!(
        "INSERT INTO [{s}].[src] VALUES (3, 30, '2026-01-03'), (4, 40, '2026-01-01 23:30:00')"
    ))
    .await
    .unwrap();
    for stmt in rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap() {
        a.execute_statement(&stmt).await.unwrap();
    }
    // `ts > DATEADD(hour, -1, MAX(ts))` with MAX = Jan 2 00:00 admits both
    // new rows AND re-reads row 2 (the lookback window exists to re-read
    // late rows; an append keeps the duplicate, a `unique_key` would merge it).
    assert_eq!(count(&a, &tgt).await, "5");

    // Named-column append (the model's column order differs from the
    // target's) through the dialect hook.
    let reordered = model(
        s,
        "tgt",
        ir.materialization.clone(),
        &format!(
            "WITH base AS (SELECT id, amount, ts FROM [{s}].[src])\n\
             SELECT ts, amount, id FROM base WHERE @incremental_filter"
        ),
    );
    a.execute_statement(&format!(
        "INSERT INTO [{s}].[src] VALUES (5, 50, '2026-01-04')"
    ))
    .await
    .unwrap();
    let cols = vec!["ts".to_string(), "amount".to_string(), "id".to_string()];
    for stmt in
        rocky_core::sql_gen::generate_incremental_transformation_sql(&reordered, d, Some(&cols))
            .unwrap()
    {
        a.execute_statement(&stmt).await.unwrap();
    }
    // MAX = Jan 3: row 3 (re-read by the lookback) and row 5.
    assert_eq!(count(&a, &tgt).await, "7");

    // `ADD` (no COLUMN keyword) for a new column.
    for stmt in rocky_core::drift::generate_add_column_sql(
        &tref(s, "tgt"),
        &[rocky_ir::ColumnInfo {
            name: "region".into(),
            data_type: "NVARCHAR(20)".into(),
            nullable: true,
        }],
        d,
    )
    .unwrap()
    {
        a.execute_statement(&stmt).await.unwrap();
    }
    let after = a.describe_table(&tref(s, "tgt")).await.unwrap();
    assert_eq!(after.last().unwrap().data_type, "NVARCHAR(20)");

    // The probe SQL `rocky run` uses to read a model's output columns.
    let probe = d.wrap_select_limited(
        &format!(
            "\n{}\n",
            rocky_core::incremental_filter::unfiltered_sql_for(&sql, d)
        ),
        "_rocky_probe",
        "*",
        0,
    );
    let r = a.execute_query(&probe).await.unwrap();
    assert_eq!(r.columns, vec!["id", "amount", "ts"]);
    assert!(r.rows.is_empty());
}

#[tokio::test]
async fn replication_watermark_and_type_drift() {
    let s = "rocky_live_repl";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[src] (id INT, amount INT, _synced DATETIME2(7)); \
         INSERT INTO [{s}].[src] VALUES (1, 10, '2026-01-01 00:00:00.1234567'), (2, 20, '2026-01-02')"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let tgt = d.format_table_ref("", s, "tgt").unwrap();
    let src = d.format_table_ref("", s, "src").unwrap();
    a.execute_statement(&d.create_table_as_new(&tgt, &format!("SELECT * FROM {src}")))
        .await
        .unwrap();
    // The watermark read back from the server, then a filtered copy: the
    // 7-digit literal does not re-admit the row it was read from.
    let max = scalar(&a, &format!("SELECT MAX(_synced) FROM {src} WHERE id = 1"))
        .await
        .unwrap();
    let parsed = chrono::NaiveDateTime::parse_from_str(&max, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc();
    let select = d.select_clause(&ColumnSelection::All, &[]).unwrap();
    let wh = d.watermark_where("_synced", Some(&parsed)).unwrap();
    let r = a
        .execute_query(&format!("SELECT COUNT(*) FROM {src} {wh}"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!("1"));
    a.execute_statement(&d.insert_into(&tgt, &format!("{select} FROM {src} {wh}")))
        .await
        .unwrap();
    assert_eq!(count(&a, &tgt).await, "3");

    // INT -> BIGINT is altered in place.
    a.execute_statement(&format!("ALTER TABLE {src} ALTER COLUMN amount BIGINT"))
        .await
        .unwrap();
    let src_cols = a.describe_table(&tref(s, "src")).await.unwrap();
    let tgt_cols = a.describe_table(&tref(s, "tgt")).await.unwrap();
    let drift = rocky_core::drift::detect_drift(&tref(s, "tgt"), &src_cols, &tgt_cols, d);
    assert_eq!(
        drift.action,
        rocky_ir::DriftAction::AlterColumnTypes,
        "{drift:?}"
    );
    for stmt in
        rocky_core::drift::generate_alter_column_sql(&tref(s, "tgt"), &drift.drifted_columns, d)
            .unwrap()
    {
        a.execute_statement(&stmt).await.unwrap();
    }
    let after = a.describe_table(&tref(s, "tgt")).await.unwrap();
    assert_eq!(after[1].data_type, "BIGINT");
}

#[tokio::test]
async fn merge_upserts_with_cte_source() {
    let s = "rocky_live_merge";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let tgt = d.format_table_ref("", s, "t").unwrap();
    a.execute_statement(&format!(
        "CREATE TABLE {tgt} (id INT, name NVARCHAR(20), amount INT); \
         INSERT INTO {tgt} VALUES (1, N'a', 1), (2, N'b', 2)"
    ))
    .await
    .unwrap();
    let ir = model(
        s,
        "t",
        MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("id")],
            update_columns: ColumnSelection::Explicit(vec![
                Arc::from("id"),
                Arc::from("name"),
                Arc::from("amount"),
            ]),
        },
        "WITH incoming AS (SELECT 2 AS id, N'B' AS name, 20 AS amount UNION ALL \
         SELECT 3, N'c', 3)\nSELECT id, name, amount FROM incoming",
    );
    for _ in 0..2 {
        for stmt in rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap() {
            a.execute_statement(&stmt).await.unwrap();
        }
    }
    assert_eq!(count(&a, &tgt).await, "3");
    assert_eq!(
        scalar(&a, &format!("SELECT name FROM {tgt} WHERE id = 2"))
            .await
            .as_deref(),
        Some("B")
    );
    // Key-only merge: only the NOT MATCHED arm.
    let key_only = d
        .merge_into(
            &tgt,
            "SELECT 4 AS id UNION ALL SELECT 1",
            &[Arc::from("id")],
            &ColumnSelection::Explicit(vec![Arc::from("id")]),
        )
        .unwrap();
    a.execute_statement(&key_only).await.unwrap();
    assert_eq!(count(&a, &tgt).await, "4");
}

#[tokio::test]
async fn delete_insert_and_time_interval_are_atomic() {
    let s = "rocky_live_di";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    a.execute_statement(&format!(
        "CREATE TABLE {t} (region NVARCHAR(10), ds DATE, v INT); \
         INSERT INTO {t} VALUES (N'eu', '2026-01-01', 1), (N'us', '2026-01-01', 2), \
         (N'eu', '2026-01-02', 3)"
    ))
    .await
    .unwrap();
    let ir = model(
        s,
        "t",
        MaterializationStrategy::DeleteInsert {
            partition_by: vec![Arc::from("region"), Arc::from("ds")],
        },
        "WITH x AS (SELECT N'eu' AS region, CAST('2026-01-01' AS DATE) AS ds, 10 AS v)\n\
         SELECT region, ds, v FROM x",
    );
    let stmts = rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap();
    assert_eq!(stmts.len(), 1);
    a.execute_statement(&stmts[0]).await.unwrap();
    assert_eq!(count(&a, &t).await, "3");
    assert_eq!(
        scalar(&a, &format!("SELECT SUM(v) FROM {t}"))
            .await
            .as_deref(),
        Some("15")
    );

    // A failing INSERT rolls back its DELETE.
    let bad = d.delete_insert_statements(
        format!("DELETE FROM {t} WHERE v = 10"),
        d.insert_into(
            &t,
            "SELECT N'eu' AS region, CAST('2026-01-01' AS DATE) AS ds, 1/0 AS v",
        ),
    );
    assert!(a.execute_statement(&bad[0]).await.is_err());
    assert_eq!(count(&a, &t).await, "3");

    // time_interval overwrite: one transaction.
    let stmts = d
        .insert_overwrite_partition(
            &t,
            "ds >= '2026-01-02 00:00:00' AND ds < '2026-01-03 00:00:00'",
            "SELECT N'eu' AS region, CAST('2026-01-02' AS DATE) AS ds, 30 AS v",
        )
        .unwrap();
    a.execute_statement(&stmts[0]).await.unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT v FROM {t} WHERE ds = '2026-01-02'"))
            .await
            .as_deref(),
        Some("30")
    );
    let failing = d
        .insert_overwrite_partition(
            &t,
            "ds >= '2026-01-02 00:00:00' AND ds < '2026-01-03 00:00:00'",
            "SELECT N'eu' AS region, CAST('2026-01-02' AS DATE) AS ds, 1/0 AS v",
        )
        .unwrap();
    assert!(a.execute_statement(&failing[0]).await.is_err());
    assert_eq!(
        scalar(&a, &format!("SELECT v FROM {t} WHERE ds = '2026-01-02'"))
            .await
            .as_deref(),
        Some("30")
    );
}

#[tokio::test]
async fn views_and_kind_switch() {
    let s = "rocky_live_view";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let v = d.format_table_ref("", s, "v").unwrap();
    let ir = model(
        s,
        "v",
        MaterializationStrategy::View,
        "WITH x AS (SELECT 1 AS a)\nSELECT a FROM x",
    );
    for _ in 0..2 {
        for stmt in rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap() {
            a.execute_statement(&stmt)
                .await
                .unwrap_or_else(|e| panic!("{stmt}: {e}"));
        }
    }
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );
    assert_eq!(
        scalar(&a, &format!("SELECT a FROM {v}")).await.as_deref(),
        Some("1")
    );
    assert!(d.materialized_view_ddl(&v, "SELECT 1").is_err());

    // view -> table and back through the atomic kind switch.
    a.atomic_drop_and_create(
        &format!("DROP VIEW {v}"),
        &d.create_table_as_new(&v, "SELECT 2 AS a"),
    )
    .await
    .unwrap();
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::Table
    );
    a.atomic_drop_and_create(
        &d.drop_table_sql(&v),
        &d.view_ddl(&v, "SELECT 3 AS a").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );
    assert_eq!(
        a.promotion_destination_kind(&tref(s, "v")).await.unwrap(),
        Some(ObjectKind::View)
    );
    assert_eq!(
        a.promotion_destination_kind(&tref(s, "absent"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        a.object_kind(&tref(s, "absent")).await.unwrap(),
        ObjectKind::Unknown
    );
}

#[tokio::test]
async fn quality_check_and_preview_sql_run() {
    let s = "rocky_live_checks";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    a.execute_statement(&format!(
        "CREATE TABLE {t} (id INT, name NVARCHAR(20), ts DATETIME2); \
         INSERT INTO {t} VALUES (1, N'a', '2020-01-01'), (2, NULL, '2020-01-02'), (3, N'c', NULL)"
    ))
    .await
    .unwrap();
    // Null-rate check with TABLESAMPLE.
    let null_rate =
        rocky_core::checks::generate_null_rate_sql(&tref(s, "t"), &["name".to_string()], 100, d)
            .unwrap();
    a.execute_query(&null_rate).await.unwrap();
    // TOP-limited sample and DISTINCT domain queries.
    let r = a
        .execute_query(&d.select_limited("*", &format!("FROM {t} ORDER BY id"), 2))
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    let r = a
        .execute_query(&d.select_limited(
            &format!("DISTINCT CAST(name AS {}) AS v", d.string_type_name()),
            &format!("FROM {t} WHERE name IS NOT NULL ORDER BY v"),
            1,
        ))
        .await
        .unwrap();
    assert_eq!(r.rows, vec![vec![serde_json::json!("a")]]);
    // Older-than and null-safe comparisons parse and evaluate.
    let older = d.date_minus_days_expr(30).unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT COUNT(*) FROM {t} WHERE ts < {older}"))
            .await
            .as_deref(),
        Some("2")
    );
    let neq = d.null_safe_neq("a.name", "b.name");
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT COUNT(*) FROM {t} a JOIN {t} b ON a.id = b.id + 1 WHERE {neq}")
        )
        .await
        .as_deref(),
        Some("2")
    );
    // Surrogate key matches MD5 of the same text on any other warehouse.
    assert_eq!(
        scalar(&a, &format!("SELECT {}", d.surrogate_key_expr(&["'abc'"])))
            .await
            .as_deref(),
        Some("900150983cd24fb0d6963f7d28e17f72")
    );
}

#[tokio::test]
async fn branch_clone_and_schema_creation() {
    let s = "rocky_live_branch";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    // Idempotent schema creation.
    let branch = "rocky_live_branch_b";
    let create = d.create_schema_sql("", branch).unwrap().unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&format!("DROP TABLE IF EXISTS [{branch}].[t]"))
        .await
        .unwrap();
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[t] (id INT); INSERT INTO [{s}].[t] VALUES (1)"
    ))
    .await
    .unwrap();
    a.clone_table_for_branch(&tref(s, "t"), branch)
        .await
        .unwrap();
    assert_eq!(count(&a, &format!("[{branch}].[t]")).await, "1");
    assert!(
        a.list_tables("", s)
            .await
            .unwrap()
            .contains(&"t".to_string())
    );
}

#[tokio::test]
async fn auth_failure_is_permanent_and_classified() {
    let Some(mut cfg) = config() else {
        return;
    };
    cfg.auth = rocky_sqlserver::Auth::SqlPassword {
        user: "sa".into(),
        password: "definitely-wrong".into(),
    };
    let a = SqlServerWarehouseAdapter::new(cfg).unwrap();
    let err = a.ping().await.unwrap_err();
    let inner = err
        .inner()
        .downcast_ref::<rocky_sqlserver::SqlServerError>()
        .expect("typed error");
    assert!(inner.is_auth(), "{inner}");
    assert_eq!(
        a.classify_failure(&err),
        rocky_core::failure_class::FailureClass::Permanent
    );
}

/// A source `IDENTITY` column must not become an `IDENTITY` on the table
/// Rocky creates: later inserts of that column would fail (error 8101).
#[tokio::test]
async fn identity_source_columns_are_not_inherited() {
    let s = "rocky_live_ident";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[src] (id INT IDENTITY, v INT); \
         INSERT INTO [{s}].[src] (v) VALUES (1), (2)"
    ))
    .await
    .unwrap();
    let src = d.format_table_ref("", s, "src").unwrap();
    for (table, create) in [
        (
            "first",
            d.create_table_as_new(
                &d.format_table_ref("", s, "first").unwrap(),
                &format!("SELECT id, v FROM {src}"),
            ),
        ),
        (
            "full",
            d.create_table_as(
                &d.format_table_ref("", s, "full").unwrap(),
                &format!("SELECT id, v FROM {src}"),
            ),
        ),
    ] {
        a.execute_statement(&create).await.unwrap();
        let t = d.format_table_ref("", s, table).unwrap();
        assert_eq!(
            scalar(
                &a,
                &format!(
                    "SELECT CAST(OBJECTPROPERTY(OBJECT_ID(N'{t}'), 'TableHasIdentity') AS INT)"
                )
            )
            .await
            .as_deref(),
            Some("0"),
            "{table}"
        );
        assert_eq!(count(&a, &t).await, "2");
        a.execute_statement(&d.insert_into(&t, &format!("SELECT id, v FROM {src}")))
            .await
            .unwrap();
        assert_eq!(count(&a, &t).await, "4");
    }
}

/// A `DATETIME` watermark (1/300 s ticks) does not re-admit its own row.
#[tokio::test]
async fn datetime_watermark_does_not_readmit_its_row() {
    let s = "rocky_live_dtwm";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    a.execute_statement(&format!(
        "CREATE TABLE [{s}].[src] (id INT, ts DATETIME); \
         INSERT INTO [{s}].[src] VALUES (1, '2026-01-01 00:00:00.007'), \
         (2, '2026-01-01 00:00:00.003'), (3, '2026-01-01 00:00:00.010')"
    ))
    .await
    .unwrap();
    let src = d.format_table_ref("", s, "src").unwrap();
    for id in 1..=3 {
        let max = scalar(&a, &format!("SELECT ts FROM {src} WHERE id = {id}"))
            .await
            .unwrap();
        let parsed = chrono::NaiveDateTime::parse_from_str(&max, "%Y-%m-%d %H:%M:%S%.f")
            .unwrap()
            .and_utc();
        let wh = d.watermark_where("ts", Some(&parsed)).unwrap();
        let r = scalar(
            &a,
            &format!("SELECT COUNT(*) FROM {src} {wh} AND id = {id}"),
        )
        .await;
        assert_eq!(
            r.as_deref(),
            Some("0"),
            "row {id} ({max}) re-admitted by {wh}"
        );
    }
}
