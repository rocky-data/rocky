//! Live tests against a real ClickHouse server.
//!
//! Skipped (each test returns early) unless `ROCKY_CLICKHOUSE_TEST_HOST` is
//! set. Connection settings:
//!
//! | Env var | Default |
//! |---|---|
//! | `ROCKY_CLICKHOUSE_TEST_HOST` | — (required; `host` or `host:port`, HTTP interface) |
//! | `ROCKY_CLICKHOUSE_TEST_USER` | `default` |
//! | `ROCKY_CLICKHOUSE_TEST_PASSWORD` | unset |
//! | `ROCKY_CLICKHOUSE_TEST_SECURE` | unset (`true` for HTTPS) |
//!
//! Each test works in its own database (`rocky_live_<name>`), dropped
//! first, so tests run in parallel and re-run cleanly.
//!
//! These drive the adapter and the generators `rocky run` uses
//! (`rocky_core::sql_gen`, `rocky_core::drift`) — the SQL each strategy
//! emits is executed for real and its effect read back.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rocky_clickhouse::{ChConfig, ChError, ClickHouseWarehouseAdapter};
use rocky_core::traits::{ObjectKind, WarehouseAdapter};
use rocky_ir::{ColumnSelection, TableRef};

fn config() -> Option<ChConfig> {
    let host = std::env::var("ROCKY_CLICKHOUSE_TEST_HOST").ok()?;
    let user = std::env::var("ROCKY_CLICKHOUSE_TEST_USER").ok();
    let password = std::env::var("ROCKY_CLICKHOUSE_TEST_PASSWORD").ok();
    let cfg = ChConfig::new(
        Some(&host),
        None,
        user.as_deref(),
        password.as_deref(),
        Duration::from_secs(60),
    )
    .expect("valid test config");
    let mut extra = BTreeMap::new();
    if let Ok(secure) = std::env::var("ROCKY_CLICKHOUSE_TEST_SECURE") {
        extra.insert("secure".to_string(), serde_json::json!(secure));
    }
    Some(cfg.apply_extra(&extra).expect("valid extra"))
}

async fn setup(db: &str) -> Option<ClickHouseWarehouseAdapter> {
    let adapter = ClickHouseWarehouseAdapter::new(config()?).expect("adapter");
    adapter
        .execute_statement(&format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .expect("drop database");
    adapter
        .execute_statement(&format!("CREATE DATABASE {db}"))
        .await
        .expect("create database");
    Some(adapter)
}

fn tref(db: &str, table: &str) -> TableRef {
    TableRef {
        catalog: String::new(),
        schema: db.into(),
        table: table.into(),
    }
}

async fn scalar(adapter: &ClickHouseWarehouseAdapter, sql: &str) -> Option<String> {
    let r = adapter.execute_query(sql).await.expect("query");
    r.rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_str().map(str::to_string))
}

async fn run_all(a: &ClickHouseWarehouseAdapter, stmts: &[String]) {
    for stmt in stmts {
        a.execute_statement(stmt).await.expect(stmt);
    }
}

#[tokio::test]
async fn ping_and_text_cells() {
    let Some(a) = setup("rocky_live_ping").await else {
        return;
    };
    a.ping().await.unwrap();
    let r = a
        .execute_query(
            "SELECT toInt64(42) AS i, true AS b, CAST(NULL AS Nullable(Int32)) AS n, \
             toDateTime('2026-01-02 03:04:05') AS ts, [1, 2] AS arr",
        )
        .await
        .unwrap();
    assert_eq!(r.columns, ["i", "b", "n", "ts", "arr"]);
    assert_eq!(
        r.rows[0],
        vec![
            serde_json::json!("42"),
            serde_json::json!("true"),
            serde_json::Value::Null,
            // UTC, RFC 3339: what Rocky's watermark reader parses.
            serde_json::json!("2026-01-02T03:04:05Z"),
            serde_json::json!("[1,2]"),
        ]
    );
}

#[tokio::test]
async fn literal_escape_round_trips() {
    let Some(a) = setup("rocky_live_literal").await else {
        return;
    };
    let value = "it's a \\ back\\slash\\' and\nnewline\tand tab";
    let lit = rocky_core::sql_gen::string_literal(a.dialect(), value);
    assert_eq!(
        scalar(&a, &format!("SELECT {lit}")).await.as_deref(),
        Some(value)
    );
}

#[tokio::test]
async fn describe_table_canonical_types_and_missing_table() {
    let s = "rocky_live_describe";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.orders (order_id Int64, customer_id Nullable(Int32), \
         amount Decimal(12, 2), status LowCardinality(String), \
         note LowCardinality(Nullable(String)), ok Bool, order_date Date, \
         loaded_at DateTime('UTC'), seen_at Nullable(DateTime64(3)), ratio Float64, \
         tags Array(String), n UInt64) ENGINE = MergeTree ORDER BY order_id"
    ))
    .await
    .unwrap();
    let cols = a.describe_table(&tref(s, "orders")).await.unwrap();
    let got: Vec<(&str, &str, bool)> = cols
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str(), c.nullable))
        .collect();
    assert_eq!(
        got,
        vec![
            ("order_id", "BIGINT", false),
            ("customer_id", "INTEGER", true),
            ("amount", "DECIMAL(12,2)", false),
            ("status", "TEXT", false),
            ("note", "TEXT", true),
            ("ok", "BOOLEAN", false),
            ("order_date", "DATE", false),
            ("loaded_at", "TIMESTAMP", false),
            ("seen_at", "DateTime64(3)", true),
            ("ratio", "DOUBLE", false),
            ("tags", "Array(String)", false),
            ("n", "UInt64", false),
        ]
    );
    // Every canonical name is valid ClickHouse DDL for the same column type.
    for c in &cols {
        a.execute_statement(&format!("SELECT defaultValueOfTypeName('{}')", c.data_type))
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", c.data_type));
    }

    // ClickHouse names are case-sensitive.
    let err = a.describe_table(&tref(s, "Orders")).await.unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    let err = a.describe_table(&tref(s, "nope")).await.unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    // A query against a missing table carries UNKNOWN_TABLE (60).
    let err = a
        .execute_statement(&format!("SELECT * FROM {s}.nope"))
        .await
        .unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    let err = a
        .execute_statement("SELECT * FROM rocky_live_no_such_db.t")
        .await
        .unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");

    assert_eq!(a.list_tables("", s).await.unwrap(), vec!["orders"]);
    assert_eq!(
        a.object_kind(&tref(s, "orders")).await.unwrap(),
        ObjectKind::Table
    );
    assert_eq!(
        a.object_kind(&tref(s, "nope")).await.unwrap(),
        ObjectKind::Unknown
    );
    let explain = a
        .explain(&format!("SELECT * FROM {s}.orders"))
        .await
        .unwrap();
    assert_eq!(explain.estimated_rows, Some(0));
}

/// Full refresh is idempotent and atomic: a failing `SELECT` leaves the
/// previous table in place.
#[tokio::test]
async fn full_refresh_is_idempotent_and_atomic() {
    let s = "rocky_live_full";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let target = d.format_table_ref("", s, "t").unwrap();
    a.execute_statement(&d.create_table_as(&target, "SELECT 1 AS id"))
        .await
        .unwrap();
    a.execute_statement(&d.create_table_as(&target, "SELECT 2 AS id UNION ALL SELECT 3"))
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count() FROM {target}"))
            .await
            .as_deref(),
        Some("2")
    );
    let err = a
        .execute_statement(&d.create_table_as(&target, "SELECT throwIf(1) AS id"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("395"), "{err}");
    assert_eq!(
        scalar(&a, &format!("SELECT count() FROM {target}"))
            .await
            .as_deref(),
        Some("2")
    );
    // A view replaced by a table and back: the kind switch the run loop
    // performs through `CREATE OR REPLACE`.
    let view = d.format_table_ref("", s, "v").unwrap();
    a.execute_statement(&d.view_ddl(&view, "SELECT 1 AS x").unwrap())
        .await
        .unwrap();
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );
    a.execute_statement(&d.create_table_as(&view, "SELECT 1 AS x"))
        .await
        .unwrap();
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::Table
    );
    // The run loop's opted-in kind switch: one statement, no DROP, both ways.
    let stats = a
        .atomic_drop_and_create(
            &format!("DROP TABLE {view}"),
            &d.view_ddl(&view, "SELECT 2 AS x").unwrap(),
        )
        .await
        .unwrap();
    assert!(stats.is_some());
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );
    // A failing CREATE leaves the view.
    assert!(
        a.atomic_drop_and_create(
            &format!("DROP VIEW {view}"),
            &d.create_table_as(&view, "SELECT throwIf(1) AS x"),
        )
        .await
        .is_err()
    );
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );
    // A CREATE that would need the DROP first is not taken.
    assert!(
        a.atomic_drop_and_create("DROP VIEW x", "CREATE TABLE x AS SELECT 1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn table_options_shape_the_table() {
    let s = "rocky_live_opts";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let target = d.format_table_ref("", s, "t").unwrap();
    let opts = rocky_ir::ClickHouseTableOptions {
        engine: Some("ReplacingMergeTree".into()),
        order_by: vec!["id".into()],
        partition_by: Some("toYYYYMM(d)".into()),
    };
    a.execute_statement(
        &d.create_table_as_with_clickhouse_options(
            &target,
            "SELECT toInt64(number) AS id, toDate('2026-01-01') + number * 40 AS d \
             FROM numbers(3)",
            &opts,
            true,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let r = a
        .execute_query(&format!(
            "SELECT engine, partition_key, sorting_key FROM system.tables \
             WHERE database = '{s}' AND name = 't'"
        ))
        .await
        .unwrap();
    assert_eq!(
        r.rows[0],
        vec![
            serde_json::json!("ReplacingMergeTree"),
            serde_json::json!("toYYYYMM(d)"),
            serde_json::json!("id"),
        ]
    );
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT count(DISTINCT partition) FROM system.parts WHERE database = '{s}' AND table = 't' AND active")
        )
        .await
        .as_deref(),
        Some("3")
    );
}

#[tokio::test]
async fn incremental_watermark_and_drift() {
    let s = "rocky_live_incr";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.src (id Int32, amount Int32, _synced DateTime64(3)) \
         ENGINE = MergeTree ORDER BY id"
    ))
    .await
    .unwrap();
    a.execute_statement(&format!(
        "INSERT INTO {s}.src VALUES (1, 10, '2026-01-01 00:00:00.250'), \
         (2, 20, '2026-01-02 00:00:00')"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let tgt = d.format_table_ref("", s, "tgt").unwrap();
    let src = d.format_table_ref("", s, "src").unwrap();
    a.execute_statement(&d.create_table_as(&tgt, &format!("SELECT * FROM {src}")))
        .await
        .unwrap();

    // The watermark cell parses the way the run loop reads it, fraction
    // included, and the next run copies only newer rows.
    a.execute_statement(&format!(
        "INSERT INTO {s}.src VALUES (3, 30, '2026-01-01 00:00:00.250')"
    ))
    .await
    .unwrap();
    let max = scalar(
        &a,
        &format!("SELECT MAX(_synced) FROM {s}.src WHERE id <> 2"),
    )
    .await
    .unwrap();
    let parsed: chrono::DateTime<chrono::Utc> = max.parse().unwrap();
    assert_eq!(parsed.timestamp_subsec_millis(), 250, "{max}");
    a.execute_statement(&format!("INSERT INTO {s}.src VALUES (4, 40, '2026-01-03')"))
        .await
        .unwrap();
    let select = d.select_clause(&ColumnSelection::All, &[]).unwrap();
    let wm = chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let wh = d.watermark_where("_synced", Some(&wm)).unwrap();
    a.execute_statement(&d.insert_into(&tgt, &format!("{select} FROM {src} {wh}")))
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT arrayStringConcat(groupArray(toString(id)), ',') FROM (SELECT id FROM {tgt} ORDER BY id)"))
            .await
            .as_deref(),
        Some("1,2,4")
    );

    // Schema drift: every type change rebuilds the table on ClickHouse.
    a.execute_statement(&format!("ALTER TABLE {s}.src MODIFY COLUMN amount Int64"))
        .await
        .unwrap();
    let src_cols = a.describe_table(&tref(s, "src")).await.unwrap();
    let tgt_cols = a.describe_table(&tref(s, "tgt")).await.unwrap();
    let drift = rocky_core::drift::detect_drift(&tref(s, "tgt"), &src_cols, &tgt_cols, d);
    assert_eq!(
        drift.action,
        rocky_ir::DriftAction::DropAndRecreate,
        "{drift:?}"
    );
}

#[tokio::test]
async fn time_interval_overwrite_keeps_the_target_whole_on_a_failing_select() {
    let s = "rocky_live_ti";
    let Some(a) = setup(s).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.t (ds Date, v Int32) ENGINE = MergeTree ORDER BY ds"
    ))
    .await
    .unwrap();
    a.execute_statement(&format!(
        "INSERT INTO {s}.t VALUES ('2026-01-01', 1), ('2026-01-02', 2)"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    // The exact filter shape `sql_gen` builds, against a `Date` column.
    let filter = "ds >= '2026-01-01 00:00:00' AND ds < '2026-01-02 00:00:00'";
    run_all(
        &a,
        &d.insert_overwrite_partition(
            &t,
            filter,
            "SELECT toDate('2026-01-01') AS ds, toInt32(100) AS v",
        )
        .unwrap(),
    )
    .await;
    let read = format!(
        "SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM (SELECT v FROM {t} ORDER BY ds)"
    );
    assert_eq!(scalar(&a, &read).await.as_deref(), Some("100,2"));
    // Re-running the same window is idempotent.
    run_all(
        &a,
        &d.insert_overwrite_partition(
            &t,
            filter,
            "SELECT toDate('2026-01-01') AS ds, toInt32(100) AS v",
        )
        .unwrap(),
    )
    .await;
    assert_eq!(scalar(&a, &read).await.as_deref(), Some("100,2"));
    // A failing SELECT stops before the DELETE: the window survives.
    for stmt in d
        .insert_overwrite_partition(
            &t,
            filter,
            "SELECT toDate('2026-01-01') AS ds, toInt32(throwIf(1)) AS v",
        )
        .unwrap()
    {
        if a.execute_statement(&stmt).await.is_err() {
            break;
        }
    }
    assert_eq!(scalar(&a, &read).await.as_deref(), Some("100,2"));
    // The staging table does not linger after a successful run.
    run_all(
        &a,
        &d.insert_overwrite_partition(
            &t,
            filter,
            "SELECT toDate('2026-01-01') AS ds, toInt32(7) AS v",
        )
        .unwrap(),
    )
    .await;
    assert_eq!(scalar(&a, &read).await.as_deref(), Some("7,2"));
    assert_eq!(a.list_tables("", s).await.unwrap(), vec!["t"]);
}

#[tokio::test]
async fn delete_insert_replaces_rows_by_key() {
    let s = "rocky_live_di";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    a.execute_statement(&d.create_table_as(
        &t,
        "SELECT toInt32(number + 1) AS k, toInt32(number + 1) AS v FROM numbers(3)",
    ))
    .await
    .unwrap();
    let ir = rocky_ir::ModelIr::transformation(
        rocky_ir::TargetRef {
            catalog: String::new(),
            schema: s.into(),
            table: "t".into(),
        },
        rocky_ir::MaterializationStrategy::DeleteInsert {
            partition_by: vec!["k".into()],
        },
        Vec::new(),
        "SELECT toInt32(2) AS k, toInt32(20) AS v UNION ALL SELECT toInt32(4), toInt32(40)".into(),
        rocky_ir::GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        None,
        None,
    );
    run_all(
        &a,
        &rocky_core::sql_gen::generate_transformation_sql(&ir, d).unwrap(),
    )
    .await;
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT arrayStringConcat(groupArray(toString(v)), ',') FROM (SELECT v FROM {t} ORDER BY k)")
        )
        .await
        .as_deref(),
        Some("1,20,3,40")
    );
}

#[tokio::test]
async fn quality_check_expressions_run() {
    let s = "rocky_live_checks";
    let Some(a) = setup(s).await else {
        return;
    };
    let d = a.dialect();
    a.execute_statement(&d.create_table_as(
        &format!("{s}.t"),
        "SELECT toInt64(number + 1) AS id, concat('user', toString(number + 1), '@x.io') AS email, \
         subtractDays(today(), number + 1) AS d FROM numbers(20)",
    ))
    .await
    .unwrap();
    let regex = d
        .regex_match_predicate("email", "^[a-z0-9]+@x[.]io$")
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count() FROM {s}.t WHERE {regex}"))
            .await
            .as_deref(),
        Some("20")
    );
    let older = d.date_minus_days_expr(10).unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count() FROM {s}.t WHERE d < {older}"))
            .await
            .as_deref(),
        Some("10")
    );
    let now = d.current_timestamp_expr();
    assert!(
        scalar(&a, &format!("SELECT toString({now})"))
            .await
            .is_some()
    );
    // Same hex digest as every other warehouse's `md5('1-a')`.
    let key = d.surrogate_key_expr(&["'1'", "'a'"]);
    assert_eq!(
        scalar(&a, &format!("SELECT {key}")).await.as_deref(),
        Some("fd863b01647cb662580dd67b9839d8de")
    );
    assert_eq!(
        scalar(
            &a,
            &format!(
                "SELECT count(DISTINCT {}) FROM {s}.t",
                d.surrogate_key_expr(&["id", "email"])
            )
        )
        .await
        .as_deref(),
        Some("20")
    );
    let neq = d.null_safe_neq("CAST(NULL AS Nullable(Int32))", "1");
    assert_eq!(
        scalar(&a, &format!("SELECT toString({neq})"))
            .await
            .as_deref(),
        Some("1")
    );
    let star = d.star_excluding(&["email"]).unwrap();
    let r = a
        .execute_query(&format!("SELECT {star} FROM {s}.t LIMIT 1"))
        .await
        .unwrap();
    assert_eq!(r.columns, ["id", "d"]);
}

#[tokio::test]
async fn branch_clone_and_schema_creation() {
    let s = "rocky_live_branch";
    let Some(a) = setup(s).await else {
        return;
    };
    let pr = format!("{s}_pr");
    a.execute_statement(&format!("DROP DATABASE IF EXISTS {pr}"))
        .await
        .unwrap();
    let d = a.dialect();
    let create = d.create_schema_sql("", &pr).unwrap().unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&format!(
        "CREATE TABLE {s}.t ENGINE = MergeTree ORDER BY id AS SELECT toInt64(1) AS id"
    ))
    .await
    .unwrap();
    a.clone_table_for_branch(&tref(s, "t"), &pr).await.unwrap();
    a.clone_table_for_branch(&tref(s, "t"), &pr).await.unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count() FROM {pr}.t"))
            .await
            .as_deref(),
        Some("1")
    );
    // The clone keeps the source's sorting key.
    assert_eq!(
        scalar(
            &a,
            &format!(
                "SELECT sorting_key FROM system.tables WHERE database = '{pr}' AND name = 't'"
            )
        )
        .await
        .as_deref(),
        Some("id")
    );
    a.execute_statement(&format!("DROP DATABASE {pr}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn auth_failure_is_permanent_and_merge_is_refused() {
    let Some(mut cfg) = config() else {
        return;
    };
    cfg.user = "rocky_no_such_user".into();
    let a = ClickHouseWarehouseAdapter::new(cfg).unwrap();
    let err = a.ping().await.unwrap_err();
    let ch = err.inner().downcast_ref::<ChError>().expect("typed error");
    assert!(matches!(ch, ChError::Server { .. }), "{ch}");
    assert_eq!(
        a.classify_failure(&err),
        rocky_core::failure_class::FailureClass::Permanent
    );
    let merge = a
        .dialect()
        .merge_into("d.t", "SELECT 1", &[Arc::from("id")], &ColumnSelection::All)
        .unwrap_err();
    assert!(merge.to_string().contains("E053"), "{merge}");
}
