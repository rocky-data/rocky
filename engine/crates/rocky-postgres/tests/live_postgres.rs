//! Live tests against a real PostgreSQL server.
//!
//! Skipped (each test returns early) unless `ROCKY_POSTGRES_TEST_HOST` is
//! set. Connection settings:
//!
//! | Env var | Default |
//! |---|---|
//! | `ROCKY_POSTGRES_TEST_HOST` | — (required; `host` or `host:port`) |
//! | `ROCKY_POSTGRES_TEST_DB` | `postgres` |
//! | `ROCKY_POSTGRES_TEST_USER` | `postgres` |
//! | `ROCKY_POSTGRES_TEST_PASSWORD` | unset |
//! | `ROCKY_POSTGRES_TEST_SSLMODE` | `prefer` |
//! | `ROCKY_POSTGRES_TEST_SSLROOTCERT` | unset (PEM trusted under `verify-full`) |
//!
//! Each test works in its own schema (`rocky_live_<name>`), dropped first,
//! so tests run in parallel and re-run cleanly.
//!
//! These drive the adapter and the generators `rocky run` uses
//! (`rocky_core::sql_gen`, `rocky_core::drift`) — the SQL each strategy
//! emits is executed for real and its effect read back.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rocky_core::traits::{ObjectKind, WarehouseAdapter};
use rocky_ir::{ColumnSelection, TableRef};
use rocky_postgres::{Flavor, MergeMode, PgConfig, PostgresWarehouseAdapter, SslMode};

fn config(merge_mode: MergeMode) -> Option<PgConfig> {
    let host = std::env::var("ROCKY_POSTGRES_TEST_HOST").ok()?;
    let db = std::env::var("ROCKY_POSTGRES_TEST_DB").unwrap_or_else(|_| "postgres".into());
    let user = std::env::var("ROCKY_POSTGRES_TEST_USER").unwrap_or_else(|_| "postgres".into());
    let password = std::env::var("ROCKY_POSTGRES_TEST_PASSWORD").ok();
    let mut cfg = PgConfig::new(
        Flavor::Postgres,
        Some(&host),
        Some(&db),
        Some(&user),
        password.as_deref(),
        Duration::from_secs(60),
    )
    .expect("valid test config");
    if let Ok(mode) = std::env::var("ROCKY_POSTGRES_TEST_SSLMODE") {
        cfg.sslmode = SslMode::parse(&mode).expect("valid sslmode");
    }
    if let Ok(path) = std::env::var("ROCKY_POSTGRES_TEST_SSLROOTCERT") {
        cfg.sslrootcert = Some(path);
    }
    cfg.merge_mode = merge_mode;
    Some(cfg)
}

async fn setup(schema: &str, merge_mode: MergeMode) -> Option<PostgresWarehouseAdapter> {
    let cfg = config(merge_mode)?;
    let adapter = PostgresWarehouseAdapter::from_config(cfg, false).expect("adapter");
    adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
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

async fn scalar(adapter: &PostgresWarehouseAdapter, sql: &str) -> Option<String> {
    let r = adapter.execute_query(sql).await.expect("query");
    r.rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_str().map(str::to_string))
}

#[tokio::test]
async fn ping_and_typed_cells() {
    let Some(a) = setup("rocky_live_ping", MergeMode::Merge).await else {
        return;
    };
    a.ping().await.expect("ping");
    let r = a
        .execute_query(
            "SELECT 1::int AS i, true AS b, NULL::text AS n, \
             TIMESTAMPTZ '2026-09-15 10:00:00.25+00' AS tz, \
             TIMESTAMP '2026-09-15 10:00:00.25' AS ts, 'x'::text AS t",
        )
        .await
        .expect("query");
    assert_eq!(r.columns, vec!["i", "b", "n", "tz", "ts", "t"]);
    let row = &r.rows[0];
    assert_eq!(row[0], serde_json::json!("1"));
    assert_eq!(row[1], serde_json::json!("true"));
    assert_eq!(row[2], serde_json::Value::Null);
    assert_eq!(row[3], serde_json::json!("2026-09-15T10:00:00.250Z"));
    assert_eq!(row[4], serde_json::json!("2026-09-15 10:00:00.25"));
    assert_eq!(row[5], serde_json::json!("x"));
}

/// The literal rule the dialect states is proven against the server's own
/// lexer: a value holding a quote AND a backslash round-trips byte-exact.
#[tokio::test]
async fn literal_escape_round_trips() {
    let Some(a) = setup("rocky_live_literal", MergeMode::Merge).await else {
        return;
    };
    let value = "it's a \\ back\\slash\\' and\nnewline";
    let lit = rocky_core::sql_gen::string_literal(a.dialect(), value);
    assert_eq!(
        scalar(&a, &format!("SELECT {lit}")).await.as_deref(),
        Some(value)
    );
}

#[tokio::test]
async fn describe_table_canonical_types_and_missing_table() {
    let s = "rocky_live_describe";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.orders (order_id BIGINT NOT NULL, customer_id INTEGER, \
         amount NUMERIC(12,2), status VARCHAR(20), note TEXT, ok BOOLEAN, \
         order_date DATE, loaded_at TIMESTAMP, seen_at TIMESTAMPTZ, ratio DOUBLE PRECISION)"
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
            ("amount", "NUMERIC(12,2)", true),
            ("status", "VARCHAR(20)", true),
            ("note", "TEXT", true),
            ("ok", "BOOLEAN", true),
            ("order_date", "DATE", true),
            ("loaded_at", "TIMESTAMP", true),
            ("seen_at", "TIMESTAMPTZ", true),
            ("ratio", "DOUBLE PRECISION", true),
        ]
    );
    // Bare rendering folds case, so a mixed-case reference names the table.
    assert!(a.describe_table(&tref(s, "Orders")).await.is_ok());

    let err = a.describe_table(&tref(s, "nope")).await.unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");
    // A query against a missing table carries SQLSTATE 42P01.
    let err = a
        .execute_statement(&format!("SELECT * FROM {s}.nope"))
        .await
        .unwrap_err();
    assert!(a.is_missing_object_error(&err), "{err}");

    let tables = a.list_tables("", s).await.unwrap();
    assert_eq!(tables, vec!["orders"]);
    assert_eq!(
        a.object_kind(&tref(s, "orders")).await.unwrap(),
        ObjectKind::Table
    );
    assert_eq!(
        a.object_kind(&tref(s, "nope")).await.unwrap(),
        ObjectKind::Unknown
    );
}

/// Full refresh is idempotent and atomic, and a failure in the CTAS leaves
/// the previous table in place.
#[tokio::test]
async fn full_refresh_is_idempotent_and_atomic() {
    let s = "rocky_live_full";
    let Some(a) = setup(s, MergeMode::Merge).await else {
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
        scalar(&a, &format!("SELECT count(*) FROM {target}"))
            .await
            .as_deref(),
        Some("2")
    );
    // A failing body: the DROP in the same string must roll back.
    let err = a
        .execute_statement(&d.create_table_as(&target, "SELECT 1/0 AS id"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("division by zero"), "{err}");
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {target}"))
            .await
            .as_deref(),
        Some("2")
    );
    // The pool still works after an error (the failed connection was dropped).
    a.ping().await.unwrap();
}

#[tokio::test]
async fn incremental_watermark_and_drift() {
    let s = "rocky_live_incr";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.src (id INTEGER, amount INTEGER, _synced TIMESTAMP); \
         INSERT INTO {s}.src VALUES (1, 10, '2026-01-01 00:00:00.25'), (2, 20, '2026-01-02')"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let tgt = d.format_table_ref("", s, "tgt").unwrap();
    let src = d.format_table_ref("", s, "src").unwrap();
    a.execute_statement(&d.create_table_as(&tgt, &format!("SELECT * FROM {src}")))
        .await
        .unwrap();

    // Watermark from the target's max; the next run copies only newer rows.
    let max = scalar(&a, &format!("SELECT MAX(_synced) FROM {tgt}"))
        .await
        .unwrap();
    let parsed = chrono::NaiveDateTime::parse_from_str(&max, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc();
    a.execute_statement(&format!("INSERT INTO {s}.src VALUES (3, 30, '2026-01-03')"))
        .await
        .unwrap();
    let select = d.select_clause(&ColumnSelection::All, &[]).unwrap();
    let wh = d.watermark_where("_synced", Some(&parsed)).unwrap();
    a.execute_statement(&d.insert_into(&tgt, &format!("{select} FROM {src} {wh}")))
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {tgt}"))
            .await
            .as_deref(),
        Some("3")
    );

    // Schema drift: source widens INTEGER -> BIGINT; the dialect allows an
    // in-place ALTER, and the generated statements run.
    a.execute_statement(&format!(
        "ALTER TABLE {s}.src ALTER COLUMN amount TYPE BIGINT"
    ))
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

    // An unsafe change (INTEGER -> TEXT) degrades to drop-and-recreate.
    let unsafe_drift = rocky_core::drift::detect_drift(
        &tref(s, "tgt"),
        &[rocky_ir::ColumnInfo {
            name: "id".into(),
            data_type: "TEXT".into(),
            nullable: true,
        }],
        &[rocky_ir::ColumnInfo {
            name: "id".into(),
            data_type: "INTEGER".into(),
            nullable: true,
        }],
        d,
    );
    assert_eq!(unsafe_drift.action, rocky_ir::DriftAction::DropAndRecreate);
}

async fn merge_round_trip(mode: MergeMode, schema: &str) {
    let Some(a) = setup(schema, mode).await else {
        return;
    };
    // No unique index up front: as on a table Rocky's first-run CTAS
    // created. `on_conflict` must build the one it needs.
    let unique = "";
    a.execute_statement(&format!(
        "CREATE TABLE {schema}.tgt (id INTEGER, name TEXT, amount INTEGER{unique}); \
         INSERT INTO {schema}.tgt VALUES (1, 'a', 10), (2, 'b', 20)"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let tgt = d.format_table_ref("", schema, "tgt").unwrap();
    let keys: Vec<Arc<str>> = vec![Arc::from("id")];
    let cols = ColumnSelection::Explicit(vec!["id".into(), "name".into(), "amount".into()]);
    let src = "SELECT 2 AS id, 'B' AS name, 21 AS amount UNION ALL SELECT 3, 'c', 30";
    let sql = d.merge_into(&tgt, src, &keys, &cols).unwrap();
    a.execute_statement(&sql).await.unwrap();
    // Re-running is idempotent.
    a.execute_statement(&sql).await.unwrap();
    let r = a
        .execute_query(&format!("SELECT id, name, amount FROM {tgt} ORDER BY id"))
        .await
        .unwrap();
    let got: Vec<Vec<String>> = r
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect()
        })
        .collect();
    assert_eq!(
        got,
        vec![
            vec!["1", "a", "10"],
            vec!["2", "B", "21"],
            vec!["3", "c", "30"],
        ]
    );
}

#[tokio::test]
async fn merge_strategy_round_trips() {
    merge_round_trip(MergeMode::Merge, "rocky_live_merge").await;
}

#[tokio::test]
async fn on_conflict_strategy_round_trips() {
    merge_round_trip(MergeMode::OnConflict, "rocky_live_onconflict").await;
}

#[tokio::test]
async fn time_interval_overwrite_is_atomic() {
    let s = "rocky_live_ti";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.t (ds DATE, v INTEGER); \
         INSERT INTO {s}.t VALUES ('2026-01-01', 1), ('2026-01-02', 2)"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    let filter = "ds >= '2026-01-01 00:00:00' AND ds < '2026-01-02 00:00:00'";
    for stmt in d
        .insert_overwrite_partition(&t, filter, "SELECT DATE '2026-01-01' AS ds, 100 AS v")
        .unwrap()
    {
        a.execute_statement(&stmt).await.unwrap();
    }
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT string_agg(v::text, ',' ORDER BY ds) FROM {t}")
        )
        .await
        .as_deref(),
        Some("100,2")
    );
    // A failing INSERT must not leave the partition deleted.
    for stmt in d
        .insert_overwrite_partition(&t, filter, "SELECT DATE '2026-01-01' AS ds, 1/0 AS v")
        .unwrap()
    {
        assert!(a.execute_statement(&stmt).await.is_err());
    }
    assert_eq!(
        scalar(
            &a,
            &format!("SELECT string_agg(v::text, ',' ORDER BY ds) FROM {t}")
        )
        .await
        .as_deref(),
        Some("100,2")
    );
}

#[tokio::test]
async fn views_materialized_views_and_kind_switch() {
    let s = "rocky_live_views";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.base AS SELECT g AS id FROM generate_series(1, 5) g"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let v = d.format_table_ref("", s, "v").unwrap();
    let mv = d.format_table_ref("", s, "mv").unwrap();
    let body = format!("SELECT id FROM {s}.base WHERE id > 2");
    a.execute_statement(&d.view_ddl(&v, &body).unwrap())
        .await
        .unwrap();
    a.execute_statement(&d.view_ddl(&v, &body).unwrap())
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {v}"))
            .await
            .as_deref(),
        Some("3")
    );
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::View
    );

    a.execute_statement(&d.materialized_view_ddl(&mv, &body).unwrap())
        .await
        .unwrap();
    a.execute_statement(&format!("INSERT INTO {s}.base VALUES (6)"))
        .await
        .unwrap();
    // Re-running rebuilds (and so refreshes) the materialized view, and picks
    // up a changed definition.
    a.execute_statement(
        &d.materialized_view_ddl(&mv, &format!("{body} AND id < 6"))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {mv}"))
            .await
            .as_deref(),
        Some("3")
    );
    a.execute_statement(&d.materialized_view_ddl(&mv, &body).unwrap())
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {mv}"))
            .await
            .as_deref(),
        Some("4")
    );
    // describe_table sees materialized views.
    assert_eq!(a.describe_table(&tref(s, "mv")).await.unwrap().len(), 1);

    // A view -> table kind switch runs atomically.
    let drop = format!("DROP VIEW IF EXISTS {v}");
    let create = d.create_table_as_new(&v, "SELECT 1 AS id");
    assert!(
        a.atomic_drop_and_create(&drop, &create)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        a.object_kind(&tref(s, "v")).await.unwrap(),
        ObjectKind::Table
    );
}

#[tokio::test]
async fn quality_check_expressions_run() {
    let s = "rocky_live_checks";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.t AS SELECT g AS id, 'user' || g || '@x.io' AS email, \
         CURRENT_DATE - g AS d FROM generate_series(1, 20) g"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let regex = d
        .regex_match_predicate("email", "^[a-z0-9]+@x[.]io$")
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {s}.t WHERE {regex}"))
            .await
            .as_deref(),
        Some("20")
    );
    let older = d.date_minus_days_expr(10).unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {s}.t WHERE d < {older}"))
            .await
            .as_deref(),
        Some("10")
    );
    let sample = d.tablesample_clause(100).unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {s}.t {sample}"))
            .await
            .as_deref(),
        Some("20")
    );
    let now = d.current_timestamp_expr();
    assert!(scalar(&a, &format!("SELECT {now}::text")).await.is_some());
    let key = d.surrogate_key_expr(&["id", "email"]);
    assert_eq!(
        scalar(&a, &format!("SELECT count(DISTINCT {key}) FROM {s}.t"))
            .await
            .as_deref(),
        Some("20")
    );
    let hash = d.row_hash_expr(&["id".into(), "email".into()]).unwrap();
    assert!(
        scalar(&a, &format!("SELECT bit_xor({hash})::text FROM {s}.t"))
            .await
            .is_some()
    );
    let neq = d.null_safe_neq("NULL::int", "1");
    assert_eq!(
        scalar(&a, &format!("SELECT ({neq})::text"))
            .await
            .as_deref(),
        Some("true")
    );
}

#[tokio::test]
async fn branch_clone_and_schema_creation() {
    let s = "rocky_live_branch";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!("DROP SCHEMA IF EXISTS {s}_pr CASCADE"))
        .await
        .unwrap();
    let d = a.dialect();
    let create = d
        .create_schema_sql("", &format!("{s}_pr"))
        .unwrap()
        .unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&create).await.unwrap();
    a.execute_statement(&format!("CREATE TABLE {s}.t AS SELECT 1 AS id"))
        .await
        .unwrap();
    a.clone_table_for_branch(&tref(s, "t"), &format!("{s}_pr"))
        .await
        .unwrap();
    a.clone_table_for_branch(&tref(s, "t"), &format!("{s}_pr"))
        .await
        .unwrap();
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {s}_pr.t"))
            .await
            .as_deref(),
        Some("1")
    );
    a.execute_statement(&format!("DROP SCHEMA {s}_pr CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
async fn auth_failure_is_permanent_and_classified() {
    let Some(mut cfg) = config(MergeMode::Merge) else {
        return;
    };
    cfg.password = Some("definitely-wrong".into());
    let a = PostgresWarehouseAdapter::from_config(cfg, false).unwrap();
    let err = a.ping().await.unwrap_err();
    let pg = err
        .inner()
        .downcast_ref::<rocky_postgres::PgError>()
        .expect("typed error");
    // Only a server that enforces passwords answers 28P01.
    if pg.sqlstate().is_some() {
        assert!(pg.is_auth(), "{pg}");
        assert_eq!(
            a.classify_failure(&err),
            rocky_core::failure_class::FailureClass::Permanent
        );
    }
}

#[tokio::test]
async fn unknown_extra_key_is_refused_before_connecting() {
    let Some(cfg) = config(MergeMode::Merge) else {
        return;
    };
    let mut extra = BTreeMap::new();
    extra.insert("sslmdoe".to_string(), serde_json::json!("require"));
    assert!(cfg.apply_extra(&extra).is_err());
}

#[tokio::test]
async fn delete_insert_is_atomic() {
    let s = "rocky_live_di";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.t (k INTEGER, v INTEGER); INSERT INTO {s}.t VALUES (1, 1), (2, 2)"
    ))
    .await
    .unwrap();
    let d = a.dialect();
    let t = d.format_table_ref("", s, "t").unwrap();
    let stmts = d.delete_insert_statements(
        format!("DELETE FROM {t} WHERE k IN (SELECT 1)"),
        d.insert_into(&t, "SELECT 1 AS k, 1/0 AS v"),
    );
    assert_eq!(stmts.len(), 1);
    assert!(a.execute_statement(&stmts[0]).await.is_err());
    // The failed INSERT rolled back the DELETE.
    assert_eq!(
        scalar(&a, &format!("SELECT count(*) FROM {t}"))
            .await
            .as_deref(),
        Some("2")
    );
}

/// The generic SCD2 snapshot SQL runs on PostgreSQL 15+: bootstrap, close a
/// changed row, insert its new version, invalidate a hard delete.
#[tokio::test]
async fn snapshot_sql_runs_on_postgres() {
    let s = "rocky_live_snap";
    let Some(a) = setup(s, MergeMode::Merge).await else {
        return;
    };
    a.execute_statement(&format!(
        "CREATE TABLE {s}.src (id INTEGER, name TEXT, updated_at TIMESTAMP); \
         INSERT INTO {s}.src VALUES (1, 'a', '2026-01-01'), (2, 'b', '2026-01-01')"
    ))
    .await
    .unwrap();
    let ir = rocky_ir::ModelIr::snapshot(
        rocky_ir::TargetRef {
            catalog: String::new(),
            schema: s.into(),
            table: "snap".into(),
        },
        rocky_ir::SourceRef {
            catalog: String::new(),
            schema: s.into(),
            table: "src".into(),
        },
        vec![Arc::from("id")],
        "updated_at".into(),
        true,
        rocky_ir::GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
    );
    let cols: Vec<String> = vec!["id".into(), "name".into(), "updated_at".into()];
    async fn run(a: &PostgresWarehouseAdapter, ir: &rocky_ir::ModelIr, cols: &[String]) {
        for stmt in rocky_core::sql_gen::generate_snapshot_sql(ir, a.dialect(), cols).unwrap() {
            a.execute_statement(&stmt).await.unwrap();
        }
    }
    run(&a, &ir, &cols).await;
    a.execute_statement(&format!(
        "UPDATE {s}.src SET name = 'a2', updated_at = '2026-01-02' WHERE id = 1; \
         DELETE FROM {s}.src WHERE id = 2"
    ))
    .await
    .unwrap();
    run(&a, &ir, &cols).await;
    let r = a
        .execute_query(&format!(
            "SELECT id::text, name, (valid_to IS NULL)::text FROM {s}.snap ORDER BY id, valid_from"
        ))
        .await
        .unwrap();
    let got: Vec<Vec<&str>> = r
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.as_str().unwrap()).collect())
        .collect();
    assert_eq!(
        got,
        vec![
            vec!["1", "a", "false"],
            vec!["1", "a2", "true"],
            vec!["2", "b", "false"],
        ]
    );
}
