//! End-to-end `rocky run` against a live ClickHouse.
//!
//! Skipped unless `ROCKY_CLICKHOUSE_TEST_HOST` is set (`host` or
//! `host:port`, HTTP interface); also reads `ROCKY_CLICKHOUSE_TEST_USER`
//! (default `default`) and `ROCKY_CLICKHOUSE_TEST_PASSWORD`. Seeds its own
//! databases (`rocky_e2e_ch_raw`, `rocky_e2e_ch_marts`) through the adapter,
//! then drives the real binary over a transformation pipeline with one model
//! per strategy ClickHouse runs: view, full_refresh (reading the view, with
//! `[clickhouse]` table options), delete_insert, incremental append and
//! time_interval on a `Date` column. A second run after a source change
//! proves idempotency, the watermark, and the key replace.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use rocky_clickhouse::{ChConfig, ClickHouseWarehouseAdapter};
use rocky_core::traits::WarehouseAdapter;

struct Live {
    host: String,
    user: String,
    password: String,
}

fn live() -> Option<Live> {
    Some(Live {
        host: std::env::var("ROCKY_CLICKHOUSE_TEST_HOST").ok()?,
        user: std::env::var("ROCKY_CLICKHOUSE_TEST_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("ROCKY_CLICKHOUSE_TEST_PASSWORD").unwrap_or_default(),
    })
}

fn adapter(l: &Live) -> ClickHouseWarehouseAdapter {
    let cfg = ChConfig::new(
        Some(&l.host),
        None,
        Some(&l.user),
        Some(&l.password)
            .filter(|p| !p.is_empty())
            .map(String::as_str),
        Duration::from_secs(60),
    )
    .expect("config");
    ClickHouseWarehouseAdapter::new(cfg).expect("adapter")
}

fn model(dir: &Path, name: &str, sql: &str, toml: &str) {
    fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        dir.join(format!("{name}.toml")),
        format!("name = \"{name}\"\n{toml}"),
    )
    .unwrap();
}

fn rocky(project: &Path, l: &Live, args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(project)
        .args(args)
        .env("ROCKY_E2E_CH_PASSWORD", &l.password)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky");
    (
        out.status.success(),
        format!(
            "{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

async fn rows(a: &ClickHouseWarehouseAdapter, sql: &str) -> Vec<Vec<String>> {
    a.execute_query(sql)
        .await
        .expect(sql)
        .rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| v.as_str().unwrap_or("NULL").to_string())
                .collect()
        })
        .collect()
}

async fn exec(a: &ClickHouseWarehouseAdapter, stmts: &[&str]) {
    for stmt in stmts {
        a.execute_statement(stmt).await.expect(stmt);
    }
}

#[tokio::test]
async fn transformation_pipeline_runs_every_supported_strategy() {
    let Some(l) = live() else {
        return;
    };
    let a = adapter(&l);
    exec(
        &a,
        &[
            "DROP DATABASE IF EXISTS rocky_e2e_ch_raw",
            "DROP DATABASE IF EXISTS rocky_e2e_ch_marts",
            "CREATE DATABASE rocky_e2e_ch_raw",
            "CREATE TABLE rocky_e2e_ch_raw.orders (order_id Int64, customer_id Int64, \
             amount Float64, status String, order_date Date, loaded_at DateTime64(3)) \
             ENGINE = MergeTree ORDER BY order_id",
            "CREATE TABLE rocky_e2e_ch_raw.customers (customer_id Int64, customer_name String, \
             email String) ENGINE = MergeTree ORDER BY customer_id",
            "INSERT INTO rocky_e2e_ch_raw.orders VALUES \
             (1, 1, 10, 'paid', '2026-01-01', '2026-01-01 10:00:00.250'), \
             (2, 1, 20, 'paid', '2026-01-02', '2026-01-02 10:00:00'), \
             (3, 2, 5, 'open', '2026-01-02', '2026-01-02 11:00:00')",
            "INSERT INTO rocky_e2e_ch_raw.customers VALUES (1, 'ann', 'a@x.io'), (2, 'bob', 'b@x.io')",
        ],
    )
    .await;

    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path();
    let models = project.join("models");
    fs::create_dir(&models).unwrap();
    fs::write(
        project.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"clickhouse\"\nhost = \"{}\"\nusername = \"{}\"\n\
             password = \"${{ROCKY_E2E_CH_PASSWORD}}\"\n\n\
             [pipeline.ch]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.ch.target.governance]\nauto_create_schemas = true\n",
            l.host, l.user
        ),
    )
    .unwrap();
    fs::write(
        models.join("_defaults.toml"),
        "[target]\ncatalog = \"\"\nschema = \"rocky_e2e_ch_marts\"\n",
    )
    .unwrap();
    model(
        &models,
        "stg_orders",
        "SELECT order_id, customer_id, amount, status, order_date FROM rocky_e2e_ch_raw.orders",
        "[strategy]\ntype = \"view\"\n",
    );
    model(
        &models,
        "fct_revenue",
        "SELECT c.customer_name, SUM(o.amount) AS total FROM rocky_e2e_ch_marts.stg_orders o \
         JOIN rocky_e2e_ch_raw.customers c ON o.customer_id = c.customer_id \
         GROUP BY c.customer_name",
        "depends_on = [\"stg_orders\"]\n\n[clickhouse]\norder_by = [\"customer_name\"]\n",
    );
    model(
        &models,
        "orders_by_customer",
        "SELECT order_id, customer_id, amount FROM rocky_e2e_ch_raw.orders",
        "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"customer_id\"]\n",
    );
    model(
        &models,
        "orders_log",
        "SELECT order_id, loaded_at FROM rocky_e2e_ch_raw.orders WHERE @incremental_filter",
        "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"loaded_at\"\n",
    );
    model(
        &models,
        "daily_revenue",
        "SELECT order_date, SUM(amount) AS revenue FROM rocky_e2e_ch_raw.orders \
         WHERE order_date >= toDate(@start_date) AND order_date < toDate(@end_date) \
         GROUP BY order_date",
        "[strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\n\
         granularity = \"day\"\nfirst_partition = \"2026-01-01\"\n\n\
         [clickhouse]\npartition_by = \"toYYYYMM(order_date)\"\norder_by = [\"order_date\"]\n",
    );

    let (ok, out) = rocky(project, &l, &["compile", "--output", "json"]);
    assert!(ok, "compile failed:\n{out}");
    let (ok, out) = rocky(project, &l, &["run", "--output", "json"]);
    assert!(ok, "first run failed:\n{out}");

    // Source changes, then a second full run and a partition backfill.
    exec(
        &a,
        &[
            "INSERT INTO rocky_e2e_ch_raw.customers VALUES (3, 'cy', 'c@x.io')",
            "INSERT INTO rocky_e2e_ch_raw.orders VALUES \
             (4, 3, 7, 'paid', '2026-01-03', '2026-01-03 09:00:00')",
            // Customer 1's rows change: delete_insert must replace them.
            "ALTER TABLE rocky_e2e_ch_raw.orders UPDATE amount = 11 WHERE order_id = 1",
        ],
    )
    .await;
    let (ok, out) = rocky(project, &l, &["run", "--output", "json"]);
    assert!(ok, "second run failed:\n{out}");
    let (ok, out) = rocky(
        project,
        &l,
        &[
            "run",
            "--model",
            "daily_revenue",
            "--from",
            "2026-01-01",
            "--to",
            "2026-01-03",
            "--output",
            "json",
        ],
    );
    assert!(ok, "partition run failed:\n{out}");

    assert_eq!(
        rows(
            &a,
            "SELECT name, engine FROM system.tables WHERE database = 'rocky_e2e_ch_marts' \
             ORDER BY name",
        )
        .await,
        vec![
            vec!["daily_revenue", "MergeTree"],
            vec!["fct_revenue", "MergeTree"],
            vec!["orders_by_customer", "MergeTree"],
            vec!["orders_log", "MergeTree"],
            vec!["stg_orders", "View"],
        ]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT sorting_key, partition_key FROM system.tables \
             WHERE database = 'rocky_e2e_ch_marts' AND name = 'daily_revenue'",
        )
        .await,
        vec![vec!["order_date", "toYYYYMM(order_date)"]]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT customer_name, toString(total) FROM rocky_e2e_ch_marts.fct_revenue ORDER BY 1"
        )
        .await,
        vec![vec!["ann", "31"], vec!["bob", "5"], vec!["cy", "7"]]
    );
    // delete_insert replaced customer 1's rows (no duplicates) and added 3.
    assert_eq!(
        rows(
            &a,
            "SELECT toString(order_id), toString(amount) FROM rocky_e2e_ch_marts.orders_by_customer \
             ORDER BY order_id"
        )
        .await,
        vec![
            vec!["1", "11"],
            vec!["2", "20"],
            vec!["3", "5"],
            vec!["4", "7"]
        ]
    );
    // Append-only incremental: each order once, the newer one appended.
    assert_eq!(
        rows(
            &a,
            "SELECT toString(order_id) FROM rocky_e2e_ch_marts.orders_log ORDER BY order_id"
        )
        .await,
        vec![vec!["1"], vec!["2"], vec!["3"], vec!["4"]]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT toString(order_date), toString(revenue) FROM rocky_e2e_ch_marts.daily_revenue \
             ORDER BY 1"
        )
        .await,
        vec![
            vec!["2026-01-01", "11"],
            vec!["2026-01-02", "25"],
            vec!["2026-01-03", "7"],
        ]
    );
    // No staging table lingers.
    assert!(
        rows(
            &a,
            "SELECT name FROM system.tables WHERE database = 'rocky_e2e_ch_marts' \
             AND name LIKE '%rocky_stage%'"
        )
        .await
        .is_empty()
    );

    exec(
        &a,
        &[
            "DROP DATABASE rocky_e2e_ch_marts",
            "DROP DATABASE rocky_e2e_ch_raw",
        ],
    )
    .await;
}

/// `merge` on a ClickHouse-only project fails `rocky compile` with E053
/// before anything reaches the server.
#[tokio::test]
async fn merge_is_refused_before_any_ddl() {
    let Some(l) = live() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path();
    let models = project.join("models");
    fs::create_dir(&models).unwrap();
    fs::write(
        project.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"clickhouse\"\nhost = \"{}\"\nusername = \"{}\"\n\
             password = \"${{ROCKY_E2E_CH_PASSWORD}}\"\n\n\
             [pipeline.ch]\ntype = \"transformation\"\nmodels = \"models/**\"\ntarget = {{ adapter = \"default\" }}\n",
            l.host, l.user
        ),
    )
    .unwrap();
    model(
        &models,
        "dim_x",
        "SELECT toInt64(1) AS id",
        "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n\n[target]\ncatalog = \"\"\nschema = \"rocky_e2e_ch_merge\"\n",
    );
    let (ok, out) = rocky(project, &l, &["compile", "--output", "json"]);
    assert!(!ok, "compile must fail:\n{out}");
    assert!(out.contains("E053"), "{out}");
    let (ok, out) = rocky(project, &l, &["run", "--output", "json"]);
    assert!(!ok, "run must fail:\n{out}");
    let a = adapter(&l);
    assert!(
        rows(
            &a,
            "SELECT name FROM system.databases WHERE name = 'rocky_e2e_ch_merge'"
        )
        .await
        .is_empty(),
        "no database may be created for a refused model"
    );
}
