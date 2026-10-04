//! End-to-end `rocky run` against a live PostgreSQL.
//!
//! Skipped unless `ROCKY_POSTGRES_TEST_HOST` is set (`host` or `host:port`);
//! also reads `ROCKY_POSTGRES_TEST_DB` (default `postgres`),
//! `ROCKY_POSTGRES_TEST_USER` (default `postgres`) and
//! `ROCKY_POSTGRES_TEST_PASSWORD`. Seeds its own schemas
//! (`rocky_e2e_raw`, `rocky_e2e_marts`) through the adapter, then drives
//! the real binary over a transformation pipeline with one model per
//! materialization: view, full_refresh (reading the view), merge,
//! time_interval, materialized_view and delete_insert. A second run after a
//! source change proves idempotency and the merge upsert.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use rocky_core::traits::WarehouseAdapter;
use rocky_postgres::{Flavor, PgConfig, PostgresWarehouseAdapter};

struct Live {
    host: String,
    db: String,
    user: String,
    password: Option<String>,
}

fn live() -> Option<Live> {
    Some(Live {
        host: std::env::var("ROCKY_POSTGRES_TEST_HOST").ok()?,
        db: std::env::var("ROCKY_POSTGRES_TEST_DB").unwrap_or_else(|_| "postgres".into()),
        user: std::env::var("ROCKY_POSTGRES_TEST_USER").unwrap_or_else(|_| "postgres".into()),
        password: std::env::var("ROCKY_POSTGRES_TEST_PASSWORD").ok(),
    })
}

fn adapter(l: &Live) -> PostgresWarehouseAdapter {
    let cfg = PgConfig::new(
        Flavor::Postgres,
        Some(&l.host),
        Some(&l.db),
        Some(&l.user),
        l.password.as_deref(),
        Duration::from_secs(60),
    )
    .expect("config");
    PostgresWarehouseAdapter::from_config(cfg, false).expect("adapter")
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
        .env(
            "ROCKY_E2E_PG_PASSWORD",
            l.password.clone().unwrap_or_default(),
        )
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

async fn rows(a: &PostgresWarehouseAdapter, sql: &str) -> Vec<Vec<String>> {
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

#[tokio::test]
async fn transformation_pipeline_runs_every_materialization() {
    let Some(l) = live() else {
        return;
    };
    let a = adapter(&l);
    a.execute_statement(
        "DROP SCHEMA IF EXISTS rocky_e2e_raw CASCADE; DROP SCHEMA IF EXISTS rocky_e2e_marts CASCADE; \
         CREATE SCHEMA rocky_e2e_raw; \
         CREATE TABLE rocky_e2e_raw.orders (order_id BIGINT, customer_id BIGINT, \
           amount DOUBLE PRECISION, status VARCHAR, order_date DATE NOT NULL); \
         CREATE TABLE rocky_e2e_raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR); \
         INSERT INTO rocky_e2e_raw.orders VALUES (1,1,10,'paid','2026-01-01'), \
           (2,1,20,'paid','2026-01-02'), (3,2,5,'open','2026-01-02'); \
         INSERT INTO rocky_e2e_raw.customers VALUES (1,'ann','a@x.io'), (2,'bob','b@x.io')",
    )
    .await
    .expect("seed");

    let tmp = tempfile::tempdir().unwrap();
    let project = tmp.path();
    let models = project.join("models");
    fs::create_dir(&models).unwrap();
    fs::write(
        project.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"postgres\"\nhost = \"{}\"\ndatabase = \"{}\"\nusername = \"{}\"\n\
             password = \"${{ROCKY_E2E_PG_PASSWORD}}\"\n\n\
             [pipeline.pg]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.pg.target.governance]\nauto_create_schemas = true\n",
            l.host, l.db, l.user
        ),
    )
    .unwrap();
    fs::write(
        models.join("_defaults.toml"),
        format!(
            "[target]\ncatalog = \"{}\"\nschema = \"rocky_e2e_marts\"\n",
            l.db
        ),
    )
    .unwrap();
    model(
        &models,
        "stg_orders",
        "SELECT order_id, customer_id, amount, status, order_date FROM rocky_e2e_raw.orders",
        "[strategy]\ntype = \"view\"\n",
    );
    model(
        &models,
        "fct_revenue",
        "SELECT c.customer_name, SUM(o.amount) AS total FROM rocky_e2e_marts.stg_orders o \
         JOIN rocky_e2e_raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
        "depends_on = [\"stg_orders\"]\n",
    );
    model(
        &models,
        "dim_customers",
        "SELECT customer_id, customer_name, email FROM rocky_e2e_raw.customers",
        "[strategy]\ntype = \"merge\"\nunique_key = [\"customer_id\"]\n\
         update_columns = [\"customer_name\", \"email\"]\n",
    );
    model(
        &models,
        "daily_revenue",
        "SELECT order_date, SUM(amount) AS revenue FROM rocky_e2e_raw.orders \
         WHERE order_date >= @start_date AND order_date < @end_date GROUP BY order_date",
        "[strategy]\ntype = \"time_interval\"\ntime_column = \"order_date\"\ngranularity = \"day\"\n\
         first_partition = \"2026-01-01\"\n",
    );
    model(
        &models,
        "status_mv",
        "SELECT status, COUNT(*) AS n FROM rocky_e2e_raw.orders GROUP BY status",
        "[strategy]\ntype = \"materialized_view\"\n",
    );
    model(
        &models,
        "orders_by_customer",
        "SELECT order_id, customer_id FROM rocky_e2e_raw.orders",
        "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"customer_id\"]\n",
    );

    let (ok, out) = rocky(project, &l, &["run", "--output", "json"]);
    assert!(ok, "first run failed:\n{out}");

    // Source changes, then a second full run and a partition backfill.
    a.execute_statement(
        "UPDATE rocky_e2e_raw.customers SET email = 'ann@new.io' WHERE customer_id = 1; \
         INSERT INTO rocky_e2e_raw.customers VALUES (3, 'cy', 'c@x.io'); \
         INSERT INTO rocky_e2e_raw.orders VALUES (4, 3, 7, 'paid', '2026-01-03')",
    )
    .await
    .unwrap();
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

    let kinds = rows(
        &a,
        "SELECT c.relname, c.relkind::text FROM pg_class c JOIN pg_namespace n \
         ON n.oid = c.relnamespace WHERE n.nspname = 'rocky_e2e_marts' ORDER BY 1",
    )
    .await;
    assert_eq!(
        kinds,
        vec![
            vec!["daily_revenue", "r"],
            vec!["dim_customers", "r"],
            vec!["fct_revenue", "r"],
            vec!["orders_by_customer", "r"],
            vec!["status_mv", "m"],
            vec!["stg_orders", "v"],
        ]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT customer_id::text, email FROM rocky_e2e_marts.dim_customers ORDER BY 1"
        )
        .await,
        vec![
            vec!["1", "ann@new.io"],
            vec!["2", "b@x.io"],
            vec!["3", "c@x.io"],
        ]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT customer_name, total::text FROM rocky_e2e_marts.fct_revenue ORDER BY 1"
        )
        .await,
        vec![vec!["ann", "30"], vec!["bob", "5"], vec!["cy", "7"]]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT order_date::text, revenue::text FROM rocky_e2e_marts.daily_revenue ORDER BY 1"
        )
        .await,
        vec![
            vec!["2026-01-01", "10"],
            vec!["2026-01-02", "25"],
            vec!["2026-01-03", "7"],
        ]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT status, n::text FROM rocky_e2e_marts.status_mv ORDER BY 1"
        )
        .await,
        vec![vec!["open", "1"], vec!["paid", "3"]]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT count(*)::text FROM rocky_e2e_marts.orders_by_customer"
        )
        .await,
        vec![vec!["4"]]
    );

    a.execute_statement("DROP SCHEMA rocky_e2e_marts CASCADE; DROP SCHEMA rocky_e2e_raw CASCADE")
        .await
        .unwrap();
}
