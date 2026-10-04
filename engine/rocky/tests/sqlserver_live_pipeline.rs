//! End-to-end `rocky run` against a live SQL Server.
//!
//! Skipped unless `ROCKY_SQLSERVER_TEST_HOST` is set (`host` or
//! `host,port`); also reads `ROCKY_SQLSERVER_TEST_DB` (default
//! `rocky_test`, must exist), `ROCKY_SQLSERVER_TEST_USER` (default `sa`)
//! and `ROCKY_SQLSERVER_TEST_PASSWORD`. The server certificate is trusted
//! as-is (a local container's is self-signed). Seeds its own schemas
//! (`rocky_e2e_raw`, `rocky_e2e_marts`) through the adapter, then drives the
//! real binary over a transformation pipeline with one model per supported
//! materialization: view, full_refresh (reading the view, with a CTE),
//! merge, time_interval, delete_insert and incremental (CTE +
//! `@incremental_filter`). A second run after a source change proves
//! idempotency, the merge upsert and the incremental append.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use rocky_core::traits::WarehouseAdapter;
use rocky_sqlserver::{Credentials, SqlServerConfig, SqlServerWarehouseAdapter};

struct Live {
    host: String,
    db: String,
    user: String,
    password: String,
}

fn live() -> Option<Live> {
    Some(Live {
        host: std::env::var("ROCKY_SQLSERVER_TEST_HOST").ok()?,
        db: std::env::var("ROCKY_SQLSERVER_TEST_DB").unwrap_or_else(|_| "rocky_test".into()),
        user: std::env::var("ROCKY_SQLSERVER_TEST_USER").unwrap_or_else(|_| "sa".into()),
        password: std::env::var("ROCKY_SQLSERVER_TEST_PASSWORD").unwrap_or_default(),
    })
}

fn adapter(l: &Live) -> SqlServerWarehouseAdapter {
    let mut extra = std::collections::BTreeMap::new();
    extra.insert("trust_server_certificate".into(), serde_json::json!(true));
    let creds = Credentials {
        username: Some(&l.user),
        password: Some(&l.password),
        ..Credentials::default()
    };
    let cfg = SqlServerConfig::new(
        Some(&l.host),
        Some(&l.db),
        &creds,
        Duration::from_secs(60),
        &extra,
    )
    .expect("config");
    SqlServerWarehouseAdapter::new(cfg).expect("adapter")
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
        .env("ROCKY_E2E_MSSQL_PASSWORD", &l.password)
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

async fn rows(a: &SqlServerWarehouseAdapter, sql: &str) -> Vec<Vec<String>> {
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

/// Drop every view and table in `schema`, then the schema.
async fn drop_schema(a: &SqlServerWarehouseAdapter, schema: &str) {
    a.execute_statement(&format!(
        "DECLARE @sql NVARCHAR(MAX) = N''; \
         SELECT @sql += N'DROP ' + CASE o.type WHEN 'V' THEN N'VIEW ' ELSE N'TABLE ' END \
           + QUOTENAME(s.name) + N'.' + QUOTENAME(o.name) + N'; ' \
         FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id \
         WHERE s.name = N'{schema}' AND o.type IN ('U', 'V') ORDER BY o.type DESC; \
         EXEC(@sql); \
         IF SCHEMA_ID(N'{schema}') IS NOT NULL EXEC(N'DROP SCHEMA [{schema}]');"
    ))
    .await
    .expect("drop schema");
}

#[tokio::test]
async fn transformation_pipeline_runs_every_materialization() {
    let Some(l) = live() else {
        return;
    };
    let a = adapter(&l);
    drop_schema(&a, "rocky_e2e_marts").await;
    drop_schema(&a, "rocky_e2e_raw").await;
    a.execute_statement("EXEC(N'CREATE SCHEMA rocky_e2e_raw')")
        .await
        .expect("schema");
    a.execute_statement(
        "CREATE TABLE rocky_e2e_raw.orders (order_id BIGINT, customer_id BIGINT, \
           amount FLOAT, status NVARCHAR(10), order_date DATE NOT NULL, \
           updated_at DATETIME2(7)); \
         CREATE TABLE rocky_e2e_raw.customers (customer_id BIGINT, customer_name NVARCHAR(20), \
           email NVARCHAR(40)); \
         INSERT INTO rocky_e2e_raw.orders VALUES \
           (1,1,10,N'paid','2026-01-01','2026-01-01 10:00:00.1234567'), \
           (2,1,20,N'paid','2026-01-02','2026-01-02 10:00:00'), \
           (3,2,5,N'open','2026-01-02','2026-01-02 11:00:00'); \
         INSERT INTO rocky_e2e_raw.customers VALUES (1,N'ann',N'a@x.io'), (2,N'bob',N'b@x.io')",
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
            "[adapter]\ntype = \"sqlserver\"\nhost = \"{}\"\ndatabase = \"{}\"\nusername = \"{}\"\n\
             password = \"${{ROCKY_E2E_MSSQL_PASSWORD}}\"\n\n\
             [adapter.extra]\ntrust_server_certificate = true\n\n\
             [pipeline.mssql]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.mssql.target.governance]\nauto_create_schemas = true\n",
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
        "WITH o AS (SELECT customer_id, amount FROM rocky_e2e_marts.stg_orders)\n\
         SELECT c.customer_name, SUM(o.amount) AS total FROM o \
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
        "orders_by_customer",
        "SELECT order_id, customer_id FROM rocky_e2e_raw.orders",
        "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"customer_id\"]\n",
    );
    model(
        &models,
        "orders_incremental",
        "WITH src AS (SELECT order_id, amount, updated_at FROM rocky_e2e_raw.orders)\n\
         SELECT order_id, amount, updated_at FROM src WHERE @incremental_filter",
        "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"updated_at\"\n",
    );

    let (ok, out) = rocky(project, &l, &["run", "--output", "json"]);
    assert!(ok, "first run failed:\n{out}");

    // Source changes, then a second full run and a partition backfill.
    a.execute_statement(
        "UPDATE rocky_e2e_raw.customers SET email = N'ann@new.io' WHERE customer_id = 1; \
         INSERT INTO rocky_e2e_raw.customers VALUES (3, N'cy', N'c@x.io'); \
         INSERT INTO rocky_e2e_raw.orders VALUES (4, 3, 7, N'paid', '2026-01-03', \
           '2026-01-03 09:00:00')",
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
        "SELECT o.name, RTRIM(o.type) FROM sys.objects o JOIN sys.schemas s \
         ON s.schema_id = o.schema_id WHERE s.name = N'rocky_e2e_marts' AND o.type IN ('U', 'V') \
         ORDER BY o.name",
    )
    .await;
    assert_eq!(
        kinds,
        vec![
            vec!["daily_revenue", "U"],
            vec!["dim_customers", "U"],
            vec!["fct_revenue", "U"],
            vec!["orders_by_customer", "U"],
            vec!["orders_incremental", "U"],
            vec!["stg_orders", "V"],
        ],
        "no staging table may survive a full refresh"
    );
    assert_eq!(
        rows(
            &a,
            "SELECT CAST(customer_id AS NVARCHAR(10)), email FROM rocky_e2e_marts.dim_customers ORDER BY 1"
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
            "SELECT customer_name, CAST(total AS NVARCHAR(10)) FROM rocky_e2e_marts.fct_revenue ORDER BY 1"
        )
        .await,
        vec![vec!["ann", "30"], vec!["bob", "5"], vec!["cy", "7"]]
    );
    assert_eq!(
        rows(
            &a,
            "SELECT CAST(order_date AS NVARCHAR(10)), CAST(revenue AS NVARCHAR(10)) \
             FROM rocky_e2e_marts.daily_revenue ORDER BY 1"
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
            "SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM rocky_e2e_marts.orders_by_customer"
        )
        .await,
        vec![vec!["4"]]
    );
    // Incremental: three rows on the first run, the fourth appended once.
    assert_eq!(
        rows(
            &a,
            "SELECT CAST(order_id AS NVARCHAR(10)) FROM rocky_e2e_marts.orders_incremental ORDER BY 1"
        )
        .await,
        vec![vec!["1"], vec!["2"], vec!["3"], vec!["4"]]
    );

    // `--full-refresh` rebuilds the incremental model with every
    // `@incremental_filter` as `(1 = 1)` (T-SQL has no TRUE).
    let (ok, out) = rocky(
        project,
        &l,
        &[
            "run",
            "--model",
            "orders_incremental",
            "--full-refresh",
            "--output",
            "json",
        ],
    );
    assert!(ok, "full-refresh run failed:\n{out}");
    assert_eq!(
        rows(
            &a,
            "SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM rocky_e2e_marts.orders_incremental"
        )
        .await,
        vec![vec!["4"]]
    );

    drop_schema(&a, "rocky_e2e_marts").await;
    drop_schema(&a, "rocky_e2e_raw").await;
}
