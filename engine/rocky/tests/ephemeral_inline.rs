//! `type = "ephemeral"` end to end on DuckDB: the model is inlined as a CTE
//! into its consumers, never materialized, and refused (E038) when selected
//! directly.
//!
//! Chain: `raw.orders` (seeded) → `eph_orders` (ephemeral) → `fct` (table),
//! plus a second consumer `fct_count` so one ephemeral model feeds two.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
type = "transformation"
models = "models"

[pipeline.probe.target.governance]
auto_create_schemas = true
"#;

fn sidecar(strategy: &str) -> String {
    format!(
        "[strategy]\ntype = \"{strategy}\"\n\n[target]\ncatalog = \"probe\"\nschema = \"main\"\n"
    )
}

fn project() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();

    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    conn.execute_batch(
        "CREATE SCHEMA raw;
         CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, \
           status VARCHAR, order_date DATE);
         INSERT INTO raw.orders VALUES
           (1, 10, 5.0, 'paid', DATE '2024-01-01'),
           (2, 10, 7.0, 'paid', DATE '2024-01-02'),
           (3, 20, 9.0, 'refunded', DATE '2024-01-03'),
           (4, 20, 11.0, 'paid', DATE '2024-01-04');",
    )
    .unwrap();
    drop(conn);

    let write = |name: &str, sql: &str, strategy: &str| {
        fs::write(models.join(format!("{name}.sql")), sql).unwrap();
        fs::write(models.join(format!("{name}.toml")), sidecar(strategy)).unwrap();
    };
    write(
        "eph_orders",
        "SELECT order_id, customer_id, amount FROM raw.orders WHERE status = 'paid'\n",
        "ephemeral",
    );
    // Qualified column through the ephemeral model's name: the inliner keeps
    // the name as the alias so this still binds.
    write(
        "fct",
        "SELECT eph_orders.customer_id, SUM(eph_orders.amount) AS total \
         FROM eph_orders GROUP BY eph_orders.customer_id\n",
        "full_refresh",
    );
    // An existing WITH clause: the ephemeral CTE is merged in front of it.
    write(
        "fct_count",
        "WITH paid AS (SELECT order_id FROM eph_orders) SELECT COUNT(*) AS n FROM paid\n",
        "full_refresh",
    );
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("rocky must launch")
}

fn json(out: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body = stdout
        .lines()
        .skip_while(|l| !l.trim_start().starts_with('{'))
        .collect::<Vec<_>>()
        .join("\n");
    serde_json::from_str(&body).unwrap_or_else(|e| {
        panic!(
            "output must be JSON ({e})\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn relation_exists(root: &Path, table: &str) -> bool {
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

#[test]
fn run_inlines_the_ephemeral_model_and_never_materializes_it() {
    let tmp = project();
    let root = tmp.path();

    let out = rocky(root, &["run", "--output", "json"]);
    assert!(
        out.status.success(),
        "run must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    let materialized: Vec<String> = v["materializations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            m["asset_key"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(
        materialized.contains(&"fct".to_string()),
        "{materialized:?}"
    );
    assert!(
        materialized.contains(&"fct_count".to_string()),
        "{materialized:?}"
    );
    assert!(
        !materialized.contains(&"eph_orders".to_string()),
        "an ephemeral model is never reported as materialized: {materialized:?}"
    );

    assert!(
        !relation_exists(root, "eph_orders"),
        "no eph_orders relation in the warehouse"
    );

    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let mut stmt = conn
        .prepare("SELECT customer_id, total FROM main.fct ORDER BY customer_id")
        .unwrap();
    let rows: Vec<(i64, f64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        rows,
        vec![(10, 12.0), (20, 11.0)],
        "only paid orders, summed"
    );
    let n: i64 = conn
        .query_row("SELECT n FROM main.fct_count", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);
}

#[test]
fn run_dag_skips_the_ephemeral_node_and_builds_its_consumers() {
    let tmp = project();
    let root = tmp.path();
    let out = rocky(root, &["run", "--dag", "--output", "json"]);
    assert!(
        out.status.success(),
        "run --dag must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    assert_eq!(v["failed"], 0, "{v}");
    assert!(!relation_exists(root, "eph_orders"));
    assert!(relation_exists(root, "fct"));
}

#[test]
fn compile_and_emit_sql_show_the_inlined_cte() {
    let tmp = project();
    let root = tmp.path();

    let out = rocky(
        root,
        &[
            "compile",
            "--models",
            "models",
            "--expand-macros",
            "--output",
            "json",
        ],
    );
    assert!(
        out.status.success(),
        "compile must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    let fct = v["expanded_sql"]["fct"].as_str().expect("fct expanded SQL");
    assert!(fct.contains("__rocky_ephemeral__eph_orders"), "{fct}");
    assert!(fct.contains("AS eph_orders"), "{fct}");

    let out = rocky(root, &["emit-sql", "--models", "models"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "emit-sql must exit 0\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("WITH __rocky_ephemeral__eph_orders AS"),
        "{stdout}"
    );
    assert!(
        !stdout.contains("TABLE probe.main.eph_orders") && !stdout.contains("main.eph_orders AS"),
        "no statement materializes the ephemeral model:\n{stdout}"
    );
}

#[test]
fn selecting_an_ephemeral_model_directly_is_e038() {
    let tmp = project();
    let root = tmp.path();
    let out = rocky(root, &["run", "--model", "eph_orders", "--output", "json"]);
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("E038"), "{all}");
    assert!(!relation_exists(root, "eph_orders"));
}

/// `--select` that resolves to only an ephemeral model (e.g. a
/// `state:modified` run where only the ephemeral SQL changed) is "nothing to
/// do", not E038: an ephemeral model is never built on its own. Only the
/// literal `--model <ephemeral>` above refuses.
#[test]
fn selecting_only_an_ephemeral_model_is_nothing_to_do() {
    let tmp = project();
    let root = tmp.path();
    let out = rocky(root, &["run", "--select", "eph_orders", "--output", "json"]);
    assert!(
        out.status.success(),
        "run must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    assert_eq!(
        v["materializations"].as_array().map(Vec::len),
        Some(0),
        "{v:#}"
    );
    assert!(!relation_exists(root, "eph_orders"));
    assert!(!relation_exists(root, "fct"));

    // An ephemeral model next to one buildable model collapses to that
    // model alone and builds it.
    let out = rocky(
        root,
        &["run", "--select", "eph_orders", "fct", "--output", "json"],
    );
    assert!(
        out.status.success(),
        "run must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(relation_exists(root, "fct"));
    assert!(!relation_exists(root, "eph_orders"));
}

/// The SQL lints that `rocky compile` runs after typecheck (E042/E043
/// operand checks, P001 portability) judge each model as authored. An
/// ephemeral model's defect is reported once, on the ephemeral model — not
/// again on every consumer that inlines it.
#[test]
fn ephemeral_defects_are_reported_once_not_on_every_consumer() {
    let tmp = project();
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/seed.sql"),
        "CREATE SCHEMA IF NOT EXISTS raw;
         CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, \
           status VARCHAR, order_date DATE);",
    )
    .unwrap();
    // SUM over a VARCHAR column (E042 on DuckDB) and NVL (P001 for BigQuery).
    fs::write(
        models.join("eph_bad.sql"),
        "SELECT order_id, SUM(status) AS s, NVL(amount, 0) AS a FROM raw.orders \
         GROUP BY order_id, amount\n",
    )
    .unwrap();
    fs::write(models.join("eph_bad.toml"), sidecar("ephemeral")).unwrap();
    for consumer in ["c1", "c2"] {
        fs::write(
            models.join(format!("{consumer}.sql")),
            "SELECT order_id, s, a FROM eph_bad\n",
        )
        .unwrap();
        fs::write(
            models.join(format!("{consumer}.toml")),
            sidecar("full_refresh"),
        )
        .unwrap();
    }

    let per_code = |args: &[&str], code: &str| -> Vec<String> {
        let out = rocky(root, args);
        let v = json(&out);
        v["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["code"] == code)
            .map(|d| d["model"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(
        per_code(&["compile", "--with-seed", "--output", "json"], "E042"),
        vec!["eph_bad".to_string()]
    );
    assert_eq!(
        per_code(
            &[
                "compile",
                "--with-seed",
                "--target-dialect",
                "bq",
                "--output",
                "json"
            ],
            "P001"
        ),
        vec!["eph_bad".to_string()]
    );
}
