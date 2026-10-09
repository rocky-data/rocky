//! `rocky run --defer --defer-to-state <PATH>` end to end on DuckDB.
//!
//! A production project writes `prod.orders` and `prod.report` and records
//! them in `prod.redb`. A development project configures the same models in
//! schema `dev`. Building only `report` in dev with `--defer-to-state
//! prod.redb` must read `prod.orders` (the table the production run
//! recorded), not `dev.orders`, and must leave the production state file
//! untouched.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn config(db: &Path) -> String {
    format!(
        "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
         [pipeline.transform]\ntype = \"transformation\"\nmodels = \"models\"\n\n\
         [pipeline.transform.target.governance]\nauto_create_schemas = true\n",
        db.display()
    )
}

fn write_model(dir: &Path, name: &str, sql: &str, schema: &str) {
    fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"{schema}\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
}

/// One project directory: `orders` holds `amount`, `report` reads `orders`.
fn project(root: &Path, env: &str, amount: u32, report_from: &str, db: &Path) {
    let models = root.join(env).join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(root.join(env).join("rocky.toml"), config(db)).unwrap();
    write_model(
        &models,
        "orders",
        &format!("SELECT 1 AS id, {amount} AS amount"),
        env,
    );
    write_model(
        &models,
        "report",
        &format!("SELECT SUM(amount) AS total FROM {report_from}"),
        env,
    );
}

fn rocky(dir: &Path, state: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "--state-path"])
        .arg(state)
        .args(args)
        .current_dir(dir)
        .output()
        .expect("rocky must launch")
}

fn describe(out: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn total(db: &Path, table: &str) -> Option<i64> {
    let conn = duckdb::Connection::open(db).unwrap();
    conn.query_row(&format!("SELECT total FROM {table}"), [], |r| r.get(0))
        .ok()
}

#[test]
fn defer_to_state_reads_the_table_the_production_run_recorded() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let db = root.join("shared.duckdb");
    // Production reads its upstream by qualified name; dev reads it bare, so
    // only a deferral can point dev's `report` at production's `orders`.
    project(root, "prod", 100, "prod.orders", &db);
    project(root, "dev", 1, "orders", &db);
    {
        let conn = duckdb::Connection::open(&db).unwrap();
        conn.execute_batch("CREATE SCHEMA prod; CREATE SCHEMA dev;")
            .unwrap();
    }
    let prod_dir = root.join("prod");
    let dev_dir = root.join("dev");
    let prod_state = root.join("prod.redb");

    let out = rocky(
        &prod_dir,
        &prod_state,
        &["run", "--pipeline", "transform", "-o", "json"],
    );
    assert!(out.status.success(), "production run: {}", describe(&out));
    assert_eq!(total(&db, "prod.report"), Some(100));

    // A state that never built `orders` refuses, naming the upstream, and
    // writes nothing.
    let dev_only_state = root.join("dev-only.redb");
    let out = rocky(
        &dev_dir,
        &root.join("dev-a.redb"),
        &[
            "run",
            "--pipeline",
            "transform",
            "--model",
            "report",
            "--defer",
            "--defer-to-state",
            prod_state.to_str().unwrap(),
            "--defer-run-id",
            "no-such-run",
            "-o",
            "json",
        ],
    );
    assert!(!out.status.success(), "unknown run id: {}", describe(&out));
    assert!(
        describe(&out).contains("holds no run 'no-such-run'"),
        "{}",
        describe(&out)
    );
    // Build `report` alone in dev without deferral: it fails (no
    // `main.orders`), so this state records no execution of `orders`.
    let _ = rocky(
        &dev_dir,
        &dev_only_state,
        &[
            "run",
            "--pipeline",
            "transform",
            "--model",
            "report",
            "-o",
            "json",
        ],
    );
    let out = rocky(
        &dev_dir,
        &root.join("dev-b.redb"),
        &[
            "run",
            "--pipeline",
            "transform",
            "--model",
            "report",
            "--defer",
            "--defer-to-state",
            dev_only_state.to_str().unwrap(),
            "-o",
            "json",
        ],
    );
    assert!(
        !out.status.success(),
        "state lacks upstream: {}",
        describe(&out)
    );
    assert!(
        describe(&out).contains("reads upstream 'orders'"),
        "{}",
        describe(&out)
    );
    assert_eq!(total(&db, "dev.report"), None, "a refusal writes nothing");

    let prod_state_before = fs::read(&prod_state).unwrap();
    let out = rocky(
        &dev_dir,
        &root.join("dev-c.redb"),
        &[
            "run",
            "--pipeline",
            "transform",
            "--model",
            "report",
            "--defer",
            "--defer-to-state",
            prod_state.to_str().unwrap(),
            "-o",
            "json",
        ],
    );
    assert!(out.status.success(), "deferred dev run: {}", describe(&out));
    assert_eq!(
        total(&db, "dev.report"),
        Some(100),
        "dev.report must read prod.orders (100), not dev.orders"
    );
    assert_eq!(
        fs::read(&prod_state).unwrap(),
        prod_state_before,
        "the production state file is opened read-only"
    );

    // `--defer-to <SCHEMA>` keeps working.
    let out = rocky(
        &dev_dir,
        &root.join("dev-d.redb"),
        &[
            "run",
            "--pipeline",
            "transform",
            "--model",
            "report",
            "--defer",
            "--defer-to",
            "prod",
            "-o",
            "json",
        ],
    );
    assert!(out.status.success(), "--defer-to: {}", describe(&out));
    assert_eq!(total(&db, "dev.report"), Some(100));
}

#[test]
fn defer_to_state_and_defer_to_are_mutually_exclusive() {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args([
            "run",
            "--model",
            "report",
            "--defer",
            "--defer-to",
            "prod",
            "--defer-to-state",
            "prod.redb",
        ])
        .output()
        .expect("rocky must launch");
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("cannot be used with"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
