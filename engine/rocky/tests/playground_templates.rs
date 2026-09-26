//! Every `rocky playground` template builds its models on the first
//! `rocky run` (#1983).
//!
//! Before the fix, `ecommerce` and `showcase` scaffolded a replication
//! pipeline over an in-memory DuckDB with an unloaded seed. `rocky run`
//! copied 0 tables and exited 0.
//!
//! ```text
//!   playground --template T  ->  compile  ->  test  ->  run  ->  run again
//!                                                       N tables in warehouse.main
//! ```

use std::path::Path;
use std::process::{Command, Output};

fn rocky(cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(args)
        .current_dir(cwd)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed; stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn run_models(project: &Path) -> usize {
    let out = rocky(project, &["--output", "json", "run"]);
    assert_ok(&out, "rocky run");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "run stdout is not JSON ({e}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    json["materializations"]
        .as_array()
        .unwrap_or_else(|| panic!("no materializations: {json}"))
        .len()
}

fn tables_in_main(project: &Path, db_file: &str) -> i64 {
    let conn = duckdb::Connection::open(project.join(db_file)).expect("open duckdb");
    conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main'",
        [],
        |r| r.get(0),
    )
    .expect("count tables")
}

fn scaffold_compile_test_run(template: &str, db_file: &str, models: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let scaffold = rocky(tmp.path(), &["playground", "demo", "--template", template]);
    assert_ok(&scaffold, "rocky playground");
    let project = tmp.path().join("demo");

    assert_ok(&rocky(&project, &["compile"]), "rocky compile");
    assert_ok(&rocky(&project, &["test"]), "rocky test");

    assert_eq!(run_models(&project), models, "{template}: first run");
    assert_eq!(
        tables_in_main(&project, db_file),
        models as i64,
        "{template}: every model is a table in {db_file}"
    );

    // The second run takes the merge path on the merge models.
    assert_eq!(run_models(&project), models, "{template}: second run");
}

#[test]
fn ecommerce_template_builds_every_model() {
    scaffold_compile_test_run("ecommerce", "warehouse.duckdb", 10);
}

#[test]
fn showcase_template_builds_every_model() {
    scaffold_compile_test_run("showcase", "warehouse.duckdb", 11);
}

#[test]
fn quickstart_template_builds_every_model() {
    scaffold_compile_test_run("quickstart", "playground.duckdb", 3);
}
