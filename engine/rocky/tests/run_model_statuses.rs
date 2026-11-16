//! `rocky run` reports what happened to every model (`model_statuses`), and
//! `rocky run --retry-failed` reruns only the failed and never-built ones
//! (#2616, #2617).
//!
//! The project is the three-model one of `run_policy_gate.rs`:
//!
//! ```text
//!   stg_orders -> customer_orders        status_counts (independent, fails)
//! ```
//!
//! `status_counts` is rejected by DuckDB when it runs, not when it compiles,
//! so the run reaches the executor and stops there.
//!
//! End-to-end through the real binary: the roll-up reads the executor's own
//! state, which no unit test of the output type can see.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.shop]
type = "transformation"
models = "models/**"

[pipeline.shop.target]

[pipeline.shop.target.governance]
auto_create_schemas = true
"#;

const CONTAIN: &str = "\n[resilience]\ncontain_failures = true\n";

const BROKEN_STATUS_COUNTS: &str =
    "select status, count(*) as n from main.raw_orders where sum(order_id) > 0 group by status";
const FIXED_STATUS_COUNTS: &str =
    "select status, count(*) as n from main.raw_orders group by status";

fn write_model(dir: &Path, name: &str, sql: &str) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
             catalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
}

fn project(dir: &Path, extra_config: &str, status_counts_sql: &str) {
    project_with(dir, extra_config, "status_counts", status_counts_sql);
}

/// `failing` is the independent third model. Its name sets the order inside
/// layer 0, which runs alphabetically: `status_counts` sorts before
/// `stg_orders`, so it runs first.
fn project_with(dir: &Path, extra_config: &str, failing: &str, failing_sql: &str) {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE raw_orders (order_id INTEGER NOT NULL, customer_id INTEGER NOT NULL, \
         status VARCHAR NOT NULL);
         INSERT INTO raw_orders VALUES (1, 10, 'shipped'), (2, 10, 'pending'), (3, 11, 'shipped');",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), format!("{CONFIG}{extra_config}")).unwrap();
    write_model(
        dir,
        "stg_orders",
        "select order_id, customer_id, status from main.raw_orders",
    );
    write_model(
        dir,
        "customer_orders",
        "select customer_id, count(*) as orders from stg_orders group by customer_id",
    );
    write_model(dir, failing, failing_sql);
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .env_remove("ROCKY_PRINCIPAL")
        .output()
        .expect("spawn rocky")
}

fn describe(out: &Output) -> String {
    format!(
        "exit {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn json(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON ({e})\n{}", describe(out)))
}

/// `(model, status)` pairs of a run's `model_statuses`, in run order.
fn statuses(v: &serde_json::Value) -> Vec<(String, String)> {
    v["model_statuses"]
        .as_array()
        .unwrap_or_else(|| panic!("no model_statuses in {v}"))
        .iter()
        .map(|s| {
            (
                s["model"].as_str().unwrap().to_string(),
                s["status"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn status_of(v: &serde_json::Value, model: &str) -> String {
    statuses(v)
        .into_iter()
        .find(|(m, _)| m == model)
        .unwrap_or_else(|| panic!("{model} missing from {v}"))
        .1
}

/// The models a run materialized, by table name.
fn built(v: &serde_json::Value) -> Vec<String> {
    let mut tables: Vec<String> = v["materializations"]
        .as_array()
        .expect("materializations")
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
    tables.sort();
    tables
}

#[test]
fn fail_fast_run_reports_failed_and_not_started_models() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), "", BROKEN_STATUS_COUNTS);

    let out = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(
        !out.status.success(),
        "a failing model fails the run\n{}",
        describe(&out)
    );
    let v = json(&out);
    // Layer 0 is `status_counts` then `stg_orders`, in name order, and the
    // run stops at the first failure. So `stg_orders` never starts, and
    // neither does `customer_orders` in layer 1.
    assert_eq!(
        statuses(&v),
        [
            ("status_counts".to_string(), "failed".to_string()),
            ("stg_orders".to_string(), "not_started".to_string()),
            ("customer_orders".to_string(), "not_started".to_string()),
        ]
    );
    for model in ["stg_orders", "customer_orders"] {
        let entry = v["model_statuses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["model"] == model)
            .unwrap();
        assert_eq!(entry["reason"], "run_stopped", "{model}");
    }

    // Text mode: the summary lists the same models and the hint.
    let text = rocky(dir.path(), &["--output", "table", "run"]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(
        stdout.contains("  failed: status_counts"),
        "{}",
        describe(&text)
    );
    assert!(
        stdout.contains("  not started: customer_orders (run stopped after status_counts failed)"),
        "{}",
        describe(&text)
    );
    assert!(
        stdout.contains("`[resilience] contain_failures = true`"),
        "{}",
        describe(&text)
    );
}

#[test]
fn contained_run_builds_independent_models() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), CONTAIN, BROKEN_STATUS_COUNTS);

    let out = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(status_of(&v, "stg_orders"), "built");
    assert_eq!(status_of(&v, "customer_orders"), "built");
    assert_eq!(status_of(&v, "status_counts"), "failed");
}

#[test]
fn retry_failed_reruns_only_failed_and_unbuilt_models() {
    let dir = tempfile::tempdir().unwrap();
    // `zz_counts` sorts after `stg_orders`, so layer 0 builds `stg_orders`
    // first and the run stops on `zz_counts` before layer 1.
    project_with(dir.path(), "", "zz_counts", BROKEN_STATUS_COUNTS);
    let first = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!first.status.success(), "{}", describe(&first));
    let first = json(&first);
    assert_eq!(status_of(&first, "stg_orders"), "built");
    assert_eq!(status_of(&first, "zz_counts"), "failed");
    assert_eq!(status_of(&first, "customer_orders"), "not_started");

    write_model(dir.path(), "zz_counts", FIXED_STATUS_COUNTS);
    let retry = rocky(dir.path(), &["--output", "json", "run", "--retry-failed"]);
    assert!(retry.status.success(), "{}", describe(&retry));
    let retry = json(&retry);
    assert_eq!(
        built(&retry),
        ["customer_orders", "zz_counts"],
        "retry builds the failed and the not-started model, not stg_orders"
    );
    assert!(
        retry.get("model_statuses").is_none(),
        "a retry that builds everything it selected omits model_statuses"
    );

    // The retry succeeded, so there is nothing left to retry.
    let again = rocky(dir.path(), &["--output", "json", "run", "--retry-failed"]);
    assert!(!again.status.success(), "{}", describe(&again));
    assert!(
        String::from_utf8_lossy(&again.stderr).contains("nothing to retry"),
        "{}",
        describe(&again)
    );
}

#[test]
fn retry_failed_refuses_unknown_run_and_conflicting_flags() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), "", BROKEN_STATUS_COUNTS);
    let failed = rocky(dir.path(), &["run"]);
    assert!(!failed.status.success());

    let bogus = rocky(dir.path(), &["run", "--retry-failed", "bogus-id"]);
    assert!(!bogus.status.success(), "{}", describe(&bogus));
    assert!(
        String::from_utf8_lossy(&bogus.stderr).contains("not found"),
        "{}",
        describe(&bogus)
    );

    for other in [
        &["--resume-latest"][..],
        &["--dag"],
        &["--watch"],
        &["--model", "stg_orders"],
        &["--select", "stg_orders"],
    ] {
        let mut args = vec!["run", "--retry-failed"];
        args.extend_from_slice(other);
        let out = rocky(dir.path(), &args);
        assert_eq!(
            out.status.code(),
            Some(2),
            "{args:?} must be a usage error\n{}",
            describe(&out)
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("cannot be used with"),
            "{args:?}\n{}",
            describe(&out)
        );
    }
}

#[test]
fn retry_failed_refuses_a_run_that_succeeded() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), "", FIXED_STATUS_COUNTS);
    let ok = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(ok.status.success(), "{}", describe(&ok));
    let ok_json = json(&ok);
    assert!(
        ok_json.get("model_statuses").is_none(),
        "a fully successful run omits model_statuses: {ok_json}"
    );

    let retry = rocky(dir.path(), &["run", "--retry-failed"]);
    assert!(!retry.status.success(), "{}", describe(&retry));
    assert!(
        String::from_utf8_lossy(&retry.stderr).contains("nothing to retry"),
        "{}",
        describe(&retry)
    );
}
