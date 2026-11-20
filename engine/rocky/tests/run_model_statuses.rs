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

const FAIL_FAST: &str = "\n[resilience]\ncontain_failures = false\n";

const CONTAIN: &str = "\n[resilience]\ncontain_failures = true\n";

// The broken models must fail when DuckDB runs them, not at compile time:
// `rocky compile` refuses an aggregate in `WHERE` (E065), so the failure is a
// cast of `'shipped'` to an integer, which DuckDB only raises on a row.
const BROKEN_STATUS_COUNTS: &str = "select status, count(*) as n from main.raw_orders where cast(status as integer) > 0 group by status";
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
    project(dir.path(), FAIL_FAST, BROKEN_STATUS_COUNTS);

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
fn default_config_contains_a_failure_and_exits_partial_failure() {
    let dir = tempfile::tempdir().unwrap();
    // Empty extra config: no `[resilience]` table, so the default applies.
    project(dir.path(), "", BROKEN_STATUS_COUNTS);

    let out = rocky(dir.path(), &["--output", "json", "run"]);
    assert_eq!(out.status.code(), Some(2), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(status_of(&v, "stg_orders"), "built");
    assert_eq!(status_of(&v, "customer_orders"), "built");
    assert_eq!(status_of(&v, "status_counts"), "failed");
}

/// `stg_orders` reads the bare raw table `orders`; the mart model
/// `orders_enriched` writes `marts.orders` and depends on `stg_orders`.
/// Containment resolves the bare `orders` read to `orders_enriched`'s target
/// table, closing a cycle with the declared edge.
fn bare_name_cycle_project(dir: &Path, extra_config: &str) {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE orders (order_id INTEGER NOT NULL);
         INSERT INTO orders VALUES (1), (2);",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), format!("{CONFIG}{extra_config}")).unwrap();
    write_model(dir, "stg_orders", "select order_id from orders");
    let models = dir.join("models");
    fs::write(
        models.join("orders_enriched.toml"),
        "name = \"orders_enriched\"\ndepends_on = [\"stg_orders\"]\n\n\
         [strategy]\ntype = \"full_refresh\"\n\n[target]\n\
         catalog = \"warehouse\"\nschema = \"marts\"\ntable = \"orders\"\n",
    )
    .unwrap();
    fs::write(
        models.join("orders_enriched.sql"),
        "select order_id from stg_orders",
    )
    .unwrap();
}

#[test]
fn bare_name_cycle_is_withheld_under_default_containment() {
    let dir = tempfile::tempdir().unwrap();
    bare_name_cycle_project(dir.path(), "");

    let out = rocky(dir.path(), &["--output", "json", "run"]);
    let v = json(&out);
    // Nothing builds, so this is a plain failure (exit 1), not a partial one.
    assert_eq!(out.status.code(), Some(1), "{}", describe(&out));
    let errors = v["errors"].as_array().expect("errors");
    assert!(
        errors
            .iter()
            .any(|e| e["asset_key"] == serde_json::json!(["<cycle>"])),
        "{}",
        describe(&out)
    );
    assert!(built(&v).is_empty(), "{}", describe(&out));
}

#[test]
fn bare_name_cycle_is_skipped_with_a_warning_under_fail_fast() {
    let dir = tempfile::tempdir().unwrap();
    bare_name_cycle_project(dir.path(), FAIL_FAST);

    let out = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(built(&v), ["orders", "stg_orders"], "{}", describe(&out));
}

#[test]
fn retry_failed_reruns_only_failed_and_unbuilt_models() {
    let dir = tempfile::tempdir().unwrap();
    // `zz_counts` sorts after `stg_orders`, so layer 0 builds `stg_orders`
    // first and the run stops on `zz_counts` before layer 1.
    project_with(dir.path(), FAIL_FAST, "zz_counts", BROKEN_STATUS_COUNTS);
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

const EDITED_STG_ORDERS: &str =
    "select order_id, customer_id, status from main.raw_orders where order_id > 0";

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn retry_rebuilds_a_model_whose_sql_changed_since_it_succeeded() {
    let dir = tempfile::tempdir().unwrap();
    project_with(dir.path(), "", "zz_counts", BROKEN_STATUS_COUNTS);
    let first = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!first.status.success(), "{}", describe(&first));
    assert_eq!(status_of(&json(&first), "stg_orders"), "built");

    // `stg_orders` succeeded in the failed run, but not with this SQL.
    write_model(dir.path(), "stg_orders", EDITED_STG_ORDERS);
    write_model(dir.path(), "zz_counts", FIXED_STATUS_COUNTS);
    let retry = rocky(dir.path(), &["--output", "json", "run", "--retry-failed"]);
    assert!(retry.status.success(), "{}", describe(&retry));
    assert_eq!(
        built(&json(&retry)),
        ["customer_orders", "stg_orders", "zz_counts"],
        "an edited model reruns even though its old build succeeded"
    );
}

#[test]
fn retry_rebuilds_the_downstream_of_a_rebuilt_model() {
    let dir = tempfile::tempdir().unwrap();
    // Contained: `stg_orders` and its dependent `customer_orders` both build,
    // and the independent `zz_counts` fails.
    project_with(dir.path(), CONTAIN, "zz_counts", BROKEN_STATUS_COUNTS);
    let first = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!first.status.success(), "{}", describe(&first));
    let first = json(&first);
    assert_eq!(status_of(&first, "stg_orders"), "built");
    assert_eq!(status_of(&first, "customer_orders"), "built");

    // Editing `stg_orders` leaves `customer_orders` byte-identical, yet it
    // reads the rebuilt table, so it must rebuild too.
    write_model(dir.path(), "stg_orders", EDITED_STG_ORDERS);
    write_model(dir.path(), "zz_counts", FIXED_STATUS_COUNTS);
    let retry = rocky(dir.path(), &["--output", "json", "run", "--retry-failed"]);
    assert!(retry.status.success(), "{}", describe(&retry));
    assert_eq!(
        built(&json(&retry)),
        ["customer_orders", "stg_orders", "zz_counts"]
    );
}

#[test]
fn contained_dependent_of_the_failing_model_is_skipped_upstream_failed() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), CONTAIN, FIXED_STATUS_COUNTS);
    write_model(
        dir.path(),
        "stg_orders",
        "select order_id, customer_id, status from main.raw_orders where cast(status as integer) > 0",
    );
    let out = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(status_of(&v, "stg_orders"), "failed");
    assert_eq!(status_of(&v, "status_counts"), "built");
    let dependent = v["model_statuses"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["model"] == "customer_orders")
        .unwrap();
    assert_eq!(dependent["status"], "skipped");
    assert_eq!(dependent["reason"], "upstream_failed");
    assert_eq!(dependent["blocked_by"], serde_json::json!(["stg_orders"]));
}

#[test]
fn production_retry_ignores_a_newer_failed_shadow_run() {
    let dir = tempfile::tempdir().unwrap();
    project_with(dir.path(), FAIL_FAST, "zz_counts", BROKEN_STATUS_COUNTS);
    let prod = rocky(dir.path(), &["--output", "json", "run"]);
    assert!(!prod.status.success(), "{}", describe(&prod));
    assert_eq!(status_of(&json(&prod), "stg_orders"), "built");

    // A newer shadow run that fails on `stg_orders` instead. If the retry
    // picked it, `stg_orders` would rerun.
    write_model(
        dir.path(),
        "stg_orders",
        "select order_id, customer_id, status from main.raw_orders where cast(status as integer) > 0",
    );
    let shadow = rocky(dir.path(), &["--output", "json", "run", "--shadow"]);
    assert!(!shadow.status.success(), "{}", describe(&shadow));
    write_model(
        dir.path(),
        "stg_orders",
        "select order_id, customer_id, status from main.raw_orders",
    );
    write_model(dir.path(), "zz_counts", FIXED_STATUS_COUNTS);

    let retry = rocky(dir.path(), &["--output", "json", "run", "--retry-failed"]);
    assert!(retry.status.success(), "{}", describe(&retry));
    assert_eq!(
        built(&json(&retry)),
        ["customer_orders", "zz_counts"],
        "the retry follows the production run, not the newer shadow run"
    );

    for flag in [&["--shadow"][..], &["--defer"], &["--defer-to", "x"]] {
        let mut args = vec!["run", "--retry-failed"];
        args.extend_from_slice(flag);
        let out = rocky(dir.path(), &args);
        assert_eq!(out.status.code(), Some(2), "{args:?}\n{}", describe(&out));
    }
}

#[test]
fn retry_refuses_a_run_of_another_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), "", BROKEN_STATUS_COUNTS);
    let mut config = fs::read_to_string(dir.path().join("rocky.toml")).unwrap();
    config.push_str(
        "\n[pipeline.other]\ntype = \"transformation\"\nmodels = \"other_models/**\"\n\n\
         [pipeline.other.target]\n\n[pipeline.other.target.governance]\nauto_create_schemas = true\n",
    );
    fs::write(dir.path().join("rocky.toml"), config).unwrap();
    let other_models = dir.path().join("other_models");
    fs::create_dir_all(&other_models).unwrap();
    fs::write(
        other_models.join("solo.toml"),
        "name = \"solo\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
         catalog = \"warehouse\"\nschema = \"main\"\ntable = \"solo\"\n",
    )
    .unwrap();
    fs::write(other_models.join("solo.sql"), "select 1 as one").unwrap();

    let other = rocky(
        dir.path(),
        &["--output", "json", "run", "--pipeline", "other"],
    );
    assert!(other.status.success(), "{}", describe(&other));
    let other_id = json(&other)["run_id"].as_str().map(str::to_string);
    let shop = rocky(
        dir.path(),
        &["--output", "json", "run", "--pipeline", "shop"],
    );
    assert!(!shop.status.success(), "{}", describe(&shop));

    // The run id is not in the JSON; read it back from history.
    let history = rocky(dir.path(), &["--output", "json", "history"]);
    let history = json(&history);
    let other_id = other_id.unwrap_or_else(|| {
        history["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["pipeline"] == "other")
            .expect("the other pipeline's run")["run_id"]
            .as_str()
            .unwrap()
            .to_string()
    });
    let out = rocky(
        dir.path(),
        &["run", "--pipeline", "shop", "--retry-failed", &other_id],
    );
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(
        stderr(&out).contains("is not a production run of pipeline 'shop'"),
        "{}",
        describe(&out)
    );
}

#[test]
fn retry_refuses_a_replication_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("rocky.toml"),
        r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "warehouse"
schema_template = "staging__{source}"
"#,
    )
    .unwrap();
    let out = rocky(dir.path(), &["run", "--retry-failed"]);
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(
        stderr(&out).contains("reruns a transformation pipeline"),
        "{}",
        describe(&out)
    );
}

#[test]
fn retry_refuses_a_remote_state_backend() {
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        "\n[state]\nbackend = \"s3\"\ns3_bucket = \"rocky-test-never-contacted\"\n",
        BROKEN_STATUS_COUNTS,
    );
    let out = rocky(dir.path(), &["run", "--retry-failed"]);
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(
        stderr(&out).contains("run the failed models with --select"),
        "{}",
        describe(&out)
    );
}
