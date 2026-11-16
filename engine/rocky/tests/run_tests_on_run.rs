//! `rocky run` evaluates a model's `[[tests]]` when asked to (#2611), and
//! `rocky plan promote` refuses a branch whose newest run recorded a failing
//! one (#2609).
//!
//! The project is the three-model one of `run_policy_gate.rs` plus a
//! downstream of `customer_orders`:
//!
//! ```text
//!   stg_orders -> customer_orders -> customer_summary      status_counts
//! ```
//!
//! `customer_orders` fans out: it joins `raw_orders` a second time and groups
//! by order, so `customer_id` repeats and its `unique` test fails.
//!
//! End-to-end through the real binary: the wiring between the run loop, the
//! run record and the promote gate is what no unit test can see.

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

const ENFORCE_IN_CONFIG: &str = "\n[run]\ntests_on_run = \"enforce\"\n";

const FAN_OUT: &str = "select o.customer_id, count(*) as orders from stg_orders o \
                       join main.raw_orders r on o.customer_id = r.customer_id \
                       group by o.customer_id, r.order_id";
const CLEAN: &str = "select customer_id, count(*) as orders from stg_orders group by customer_id";

const UNIQUE_ERROR: &str = "\n[[tests]]\ntype = \"unique\"\ncolumn = \"customer_id\"\n";
const UNIQUE_WARNING: &str =
    "\n[[tests]]\ntype = \"unique\"\ncolumn = \"customer_id\"\nseverity = \"warning\"\n";

fn write_model(dir: &Path, name: &str, sql: &str, extra_toml: &str) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
             catalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n{extra_toml}"
        ),
    )
    .unwrap();
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
}

/// The project, with `customer_orders` declaring `tests` and selecting
/// `customer_sql`. `extra_config` is appended to `rocky.toml`.
fn project(dir: &Path, extra_config: &str, customer_sql: &str, tests: &str) {
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
        "",
    );
    write_model(dir, "customer_orders", customer_sql, tests);
    write_model(
        dir,
        "customer_summary",
        "select customer_id from customer_orders",
        "",
    );
    write_model(
        dir,
        "status_counts",
        "select status, count(*) as n from main.raw_orders group by status",
        "",
    );
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

fn status_of(v: &serde_json::Value, model: &str) -> String {
    v["model_statuses"]
        .as_array()
        .unwrap_or_else(|| panic!("no model_statuses in {v}"))
        .iter()
        .find(|s| s["model"] == model)
        .unwrap_or_else(|| panic!("{model} missing from model_statuses of {v}"))["status"]
        .as_str()
        .unwrap()
        .to_string()
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

fn test_result<'a>(v: &'a serde_json::Value, model: &str) -> &'a serde_json::Value {
    v["test_results"]
        .as_array()
        .unwrap_or_else(|| panic!("no test_results in {v}"))
        .iter()
        .find(|t| t["model"] == model)
        .unwrap_or_else(|| panic!("no test result for {model} in {v}"))
}

#[test]
fn off_by_default_runs_no_test_and_adds_no_output_field() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), "", FAN_OUT, UNIQUE_ERROR);
    let out = rocky(tmp.path(), &["--output", "json", "run"]);
    assert!(out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert!(v.get("test_results").is_none(), "{v}");
    assert_eq!(built(&v).len(), 4, "every model builds: {v}");
}

#[test]
fn report_records_the_failure_and_never_fails_the_run() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), "", FAN_OUT, UNIQUE_ERROR);
    let out = rocky(
        tmp.path(),
        &["--output", "json", "run", "--tests", "report"],
    );
    assert!(out.status.success(), "{}", describe(&out));
    let v = json(&out);
    let t = test_result(&v, "customer_orders");
    assert_eq!(t["test_type"], "unique", "{v}");
    assert_eq!(t["column"], "customer_id", "{v}");
    assert_eq!(t["status"], "fail", "{v}");
    assert_eq!(t["severity"], "error", "{v}");
    assert_eq!(built(&v).len(), 4, "report never stops a model: {v}");
}

#[test]
fn enforce_flag_fails_the_model_and_withholds_its_downstream() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), "", FAN_OUT, UNIQUE_ERROR);
    let out = rocky(
        tmp.path(),
        &["--output", "json", "run", "--tests", "enforce"],
    );
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(status_of(&v, "customer_orders"), "failed", "{v}");
    assert!(
        !built(&v).contains(&"customer_summary".to_string()),
        "the downstream must not build: {v}"
    );
    assert_eq!(test_result(&v, "customer_orders")["status"], "fail", "{v}");
    // The bad table is not rolled back.
    let conn = duckdb::Connection::open(tmp.path().join("warehouse.duckdb")).unwrap();
    let n: i64 = conn
        .query_row("select count(*) from main.customer_orders", [], |r| {
            r.get(0)
        })
        .expect("the table the model wrote stays in place");
    assert!(n > 0);
}

#[test]
fn enforce_from_config_behaves_like_the_flag() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), ENFORCE_IN_CONFIG, FAN_OUT, UNIQUE_ERROR);
    let out = rocky(tmp.path(), &["--output", "json", "run"]);
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(status_of(&v, "customer_orders"), "failed", "{v}");

    // The flag wins over the config.
    let out = rocky(tmp.path(), &["--output", "json", "run", "--tests", "off"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert!(json(&out).get("test_results").is_none());
}

#[test]
fn a_passing_test_under_enforce_builds_everything() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), "", CLEAN, UNIQUE_ERROR);
    let out = rocky(
        tmp.path(),
        &["--output", "json", "run", "--tests", "enforce"],
    );
    assert!(out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(test_result(&v, "customer_orders")["status"], "pass", "{v}");
    assert_eq!(built(&v).len(), 4, "{v}");
}

#[test]
fn a_warning_severity_failure_never_fails_a_model() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), "", FAN_OUT, UNIQUE_WARNING);
    let out = rocky(
        tmp.path(),
        &["--output", "json", "run", "--tests", "enforce"],
    );
    assert!(out.status.success(), "{}", describe(&out));
    let v = json(&out);
    let t = test_result(&v, "customer_orders");
    assert_eq!(t["status"], "fail", "{v}");
    assert_eq!(t["severity"], "warning", "{v}");
    assert_eq!(built(&v).len(), 4, "{v}");
}

/// Plan files `rocky plan promote` wrote, as text.
fn plan_texts(dir: &Path) -> Vec<String> {
    let plans = dir.join(".rocky").join("plans");
    fs::read_dir(plans)
        .map(|rd| {
            rd.map(|e| fs::read_to_string(e.unwrap().path()).unwrap())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn promote_refuses_a_branch_whose_newest_run_failed_a_test() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());
    let run = rocky(
        dir,
        &[
            "--output", "json", "run", "--branch", "pr1", "--tests", "enforce",
        ],
    );
    assert!(!run.status.success(), "{}", describe(&run));

    let refused = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(!refused.status.success(), "{}", describe(&refused));
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("blocked by 1 failing test"),
        "{}",
        describe(&refused)
    );
    assert!(stderr.contains("customer_orders"), "{stderr}");
    assert!(stderr.contains("--allow-failing-tests"), "{stderr}");
    assert!(
        plan_texts(dir).is_empty(),
        "a refused promote writes no plan"
    );

    let allowed = rocky(
        dir,
        &[
            "--output",
            "json",
            "plan",
            "promote",
            "pr1",
            "--allow-failing-tests",
        ],
    );
    assert!(allowed.status.success(), "{}", describe(&allowed));
    assert!(
        String::from_utf8_lossy(&allowed.stderr).contains("--allow-failing-tests was set"),
        "{}",
        describe(&allowed)
    );
    let plans = plan_texts(dir);
    assert_eq!(plans.len(), 1, "{plans:?}");
    let plan: serde_json::Value = serde_json::from_str(&plans[0]).unwrap();
    assert!(plans[0].contains("failing_tests_allowed"), "{}", plans[0]);
    assert!(!plans[0].contains("failing_tests_blocked"));
    let _ = plan;
}

#[test]
fn promote_warns_but_proceeds_when_the_newest_run_recorded_no_tests() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());
    // Tests off: the run records none.
    let run = rocky(dir, &["--output", "json", "run", "--branch", "pr1"]);
    assert!(run.status.success(), "{}", describe(&run));

    let out = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(out.status.success(), "{}", describe(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("WARNING"), "{}", describe(&out));
    assert!(stderr.contains("customer_orders"), "{}", describe(&out));
    assert!(plan_texts(dir)[0].contains("test_evidence_missing"));
}

#[test]
fn promote_warns_but_proceeds_when_the_branch_has_no_run() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());

    let out = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(out.status.success(), "{}", describe(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("no recorded run"), "{}", describe(&out));
    assert!(plan_texts(dir)[0].contains("test_evidence_missing"));
}
