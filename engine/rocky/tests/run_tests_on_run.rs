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
    write_model_to(dir, name, name, sql, extra_toml);
}

/// A model whose `[target] table` is `table`, not its name.
fn write_model_to(dir: &Path, name: &str, table: &str, sql: &str, extra_toml: &str) {
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
             catalog = \"warehouse\"\nschema = \"main\"\ntable = \"{table}\"\n{extra_toml}"
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
        // Promote resolves its actor from git. Give it a fixed identity and
        // ignore the machine's git config, so the test runs the same on a CI
        // runner with no `user.email` as on a developer's machine.
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_COUNT", "2")
        .env("GIT_CONFIG_KEY_0", "user.email")
        .env("GIT_CONFIG_VALUE_0", "test@rocky.invalid")
        .env("GIT_CONFIG_KEY_1", "user.name")
        .env("GIT_CONFIG_VALUE_1", "rocky-test")
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
    // The branch table was tested and failed on its data: `fail`, not an
    // `error` from reading a table that does not exist.
    assert_eq!(
        test_result(&json(&run), "customer_orders")["status"],
        "fail",
        "{}",
        describe(&run)
    );

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

#[test]
fn a_newer_branch_run_of_other_models_does_not_mask_a_failing_test() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());
    let failing = rocky(
        dir,
        &[
            "--output", "json", "run", "--branch", "pr1", "--tests", "enforce",
        ],
    );
    assert!(!failing.status.success(), "{}", describe(&failing));
    // A newer, successful branch run that does not touch customer_orders.
    let other = rocky(
        dir,
        &[
            "--output",
            "json",
            "run",
            "--branch",
            "pr1",
            "--model",
            "status_counts",
        ],
    );
    assert!(other.status.success(), "{}", describe(&other));

    let refused = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(!refused.status.success(), "{}", describe(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("customer_orders"),
        "{}",
        describe(&refused)
    );
    assert!(plan_texts(dir).is_empty());

    // Fixing the data and recording a passing test clears it.
    write_model(dir, "customer_orders", CLEAN, UNIQUE_ERROR);
    let fixed = rocky(
        dir,
        &[
            "--output", "json", "run", "--branch", "pr1", "--tests", "enforce",
        ],
    );
    assert!(fixed.status.success(), "{}", describe(&fixed));
    let ok = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(ok.status.success(), "{}", describe(&ok));
    assert!(
        !String::from_utf8_lossy(&ok.stderr).contains("WARNING"),
        "{}",
        describe(&ok)
    );
}

#[test]
fn a_test_failure_is_recorded_for_a_model_whose_table_has_another_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    write_model_to(
        dir,
        "customer_orders",
        "orders_by_customer",
        FAN_OUT,
        UNIQUE_ERROR,
    );
    write_model(
        dir,
        "customer_summary",
        "select customer_id from orders_by_customer",
        "",
    );
    let run = rocky(dir, &["--output", "json", "run", "--tests", "enforce"]);
    assert!(!run.status.success(), "{}", describe(&run));
    assert_eq!(status_of(&json(&run), "customer_orders"), "failed");

    // The run record holds the failure, so `--retry-failed` reruns the
    // model. Its SQL is unchanged, so only a recorded failure selects it: a
    // record that called the build a success would skip it.
    let retry = rocky(
        dir,
        &[
            "--output",
            "json",
            "run",
            "--retry-failed",
            "--tests",
            "report",
        ],
    );
    assert!(retry.status.success(), "{}", describe(&retry));
    assert!(
        built(&json(&retry)).contains(&"orders_by_customer".to_string()),
        "{}",
        describe(&retry)
    );
}

#[test]
fn promote_names_an_untested_model_whose_table_has_another_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    write_model_to(
        dir,
        "customer_orders",
        "orders_by_customer",
        FAN_OUT,
        UNIQUE_ERROR,
    );
    write_model(
        dir,
        "customer_summary",
        "select customer_id from orders_by_customer",
        "",
    );
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());
    let run = rocky(dir, &["--output", "json", "run", "--branch", "pr1"]);
    assert!(run.status.success(), "{}", describe(&run));

    let out = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("customer_orders (run "),
        "{}",
        describe(&out)
    );
}

#[test]
fn a_test_that_cannot_run_fails_closed_under_enforce() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    // The column does not exist, so the test query errors.
    project(
        dir,
        "",
        CLEAN,
        "\n[[tests]]\ntype = \"not_null\"\ncolumn = \"no_such_column\"\n",
    );
    let out = rocky(dir, &["--output", "json", "run", "--tests", "enforce"]);
    assert!(!out.status.success(), "{}", describe(&out));
    let v = json(&out);
    assert_eq!(test_result(&v, "customer_orders")["status"], "error", "{v}");
    assert_eq!(status_of(&v, "customer_orders"), "failed", "{v}");
}

#[test]
fn a_recreated_branch_does_not_inherit_the_old_branch_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    project(dir, "", FAN_OUT, UNIQUE_ERROR);
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());
    let failing = rocky(
        dir,
        &[
            "--output", "json", "run", "--branch", "pr1", "--tests", "enforce",
        ],
    );
    assert!(!failing.status.success(), "{}", describe(&failing));
    let deleted = rocky(dir, &["branch", "delete", "pr1"]);
    assert!(deleted.status.success(), "{}", describe(&deleted));
    assert!(rocky(dir, &["branch", "create", "pr1"]).status.success());

    let out = rocky(dir, &["--output", "json", "plan", "promote", "pr1"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no recorded run"),
        "{}",
        describe(&out)
    );
}
