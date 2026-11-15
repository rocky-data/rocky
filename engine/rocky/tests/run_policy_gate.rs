//! `[policy]` gates `rocky run` (#2610).
//!
//! An agent under `default_agent_effect = "deny"` must not write through any
//! `rocky run` path, and every refusal must leave a decision that
//! `rocky audit` lists. A human under the same policy is never gated.
//!
//! ```text
//!   path                          agent + deny        human + deny
//!   run                           refused, 0 tables   3 tables
//!   run --branch pr1              refused, 0 tables
//!   run --dag                     refused, 0 tables
//!   run --resume <id> (repl.)     refused by policy
//!   run (replication)             refused, 0 tables
//! ```
//!
//! End-to-end through the real binary: the gap was in the CLI wiring (the
//! `Run` arm never resolved the principal), which no unit test of the
//! evaluator can see.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const DENY_POLICY: &str = "\n[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n";

const TRANSFORMATION: &str = r#"
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

const REPLICATION: &str = r#"
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

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

/// The three-model project of #2609: `stg_orders → customer_orders`, and an
/// independent `status_counts`, over one seeded source table.
fn transformation_project(dir: &Path, policy: &str) {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE TABLE raw_orders (order_id INTEGER NOT NULL, customer_id INTEGER NOT NULL, \
         status VARCHAR NOT NULL);
         INSERT INTO raw_orders VALUES (1, 10, 'shipped'), (2, 10, 'pending'), (3, 11, 'shipped');",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), format!("{TRANSFORMATION}{policy}")).unwrap();
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    for (name, sql) in [
        (
            "stg_orders",
            "select order_id, customer_id, status from main.raw_orders",
        ),
        (
            "customer_orders",
            "select customer_id, count(*) as orders from stg_orders group by customer_id",
        ),
        (
            "status_counts",
            "select status, count(*) as n from main.raw_orders group by status",
        ),
    ] {
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
}

fn replication_project(dir: &Path, policy: &str) {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders AS SELECT 1 AS id, 'a' AS name;",
    )
    .expect("seed source");
    drop(conn);
    fs::write(dir.join("rocky.toml"), format!("{REPLICATION}{policy}")).unwrap();
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

/// Every table in the warehouse that the seed did not create, in any schema.
fn written_tables(dir: &Path) -> Vec<String> {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    let mut stmt = conn
        .prepare(
            "select table_schema || '.' || table_name from information_schema.tables \
             where table_name <> 'raw_orders' and table_schema <> 'raw__orders' order by 1",
        )
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The `run` decisions `rocky audit` lists, as `(model, effect)` pairs.
fn run_decisions(dir: &Path) -> Vec<(String, String)> {
    let out = rocky(dir, &["--output", "json", "audit"]);
    assert!(out.status.success(), "audit must succeed\n{}", describe(&out));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("audit stdout is not JSON ({e})\n{}", describe(&out)));
    v["decisions"]
        .as_array()
        .expect("decisions array")
        .iter()
        .filter(|d| d["capability"] == "run")
        .map(|d| {
            (
                d["model"].as_str().unwrap_or_default().to_string(),
                d["effect"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// Assert an agent run under the deny policy was refused by the policy plane,
/// wrote nothing, and left a `deny` decision in the ledger.
fn assert_denied(dir: &Path, out: &Output, what: &str) {
    assert!(
        !out.status.success(),
        "{what}: a denied agent run must not exit 0\n{}",
        describe(out)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("policy DENIES"),
        "{what}: the refusal must come from the policy plane\n{}",
        describe(out)
    );
    assert_eq!(
        written_tables(dir),
        Vec::<String>::new(),
        "{what}: a denied run must write no table"
    );
    let decisions = run_decisions(dir);
    assert!(
        !decisions.is_empty() && decisions.iter().all(|(_, effect)| effect == "deny"),
        "{what}: the ledger must hold the run's deny decision(s), got {decisions:?}"
    );
}

#[test]
fn deny_denies_an_agent_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert_denied(tmp.path(), &out, "run");
    // One decision per model the run would have written.
    let mut models: Vec<String> = run_decisions(tmp.path())
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    models.sort();
    assert_eq!(
        models,
        ["customer_orders", "status_counts", "stg_orders"],
        "each model the run selects is evaluated"
    );
}

#[test]
fn deny_denies_an_agent_run_under_rocky_principal_env() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(tmp.path().join("rocky.toml"))
        .arg("run")
        .current_dir(tmp.path())
        .env("RUST_LOG", "error")
        .env("ROCKY_PRINCIPAL", "agent")
        .output()
        .expect("spawn rocky");
    assert_denied(tmp.path(), &out, "ROCKY_PRINCIPAL=agent run");
}

#[test]
fn deny_denies_an_agent_model_selection_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = rocky(
        tmp.path(),
        &["--principal", "agent", "run", "--model", "status_counts"],
    );
    assert_denied(tmp.path(), &out, "run --model");
}

#[test]
fn deny_denies_an_agent_branch_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let created = rocky(tmp.path(), &["branch", "create", "pr1"]);
    assert!(created.status.success(), "{}", describe(&created));
    let out = rocky(tmp.path(), &["--principal", "agent", "run", "--branch", "pr1"]);
    assert_denied(tmp.path(), &out, "run --branch");
}

#[test]
fn deny_denies_an_agent_dag_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["--principal", "agent", "run", "--dag"]);
    assert_denied(tmp.path(), &out, "run --dag");
}

#[test]
fn deny_denies_an_agent_replication_run() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert_denied(tmp.path(), &out, "replication run");
}

#[test]
fn deny_denies_an_agent_resume() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path(), DENY_POLICY);
    for args in [
        ["--principal", "agent", "run", "--resume-latest"].as_slice(),
        ["--principal", "agent", "run", "--resume", "run-0"].as_slice(),
    ] {
        let out = rocky(tmp.path(), args);
        assert_denied(tmp.path(), &out, &format!("{args:?}"));
    }
}

#[test]
fn a_human_run_under_the_same_policy_is_not_gated() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["run"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert_eq!(
        written_tables(tmp.path()),
        [
            "main.customer_orders",
            "main.status_counts",
            "main.stg_orders"
        ]
    );
}

#[test]
fn an_agent_run_without_a_policy_block_is_not_gated() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), "");
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert_eq!(written_tables(tmp.path()).len(), 3);
}

#[test]
fn an_agent_run_allowed_by_a_run_rule_writes() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(
        tmp.path(),
        &format!(
            "{DENY_POLICY}\n[[policy.rules]]\nprincipal = \"agent\"\ncapability = \"run\"\n\
             effect = \"allow\"\nscope = {{ any = true }}\n"
        ),
    );
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert_eq!(written_tables(tmp.path()).len(), 3);
    let decisions = run_decisions(tmp.path());
    assert_eq!(decisions.len(), 3, "{decisions:?}");
    assert!(decisions.iter().all(|(_, e)| e == "allow"), "{decisions:?}");
}
