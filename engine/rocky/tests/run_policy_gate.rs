//! `[policy]` gates `rocky run` (#2610).
//!
//! An agent under `default_agent_effect = "deny"` must not write through any
//! `rocky run` path, and every refusal must leave a decision that
//! `rocky audit` lists. A human under the same policy is never gated.
//!
//! ```text
//!   path                          agent + deny          human + deny
//!   run                           refused, 0 tables     3 tables
//!   run --model / --dag           refused, 0 tables
//!   run (replication)             refused, 0 tables
//!   run --resume / --resume-latest refused, table not built
//!   run --branch pr1              runs (branch schema): the default posture
//!                                 is waived; only a `run` rule governs it
//! ```
//!
//! `require_review` is recorded but not enforced in this change (a one-step
//! run has no plan to review); an `apply` rule never matches a `run`.
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
    replication_project_with(dir, policy, "");
}

/// `extra` runs after the seed, e.g. to plant an object that fails a copy.
fn replication_project_with(dir: &Path, policy: &str, extra: &str) {
    let conn = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    conn.execute_batch(&format!(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders AS SELECT 1 AS id, 'a' AS name;
         CREATE TABLE raw__orders.returns AS SELECT 2 AS id;
         {extra}"
    ))
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
    assert!(
        out.status.success(),
        "audit must succeed\n{}",
        describe(&out)
    );
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
    assert_denied_from(dir, out, what, &[]);
}

/// [`assert_denied`] for a warehouse that already held `before`.
fn assert_denied_from(dir: &Path, out: &Output, what: &str, before: &[&str]) {
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
        before,
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

/// A branch run writes branch schemas, not production. The default posture
/// is waived for it, so the documented agent flow (`run --branch`, then a
/// gated `plan promote`) still works under `default_agent_effect = "deny"`.
/// The verdict is still recorded, and an explicit `run` rule still decides.
#[test]
fn an_agent_branch_run_is_governed_by_explicit_run_rules_only() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let created = rocky(tmp.path(), &["branch", "create", "pr1"]);
    assert!(created.status.success(), "{}", describe(&created));
    let out = rocky(
        tmp.path(),
        &["--principal", "agent", "run", "--branch", "pr1"],
    );
    assert!(out.status.success(), "{}", describe(&out));
    let tables = written_tables(tmp.path());
    assert_eq!(tables.len(), 3, "{tables:?}");
    assert!(
        tables.iter().all(|t| !t.starts_with("main.")),
        "a branch run must not write production: {tables:?}"
    );
    let decisions = run_decisions(tmp.path());
    assert_eq!(decisions.len(), 3, "{decisions:?}");
    assert!(decisions.iter().all(|(_, e)| e == "allow"), "{decisions:?}");

    let tmp = tempfile::tempdir().unwrap();
    transformation_project(
        tmp.path(),
        &format!(
            "{DENY_POLICY}\n[[policy.rules]]\nprincipal = \"agent\"\ncapability = \"run\"\n\
             effect = \"deny\"\nscope = {{ any = true }}\n"
        ),
    );
    let created = rocky(tmp.path(), &["branch", "create", "pr1"]);
    assert!(created.status.success(), "{}", describe(&created));
    let out = rocky(
        tmp.path(),
        &["--principal", "agent", "run", "--branch", "pr1"],
    );
    assert_denied(tmp.path(), &out, "run --branch under a deny run rule");
}

#[test]
fn deny_denies_an_agent_dag_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["--principal", "agent", "run", "--dag"]);
    assert_denied(tmp.path(), &out, "run --dag");
    let mut models: Vec<String> = run_decisions(tmp.path())
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    models.sort();
    assert_eq!(models, ["customer_orders", "status_counts", "stg_orders"]);
}

#[test]
fn deny_denies_an_agent_replication_run() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path(), DENY_POLICY);
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert_denied(tmp.path(), &out, "replication run");
}

/// A resume is a replication write too. A human run fails one table (a
/// view squats its target name); the squatter is removed; an agent's resume
/// under the deny policy must still not build the table.
#[test]
fn deny_denies_an_agent_resume() {
    for flag in ["--resume-latest", "--resume"] {
        let tmp = tempfile::tempdir().unwrap();
        replication_project_with(
            tmp.path(),
            DENY_POLICY,
            "CREATE SCHEMA staging__orders; CREATE VIEW staging__orders.returns AS SELECT 9 AS id;",
        );
        let first = rocky(tmp.path(), &["run"]);
        assert!(
            !first.status.success(),
            "the seeded run must fail\n{}",
            describe(&first)
        );
        let history = rocky(tmp.path(), &["--output", "json", "history"]);
        let run_id = serde_json::from_slice::<serde_json::Value>(&history.stdout)
            .ok()
            .and_then(|v| v["runs"][0]["run_id"].as_str().map(str::to_string))
            .unwrap_or_else(|| panic!("history must list the failed run\n{}", describe(&history)));
        let conn = duckdb::Connection::open(tmp.path().join("warehouse.duckdb")).unwrap();
        conn.execute_batch("DROP VIEW staging__orders.returns")
            .unwrap();
        drop(conn);
        assert_eq!(written_tables(tmp.path()), ["staging__orders.orders"]);

        let mut args = vec!["--principal", "agent", "run", flag];
        if flag == "--resume" {
            args.push(&run_id);
        }
        let out = rocky(tmp.path(), &args);
        assert_denied_from(tmp.path(), &out, flag, &["staging__orders.orders"]);
    }
}

/// An `apply` rule was written for the reviewed two-step path. It must not
/// grant a one-step write.
#[test]
fn an_apply_rule_does_not_allow_a_run() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(
        tmp.path(),
        &format!(
            "{DENY_POLICY}\n[[policy.rules]]\nprincipal = \"agent\"\ncapability = \"apply\"\n\
             effect = \"allow\"\nscope = {{ any = true }}\n"
        ),
    );
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert_denied(tmp.path(), &out, "run under an apply allow rule");
}

/// `require_review` has no plan to review on a one-step run. This change
/// records the verdict and lets the run proceed; refusing it waits on the
/// owner.
#[test]
fn a_require_review_verdict_is_recorded_and_the_run_proceeds() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), "\n[policy]\nversion = 1\n");
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert!(out.status.success(), "{}", describe(&out));
    assert_eq!(written_tables(tmp.path()).len(), 3);
    let decisions = run_decisions(tmp.path());
    assert_eq!(decisions.len(), 3, "{decisions:?}");
    assert!(
        decisions.iter().all(|(_, e)| e == "require_review"),
        "{decisions:?}"
    );
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

/// A policy that allows agent runs by default and denies `run` on the models
/// a scope names.
fn scoped_deny(scope: &str) -> String {
    format!(
        "\n[policy]\nversion = 1\ndefault_agent_effect = \"allow\"\n\n[[policy.rules]]\n\
         principal = \"agent\"\ncapability = \"run\"\neffect = \"deny\"\nscope = {scope}\n"
    )
}

/// The subject set is the run's own selection, not a superset: a scoped deny
/// refuses a run that selects the model, and leaves a run that does not
/// select it alone. A default deny would refuse any subject string and hide
/// a wrong subject set.
#[test]
fn a_scoped_deny_gates_exactly_the_selected_models() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), &scoped_deny("{ models = [\"status_counts\"] }"));
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(String::from_utf8_lossy(&out.stderr).contains("policy DENIES"));
    assert_eq!(written_tables(tmp.path()), Vec::<String>::new());

    let out = rocky(
        tmp.path(),
        &["--principal", "agent", "run", "--model", "stg_orders"],
    );
    assert!(out.status.success(), "{}", describe(&out));
    assert_eq!(written_tables(tmp.path()), ["main.stg_orders"]);

    // A multi-model `--select` is gated over exactly its selection.
    let out = rocky(
        tmp.path(),
        &[
            "--principal",
            "agent",
            "run",
            "--select",
            "stg_orders",
            "customer_orders",
        ],
    );
    assert!(out.status.success(), "{}", describe(&out));
    let out = rocky(
        tmp.path(),
        &[
            "--principal",
            "agent",
            "run",
            "--select",
            "stg_orders",
            "status_counts",
        ],
    );
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(!written_tables(tmp.path()).contains(&"main.status_counts".to_string()));
}

/// A multi-model `--select` takes the model-only arm whatever the pipeline
/// type. On a replication-only project it builds models from `models/`, so
/// it must be gated over them, not treated as a replication run.
#[test]
fn deny_denies_a_multi_model_select_on_a_replication_project() {
    let tmp = tempfile::tempdir().unwrap();
    // The replication tables are allowed, so a replication-shaped gate would
    // pass: only gating the selected models refuses this run.
    replication_project(
        tmp.path(),
        "\n[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n\n[[policy.rules]]\n\
         principal = \"agent\"\ncapability = \"run\"\neffect = \"allow\"\n\
         scope = { models = [\"orders\", \"returns\"] }\n",
    );
    let models = tmp.path().join("models");
    fs::create_dir_all(&models).unwrap();
    for name in ["m1", "m2"] {
        fs::write(
            models.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
                 catalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
        fs::write(models.join(format!("{name}.sql")), "select 1 as id").unwrap();
    }
    let out = rocky(
        tmp.path(),
        &["--principal", "agent", "run", "--select", "m1", "m2"],
    );
    assert_denied(tmp.path(), &out, "run --select m1 m2 (replication project)");
}

/// `--dag` evaluates each pipeline's models against that pipeline's own
/// models directory, so an attribute-scoped rule sees a model of the second
/// pipeline with its real tags.
#[test]
fn a_dag_gate_reads_each_pipelines_model_attributes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(
        root.join("rocky.toml"),
        format!(
            r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.first]
type = "transformation"
models = "first/**"

[pipeline.first.target.governance]
auto_create_schemas = true

[pipeline.second]
type = "transformation"
models = "second/**"

[pipeline.second.target.governance]
auto_create_schemas = true
{}"#,
            scoped_deny("{ tags = { tier = \"gold\" } }")
        ),
    )
    .unwrap();
    for (dir, name, extra) in [
        ("first", "alpha", ""),
        ("second", "beta", "\n[tags]\ntier = \"gold\"\n"),
    ] {
        let d = root.join(dir);
        fs::create_dir_all(&d).unwrap();
        fs::write(
            d.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
                 catalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n{extra}"
            ),
        )
        .unwrap();
        fs::write(d.join(format!("{name}.sql")), "select 1 as id").unwrap();
    }
    duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    let out = rocky(root, &["--principal", "agent", "run", "--dag"]);
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(String::from_utf8_lossy(&out.stderr).contains("policy DENIES"));
    // One deny refuses the whole graph before any node runs.
    assert_eq!(written_tables(root), Vec::<String>::new());
    let decisions = run_decisions(root);
    assert!(
        decisions.contains(&("beta".to_string(), "deny".to_string())),
        "{decisions:?}"
    );
}

/// A replication pipeline in a `--dag` graph is gated over its discovered
/// tables in its sub-run, not under a node label: a targeted allow of every
/// real subject lets the graph run.
#[test]
fn a_dag_with_a_replication_pipeline_is_gated_on_its_tables() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(
        tmp.path(),
        "\n[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n\n[[policy.rules]]\n\
         principal = \"agent\"\ncapability = \"run\"\neffect = \"allow\"\n\
         scope = { models = [\"orders\", \"returns\"] }\n",
    );
    let out = rocky(tmp.path(), &["--principal", "agent", "run", "--dag"]);
    assert!(out.status.success(), "{}", describe(&out));
    let mut decisions = run_decisions(tmp.path());
    decisions.sort();
    assert_eq!(
        decisions,
        [
            ("orders".to_string(), "allow".to_string()),
            ("returns".to_string(), "allow".to_string())
        ]
    );
}

/// The in-run replication gate decides per discovered table: a `run` rule
/// that allows `orders` but not `returns` refuses the run before any
/// warehouse statement, and records both verdicts.
#[test]
fn a_replication_run_is_gated_per_discovered_table() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(
        tmp.path(),
        "\n[policy]\nversion = 1\ndefault_agent_effect = \"deny\"\n\n[[policy.rules]]\n\
         principal = \"agent\"\ncapability = \"run\"\neffect = \"allow\"\n\
         scope = { models = [\"orders\"] }\n",
    );
    let out = rocky(tmp.path(), &["--principal", "agent", "run"]);
    assert!(!out.status.success(), "{}", describe(&out));
    assert!(String::from_utf8_lossy(&out.stderr).contains("policy DENIES"));
    assert_eq!(written_tables(tmp.path()), Vec::<String>::new());
    let mut decisions = run_decisions(tmp.path());
    decisions.sort();
    assert_eq!(
        decisions,
        [
            ("orders".to_string(), "allow".to_string()),
            ("returns".to_string(), "deny".to_string())
        ]
    );
}
