//! `rocky apply` checks a plan's models before it runs them, through the
//! real `rocky plan` and `rocky apply` binaries.
//!
//! A person's apply compares the models only. So a plan whose models are
//! unchanged applies, even from another environment; a plan whose model was
//! edited refuses with `plan_models_changed`. Each case plans through the
//! real entry point, edits a model and expects the refusal, then restores
//! the model and expects the apply to succeed.
//!
//! `rocky plan` writes run plans for replication pipelines only, so every
//! project here is a replication pipeline with a `models/` directory. The
//! narrow pipeline glob case is a plan only `propose` writes; it is tested
//! in `rocky-cli` (`a_proposed_plan_fingerprints_the_pipeline_glob_apply_runs`).
//! The backfill cases are in `apply_models_changed.rs`.

use std::path::Path;
use std::process::{Command, Output};

const MODELS_CHANGED: &str = "plan_models_changed";
const CONFIG_CHANGED: &str = "plan_config_changed";

fn rocky(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .current_dir(dir)
        .env("HOME", dir)
        .env("RUST_LOG", "error")
        .env_remove("ROCKY_PRINCIPAL")
        .env_remove("ROCKY_SESSION_SOURCE")
        .env_remove("ROCKY_TEST_DB");
    command
}

fn describe(out: &Output) -> String {
    format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// `rocky plan <args>` in `dir`; the persisted run plan's id.
fn plan(dir: &Path, args: &[&str], envs: &[(&str, &str)]) -> String {
    let out = rocky(dir)
        .args(["--output", "json", "--config", "rocky.toml", "plan"])
        .args(args)
        .envs(envs.iter().copied())
        .output()
        .expect("run plan");
    assert!(out.status.success(), "plan failed: {}", describe(&out));
    let plan: serde_json::Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("plan output is not JSON ({e}): {}", describe(&out)));
    assert_eq!(plan["plan_kind"], "run", "{plan}");
    plan["plan_id"].as_str().expect("a plan id").to_string()
}

fn apply(dir: &Path, plan_id: &str, principal: Option<&str>, envs: &[(&str, &str)]) -> Output {
    let mut command = rocky(dir);
    command.args(["--output", "json", "--config", "rocky.toml"]);
    if let Some(principal) = principal {
        command.args(["--principal", principal]);
    }
    command
        .args(["apply", plan_id])
        .envs(envs.iter().copied())
        .output()
        .expect("run apply")
}

fn assert_refused(out: &Output, code: &str) {
    assert!(!out.status.success(), "must refuse: {}", describe(out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(code), "expected {code}: {}", describe(out));
}

fn assert_applied(out: &Output) {
    assert!(out.status.success(), "must apply: {}", describe(out));
}

/// Append a comment to a model file; return the original text.
fn edit(path: &Path) -> String {
    let original = std::fs::read_to_string(path).unwrap();
    std::fs::write(path, format!("{original}\n-- edited after the plan\n")).unwrap();
    original
}

/// Refuse while `path` is edited, then apply once it is restored.
fn refuses_when_edited_then_applies(
    root: &Path,
    plan_id: &str,
    path: &Path,
    envs: &[(&str, &str)],
) {
    let original = edit(path);
    assert_refused(&apply(root, plan_id, None, envs), MODELS_CHANGED);
    std::fs::write(path, original).unwrap();
    assert_applied(&apply(root, plan_id, None, envs));
}

/// A DuckDB replication pipeline over `raw__orders`, with two models under
/// `models/`. `adapter_path` is the adapter's `path` (it may be a `${VAR}`);
/// `mask` adds a `[mask]` and classifies `customers.email` as `pii`.
fn replication_project(root: &Path, adapter_path: &str, mask: bool) {
    {
        let conn = duckdb::Connection::open(root.join("fixture.duckdb")).unwrap();
        conn.execute_batch(
            "CREATE SCHEMA raw__orders; CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .unwrap();
    }
    let mask_block = if mask {
        "[mask]\npii = \"hash\"\n\n"
    } else {
        ""
    };
    std::fs::write(
        root.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"duckdb\"\npath = \"{adapter_path}\"\n\n{mask_block}\
             [pipeline.ingest]\nstrategy = \"full_refresh\"\n\n\
             [pipeline.ingest.source.discovery]\nadapter = \"default\"\n\n\
             [pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\n\
             components = [\"source\"]\n\n\
             [pipeline.ingest.target]\ncatalog_template = \"fixture\"\n\
             schema_template = \"staging__{{source}}\"\n\n\
             [pipeline.ingest.target.governance]\nauto_create_schemas = true\n"
        ),
    )
    .unwrap();
    let classification = if mask {
        "\n[classification]\nemail = \"pii\"\n"
    } else {
        ""
    };
    let model = |name: &str, sql: &str, extra: &str| {
        std::fs::create_dir_all(root.join("models")).unwrap();
        std::fs::write(root.join(format!("models/{name}.sql")), sql).unwrap();
        std::fs::write(
            root.join(format!("models/{name}.toml")),
            format!(
                "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"fixture\"\n\
                 schema = \"main\"\ntable = \"{name}\"\n{extra}"
            ),
        )
        .unwrap();
    };
    model(
        "customers",
        "SELECT 1 AS id, 'a@example.com' AS email\n",
        classification,
    );
    model("totals", "SELECT 2 AS v\n", "");
}

/// (a) A replication pipeline with models and a `[mask]`, planned with
/// `--all`: the plan binds the env-resolved mask, and a person's apply
/// compares the models only.
#[test]
fn replication_pipeline_with_models_and_a_mask_planned_with_all() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    replication_project(root, "fixture.duckdb", true);

    let plan_id = plan(root, &["--all"], &[]);
    refuses_when_edited_then_applies(root, &plan_id, &root.join("models/customers.sql"), &[]);
}

/// (c) A `--model` plan: an edit to that model refuses.
#[test]
fn a_model_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    replication_project(root, "fixture.duckdb", false);

    let plan_id = plan(root, &["--model", "totals"], &[]);
    refuses_when_edited_then_applies(root, &plan_id, &root.join("models/totals.sql"), &[]);
}

/// (e) A plan made with one value for a `${VAR}` in the adapter and applied
/// with another (two spellings of the same file). A person's apply compares
/// the models only, so it applies; an edited model still refuses. An agent's
/// apply compares the config too, and refuses with `plan_config_changed`.
#[test]
fn a_different_env_value_applies_for_a_person_and_refuses_an_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    replication_project(root, "${ROCKY_TEST_DB}", false);

    let plan_id = plan(root, &["--all"], &[("ROCKY_TEST_DB", "fixture.duckdb")]);
    let other_env = [("ROCKY_TEST_DB", "./fixture.duckdb")];

    assert_refused(
        &apply(root, &plan_id, Some("agent"), &other_env),
        CONFIG_CHANGED,
    );
    refuses_when_edited_then_applies(root, &plan_id, &root.join("models/totals.sql"), &other_env);
}
