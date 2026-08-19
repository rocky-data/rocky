//! #1397 regression: a project that declares a transformation pipeline must
//! not report a zero-node DAG as success.
//!
//! When the configured models root did not exist, `rocky dag` exited 0 with
//! zero nodes, and the dagster component cached that as a clean empty asset
//! graph — `dag_status: "success"`, zero assets, indistinguishable from a
//! project with no models. The refusal fires only when the whole graph is
//! empty AND a declared transformation root does not exist: a seed-only
//! pipeline, a replication pipeline, a sibling root holding the models, and
//! an existing-but-empty root (a supported no-op) all keep working. The
//! per-root tolerance is deliberate (see
//! `dag_column_lineage_ignores_roots_that_contribute_nothing`).

use std::fs;
use std::process::{Command, Output};

const TRANSFORMATION_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"
"#;

const REPLICATION_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"
"#;

fn run_dag(dir: &std::path::Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args(["--output", "json"])
        .arg("dag")
        .args(extra)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky dag")
}

fn write_model(models: &std::path::Path, name: &str) {
    fs::write(models.join(format!("{name}.sql")), "SELECT 1 AS id\n").expect("write sql");
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .expect("write sidecar");
}

#[test]
fn a_missing_models_root_refuses_instead_of_reporting_an_empty_dag() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), TRANSFORMATION_TOML).expect("write config");
    // No models directory, no seeds — the graph would be empty.

    let out = run_dag(dir, &[]);
    assert!(
        !out.status.success(),
        "an empty graph under a declared transformation pipeline must refuse; \
         stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("zero nodes")
            && stderr.contains("does not exist")
            && stderr.contains("'t'"),
        "the refusal must say the graph is empty and name the missing root's pipeline, got: {stderr}"
    );
}

/// An EXISTING empty root with nothing else is a supported no-op — a fresh
/// scaffold, an intentionally empty pipeline. Zero nodes at exit 0 is the
/// honest answer there; only a root that does not EXIST refuses. This is the
/// tolerance the first cut of this guard broke (red-team HIGH finding).
#[test]
fn an_existing_empty_root_is_a_no_op_not_a_refusal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), TRANSFORMATION_TOML).expect("write config");
    fs::create_dir(dir.join("models")).expect("create empty models dir");

    let out = run_dag(dir, &[]);
    assert!(
        out.status.success(),
        "an existing empty root must stay a no-op; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("dag output is JSON");
    assert!(
        json["nodes"].as_array().expect("nodes array").is_empty(),
        "the no-op graph is genuinely empty"
    );
}

/// The same refusal covers the explicit `--models` override pointing nowhere.
#[test]
fn a_models_override_matching_nothing_refuses_for_a_transformation_project() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), TRANSFORMATION_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "orders");

    let out = run_dag(dir, &["--models", "no-such-dir"]);
    assert!(
        !out.status.success(),
        "an override matching nothing must refuse when the project declares a \
         transformation pipeline; stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("zero nodes"),
        "expected the empty-graph refusal, got: {stderr}"
    );
}

/// The baseline the refusal tests rely on: the same config with a real model
/// dags fine. Without this, they could pass because the fixture is broken
/// outright.
#[test]
fn the_same_config_with_a_model_still_dags() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), TRANSFORMATION_TOML).expect("write config");
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    write_model(&models, "orders");

    let out = run_dag(dir, &[]);
    assert!(
        out.status.success(),
        "the healthy fixture must dag; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("dag output is JSON");
    assert!(
        !json["nodes"].as_array().expect("nodes array").is_empty(),
        "the healthy fixture must produce nodes"
    );
}

/// A transformation pipeline that exists for its seeds alone is a real shape
/// — no models key, no models directory, work arriving as seed CSVs. The
/// graph has seed nodes, so the empty-graph refusal must not fire.
#[test]
fn a_seed_only_pipeline_still_dags() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\n\
         [pipeline.t]\ntype = \"transformation\"\n\n\
         [pipeline.t.target]\nadapter = \"default\"\n",
    )
    .expect("write config");
    let seeds = dir.join("seeds");
    fs::create_dir(&seeds).expect("create seeds");
    fs::write(seeds.join("countries.csv"), "code,name\nUS,United States\n")
        .expect("write seed csv");

    let out = run_dag(dir, &[]);
    assert!(
        out.status.success(),
        "a seed-only pipeline must keep dagging; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("dag output is JSON");
    assert!(
        !json["nodes"].as_array().expect("nodes array").is_empty(),
        "the seed node must be in the graph"
    );
}

/// Replication-only: no transformation pipelines, so an empty model set is
/// the complete answer — with and without the `--models` override the SDK
/// sends by default. Refusing here would break every replication-only SDK
/// caller.
#[test]
fn a_replication_only_project_dags_with_and_without_the_override() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    fs::write(dir.join("rocky.toml"), REPLICATION_TOML).expect("write config");

    let plain = run_dag(dir, &[]);
    assert!(
        plain.status.success(),
        "a replication-only project must dag; stderr: {}",
        String::from_utf8_lossy(&plain.stderr)
    );

    let overridden = run_dag(dir, &["--models", "models"]);
    assert!(
        overridden.status.success(),
        "the SDK's default --models must stay tolerated on a replication-only \
         project; stderr: {}",
        String::from_utf8_lossy(&overridden.stderr)
    );
}
