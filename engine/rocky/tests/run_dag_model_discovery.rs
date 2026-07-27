//! `rocky run --dag` must discover models from the model set each transformation
//! pipeline actually declares.
//!
//! It used to hardcode `<config_dir>/models`, which was wrong in both
//! directions. A pipeline declaring `models = "transforms/**"` loaded nothing,
//! built zero transformation nodes, and exited 0 — the #1243 silent success by a
//! different route, which #1244's error propagation could never reach because
//! there was no error, just an absent directory. And a project with no
//! transformation pipeline at all was still forced to validate a `models/`
//! directory that only transformation pipelines consume, so a broken model there
//! failed a replication-only `--dag` run that plain `rocky run` executes happily.
//!
//! Seed discovery is covered here too: it shares the failure mode the model load
//! had, one seam over.

use std::fs;
use std::process::Command;

const SEED_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 5) AS customer_id,
    CAST(i AS DOUBLE) AS amount,
    TIMESTAMP '2026-01-01' + INTERVAL (i) SECOND AS _updated_at
FROM generate_series(1, 10) AS t(i);
"#;

/// A transformation pipeline whose models live somewhere other than `models/`.
const CUSTOM_GLOB_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"
models = "transforms/**"

[pipeline.transform.target]
adapter = "default"
"#;

/// Replication only — no transformation pipeline, so no model is ever executed.
const REPLICATION_ONLY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"
timestamp_column = "_updated_at"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true

[pipeline.ingest.execution]
concurrency = 1
"#;

const MODEL_SQL: &str = "SELECT order_id, customer_id, amount FROM raw__orders.orders\n";

const MODEL_TOML: &str = r#"
[target]
catalog = "fixture"
schema = "staging"
"#;

/// TOML that cannot parse — an unterminated array.
const MALFORMED_TOML: &str = "name = [\n";

fn seed_duckdb(dir: &std::path::Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(SEED_SQL).expect("seed sql");
}

fn run_dag(dir: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .arg("run")
        .arg("--dag")
        .arg("--output")
        .arg("json")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

/// A pipeline pointing at `transforms/**` must produce that model's node AND
/// execute it. Asserting only the node count is not enough: discovery and
/// execution resolve the directory separately, and a build that discovers the
/// model but hands the sub-run a hardcoded `models/` fails the node with
/// "models directory not found" while still reporting `total_nodes: 1`.
///
/// Before the fix this reported `total_nodes: 0` and exited 0.
#[test]
fn custom_models_glob_is_honored() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(dir.join("rocky.toml"), CUSTOM_GLOB_TOML).expect("write rocky.toml");
    let transforms = dir.join("transforms");
    fs::create_dir(&transforms).expect("mkdir transforms");
    fs::write(transforms.join("stg_orders.sql"), MODEL_SQL).expect("write model sql");
    fs::write(transforms.join("stg_orders.toml"), MODEL_TOML).expect("write model toml");

    let out = run_dag(dir);
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");

    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("expected a DagRunOutput on stdout: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    });
    assert_eq!(
        parsed
            .get("total_nodes")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "the transforms/ model must produce exactly one node, got:\n{stdout}"
    );
    let labels: Vec<&str> = parsed["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .filter_map(|n| n.get("label").and_then(serde_json::Value::as_str))
        .collect();
    assert!(
        labels.contains(&"stg_orders"),
        "expected the stg_orders node, got labels {labels:?}"
    );
    // The node must actually run — discovery finding it is only half the job.
    assert_eq!(
        parsed.get("failed").and_then(serde_json::Value::as_u64),
        Some(0),
        "the transforms/ model must execute, not just be discovered:\n{stdout}"
    );
    assert_eq!(
        parsed.get("completed").and_then(serde_json::Value::as_u64),
        Some(1),
        "expected the transforms/ model to complete:\n{stdout}"
    );
    assert!(
        out.status.success(),
        "the run must succeed\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}

/// No transformation pipeline means no model is ever executed, so an unrelated
/// `models/` directory must not gate the run. Plain `rocky run` does not compile
/// models for a replication-only project; `--dag` must not either.
#[test]
fn replication_only_run_ignores_an_unrelated_models_dir() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(dir.join("rocky.toml"), REPLICATION_ONLY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    fs::write(models_dir.join("broken.sql"), "SELECT 1 AS id\n").expect("write model sql");
    fs::write(models_dir.join("broken.toml"), MALFORMED_TOML).expect("write malformed sidecar");

    let out = run_dag(dir);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "a replication-only DAG run must not fail on a models dir it never \
         executes\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}

/// A transformation pipeline DOES consume models, so a broken one there is still
/// fatal — the #1244 guarantee must survive the scoping change.
#[test]
fn transformation_run_still_fails_on_a_broken_model() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(dir.join("rocky.toml"), CUSTOM_GLOB_TOML).expect("write rocky.toml");
    let transforms = dir.join("transforms");
    fs::create_dir(&transforms).expect("mkdir transforms");
    fs::write(transforms.join("broken.sql"), "SELECT 1 AS id\n").expect("write model sql");
    fs::write(transforms.join("broken.toml"), MALFORMED_TOML).expect("write malformed sidecar");

    let out = run_dag(dir);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a broken model in the pipeline's own model set must fail the run"
    );
    assert!(
        stderr.contains("failed to load models from"),
        "expected the model-load error on stderr, got:\n{stderr}"
    );
}

/// A malformed seed sidecar must fail the run rather than silently yielding an
/// empty seed list — which would drop the seed node and its seed→model edge and
/// let a model run against stale data under a green DAG.
#[test]
fn malformed_seed_sidecar_fails_the_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(dir.join("rocky.toml"), REPLICATION_ONLY_TOML).expect("write rocky.toml");
    let seeds_dir = dir.join("seeds");
    fs::create_dir(&seeds_dir).expect("mkdir seeds");
    fs::write(seeds_dir.join("countries.csv"), "code,name\nFR,France\n").expect("write seed csv");
    fs::write(seeds_dir.join("countries.toml"), MALFORMED_TOML).expect("write malformed sidecar");

    let out = run_dag(dir);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a malformed seed sidecar must fail the DAG run"
    );
    assert!(
        stderr.contains("failed to discover seeds"),
        "expected the seed-discovery error on stderr, got:\n{stderr}"
    );
}

/// `models` is a glob, not a directory. A non-`**` wildcard must still resolve
/// to the conventional directory: deriving the base by splitting on `**` alone
/// leaves `models/*.sql` intact, probes it as a literal directory that never
/// exists, and yields an empty DAG that exits 0.
#[test]
fn a_wildcard_glob_still_resolves_its_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(
        dir.join("rocky.toml"),
        CUSTOM_GLOB_TOML.replace(r#"models = "transforms/**""#, r#"models = "models/*.sql""#),
    )
    .expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    fs::write(models_dir.join("stg_orders.sql"), MODEL_SQL).expect("write model sql");
    fs::write(models_dir.join("stg_orders.toml"), MODEL_TOML).expect("write model toml");

    let out = run_dag(dir);
    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("expected a DagRunOutput on stdout: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    });
    assert_eq!(
        parsed.get("completed").and_then(serde_json::Value::as_u64),
        Some(1),
        "a `models/*.sql` glob must resolve to models/ and build it:\n{stdout}"
    );
}

/// Two models sharing a name cannot both become nodes — the node id is the
/// name. Report it here, naming both files, instead of letting the executor
/// surface `circular dependency detected involving: []` (#1261), which names
/// nothing.
#[test]
fn duplicate_model_names_are_reported_by_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed_duckdb(dir);

    fs::write(dir.join("rocky.toml"), CUSTOM_GLOB_TOML).expect("write rocky.toml");
    let transforms = dir.join("transforms");
    let nested = transforms.join("staging");
    fs::create_dir_all(&nested).expect("mkdir transforms/staging");
    for target in [&transforms, &nested] {
        fs::write(target.join("orders.sql"), MODEL_SQL).expect("write model sql");
        fs::write(target.join("orders.toml"), MODEL_TOML).expect("write model toml");
    }

    let out = run_dag(dir);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a duplicate model name must fail the run"
    );
    assert!(
        stderr.contains("duplicate model name 'orders'"),
        "expected the duplicate-name error, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("circular dependency"),
        "the duplicate must be reported by name, not as a phantom cycle:\n{stderr}"
    );
}

/// `--models` is not threaded into the DAG path, so passing it alongside `--dag`
/// must be rejected rather than silently ignored. Mirrors the `--var` guard.
#[test]
fn models_flag_with_dag_is_rejected() {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["run", "--dag", "--models", "transforms"])
        .output()
        .expect("spawn rocky");
    assert!(
        !out.status.success(),
        "`run --dag --models` must exit non-zero, got {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--models is not supported with --dag"),
        "expected the --models/--dag guard message, got stderr: {stderr}"
    );
}
