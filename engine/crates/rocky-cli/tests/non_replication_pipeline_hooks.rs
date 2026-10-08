//! #2317: `on_pipeline_start` / `on_pipeline_complete` / `on_pipeline_error`
//! fire for every pipeline type, not just replication.
#![cfg(feature = "duckdb")]

use std::path::Path;

use rocky_cli::commands::{DeferOptions, PartitionRunOptions, SkipRunOptions};
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Hook TOML that appends each event's stdin JSON to `log`.
fn hooks_toml(log: &Path, events: &[&str]) -> String {
    events
        .iter()
        .map(|e| {
            format!(
                "[[hook.{e}]]\ncommand = \"cat >> {log}\"\n\n",
                log = log.display()
            )
        })
        .collect()
}

async fn run_pipeline(dir: &Path, pipeline: &str) -> anyhow::Result<()> {
    let config_path = dir.join("rocky.toml");
    let loaded = std::sync::Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
    );
    rocky_cli::commands::run(
        &config_path,
        loaded,
        None,
        Some(pipeline),
        &dir.join("state.redb"),
        None,
        false,
        None,
        false,
        None,
        false,
        None,
        &PartitionRunOptions::default(),
        None,
        None,
        None,
        None,
        &DeferOptions::default(),
        &SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None,
        None,
        false,
        None,
        &rocky_core::config::PrincipalRef::unnamed(),
    )
    .await
    .map(|_| ())
}

fn events(log: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    serde_json::Deserializer::from_str(&text)
        .into_iter::<serde_json::Value>()
        .map(|v| v.expect("hook stdin is JSON"))
        .collect()
}

fn names(evs: &[serde_json::Value]) -> Vec<String> {
    evs.iter()
        .map(|e| e["event"].as_str().unwrap().to_string())
        .collect()
}

async fn transformation_project(dir: &Path, model_sql: &str) {
    let db = dir.join("t.duckdb");
    let adapter = DuckDbWarehouseAdapter::open(&db).unwrap();
    adapter
        .execute_statement("CREATE TABLE main.src (id INTEGER); INSERT INTO main.src VALUES (1)")
        .await
        .unwrap();
    drop(adapter);
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(dir.join("models/m.sql"), format!("{model_sql}\n")).unwrap();
    std::fs::write(
        dir.join("models/m.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"m\"\n",
    )
    .unwrap();
    let log = dir.join("hook.log");
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.tr]
type = "transformation"
models = "models/**"

[pipeline.tr.target.governance]
auto_create_schemas = true

{hooks}"#,
            db = db.display(),
            hooks = hooks_toml(
                &log,
                &[
                    "on_pipeline_start",
                    "on_compile_complete",
                    "on_before_model_run",
                    "on_after_model_run",
                    "on_pipeline_complete",
                    "on_pipeline_error",
                ]
            )
        ),
    )
    .unwrap();
}

#[tokio::test]
async fn transformation_run_fires_pipeline_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), "SELECT id FROM main.src").await;

    run_pipeline(tmp.path(), "tr").await.expect("run succeeds");

    let evs = events(&tmp.path().join("hook.log"));
    assert_eq!(
        names(&evs),
        [
            "pipeline_start",
            "compile_complete",
            "before_model_run",
            "after_model_run",
            "pipeline_complete"
        ],
        "{evs:?}"
    );
    assert!(evs.iter().all(|e| e["pipeline"] == "tr"));
    assert_eq!(evs[1]["metadata"]["model_count"], 1);
    assert!(evs[4]["duration_ms"].is_u64());
}

#[tokio::test]
async fn failing_transformation_run_fires_pipeline_error_not_complete() {
    let tmp = tempfile::tempdir().unwrap();
    transformation_project(tmp.path(), "SELECT id FROM main.does_not_exist").await;

    run_pipeline(tmp.path(), "tr")
        .await
        .expect_err("a model with a missing source fails the run");

    let evs = events(&tmp.path().join("hook.log"));
    let got = names(&evs);
    assert_eq!(
        got.first().map(String::as_str),
        Some("pipeline_start"),
        "{got:?}"
    );
    assert_eq!(
        got.last().map(String::as_str),
        Some("pipeline_error"),
        "{got:?}"
    );
    assert!(!got.contains(&"pipeline_complete".to_string()), "{got:?}");
}

#[tokio::test]
async fn snapshot_run_fires_pipeline_hooks() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let db = dir.join("s.duckdb");
    let adapter = DuckDbWarehouseAdapter::open(&db).unwrap();
    adapter
        .execute_statement("CREATE TABLE main.src (customer_id INTEGER, updated_at TIMESTAMP); INSERT INTO main.src VALUES (1, TIMESTAMP '2026-01-01')")
        .await
        .unwrap();
    drop(adapter);
    let log = dir.join("hook.log");
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.dim]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"

[pipeline.dim.source]
catalog = "s"
schema = "main"
table = "src"

[pipeline.dim.target]
catalog = "s"
schema = "main"
table = "history"

[pipeline.dim.target.governance]
auto_create_schemas = true

{hooks}"#,
            db = db.display(),
            hooks = hooks_toml(&log, &["on_pipeline_start", "on_pipeline_complete"])
        ),
    )
    .unwrap();

    run_pipeline(dir, "dim")
        .await
        .expect("snapshot run succeeds");

    assert_eq!(
        names(&events(&log)),
        ["pipeline_start", "pipeline_complete"]
    );
}
