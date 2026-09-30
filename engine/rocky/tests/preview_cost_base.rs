//! Cost preview selects its base through the real CLI and renders missing sides.

use std::path::Path;
use std::process::Command;

use chrono::{Duration, Utc};
use rocky_core::state::{RunRecord, StateStore};

fn record(store: &StateStore, id: &str, minutes: i64, rocky_branch: Option<&str>) {
    let at = Utc::now() + Duration::minutes(minutes);
    let mut run: RunRecord = serde_json::from_value(serde_json::json!({
        "run_id": id,
        "started_at": at.to_rfc3339(),
        "finished_at": at.to_rfc3339(),
        "status": "Success",
        "models_executed": [],
        "trigger": "Manual",
        "config_hash": "h"
    }))
    .expect("run fixture");
    run.git_branch = Some("main".to_string());
    run.rocky_branch = rocky_branch.map(str::to_string);
    store.record_run(&run).expect("record run");
}

fn preview_cost(root: &Path, state: &Path, branch: &str) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args([
            "--state-path",
            state.to_str().unwrap(),
            "preview",
            "cost",
            "--name",
            branch,
            "--output",
            "json",
        ])
        .output()
        .expect("spawn rocky preview cost");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("one preview cost JSON document")
}

#[test]
fn preview_cost_uses_ordinary_run_before_another_branch_run() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.redb");
    {
        let store = StateStore::open(&state).unwrap();
        record(&store, "ordinary", 0, None);
        record(&store, "branch_a_run", 1, Some("branch_a"));
        record(&store, "branch_b_run", 2, Some("branch_b"));
    }

    let output = preview_cost(dir.path(), &state, "branch_a");
    assert_eq!(output["branch_run_id"], "branch_a_run");
    assert_eq!(output["base_run_id"], "ordinary");
    assert!(
        output["markdown"]
            .as_str()
            .unwrap()
            .contains("Both runs exist, but neither recorded a model execution")
    );
}

#[test]
fn preview_cost_names_the_missing_base_and_its_next_step() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.redb");
    {
        let store = StateStore::open(&state).unwrap();
        record(&store, "branch_a_run", 0, Some("branch_a"));
        record(&store, "branch_b_run", 1, Some("branch_b"));
    }

    let output = preview_cost(dir.path(), &state, "branch_a");
    assert_eq!(output["branch_run_id"], "branch_a_run");
    assert!(output.get("base_run_id").is_none());
    let markdown = output["markdown"].as_str().unwrap();
    assert!(markdown.contains("No base run yet"), "{markdown}");
    assert!(markdown.contains("without `--branch`"), "{markdown}");
    assert!(!markdown.contains("No branch run yet"), "{markdown}");
}

#[test]
fn preview_cost_names_the_missing_branch_and_its_next_step() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.redb");
    {
        let store = StateStore::open(&state).unwrap();
        record(&store, "ordinary", 0, None);
    }

    let output = preview_cost(dir.path(), &state, "branch_a");
    assert_eq!(output["base_run_id"], "ordinary");
    let markdown = output["markdown"].as_str().unwrap();
    assert!(markdown.contains("No branch run yet"), "{markdown}");
    assert!(
        markdown.contains("rocky run --branch branch_a"),
        "{markdown}"
    );
}
