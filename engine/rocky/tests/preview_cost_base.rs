//! Cost preview selects its base through the real CLI and renders missing sides.

use std::path::Path;
use std::process::Command;

use chrono::{Duration, Utc};
use rocky_core::state::{RunRecord, RunScope, StateStore};

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
    assert!(
        markdown.contains("without `--branch` or `--shadow`"),
        "{markdown}"
    );
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

fn rocky(root: &Path, state: &Path, args: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(["--state-path", state.to_str().unwrap()])
        .args(args)
        .output()
        .expect("spawn rocky");
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON output")
}

#[test]
fn real_runs_keep_shadow_and_branch_out_of_diff_and_cost_bases() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let state = root.join("state.redb");
    let models = root.join("models");
    std::fs::create_dir(&models).unwrap();
    std::fs::write(
        root.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"probe.duckdb\"\n\
         [pipeline.probe]\ntype = \"transformation\"\nmodels = \"models\"\n\
         [pipeline.probe.target.governance]\nauto_create_schemas = true\n",
    )
    .unwrap();
    std::fs::write(
        models.join("orders.toml"),
        "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"probe\"\nschema = \"main\"\n",
    )
    .unwrap();
    let model = models.join("orders.sql");
    std::fs::write(&model, "SELECT unnest([1]) AS id").unwrap();

    let git = |args: &[&str]| {
        let out = Command::new("git")
            .current_dir(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    git(&["init", "-b", "main"]);
    git(&[
        "add",
        "rocky.toml",
        "models/orders.sql",
        "models/orders.toml",
    ]);
    git(&[
        "-c",
        "commit.gpgsign=false",
        "-c",
        "user.name=Rocky Test",
        "-c",
        "user.email=rocky@example.invalid",
        "commit",
        "-m",
        "fixture",
    ]);
    let sha = git(&["rev-parse", "HEAD"]).trim().to_string();

    rocky(
        root,
        &state,
        &["run", "--pipeline", "probe", "--output", "json"],
    );
    let production_id = StateStore::open(&state).unwrap().list_runs(1).unwrap()[0]
        .run_id
        .clone();
    std::fs::write(&model, "SELECT unnest([1, 2]) AS id").unwrap();
    rocky(
        root,
        &state,
        &["run", "--pipeline", "probe", "--shadow", "--output", "json"],
    );
    rocky(
        root,
        &state,
        &[
            "run",
            "--pipeline",
            "probe",
            "--shadow",
            "--shadow-schema",
            "scratch",
            "--output",
            "json",
        ],
    );
    rocky(
        root,
        &state,
        &["branch", "create", "preview", "--output", "json"],
    );
    std::fs::write(&model, "SELECT unnest([1, 2, 3]) AS id").unwrap();
    rocky(
        root,
        &state,
        &[
            "run",
            "--pipeline",
            "probe",
            "--branch",
            "preview",
            "--output",
            "json",
        ],
    );
    let branch_id = StateStore::open(&state).unwrap().list_runs(1).unwrap()[0]
        .run_id
        .clone();

    let store = StateStore::open(&state).unwrap();
    let runs = store.list_runs(10).unwrap();
    assert_eq!(runs.len(), 4);
    assert_eq!(
        runs.iter()
            .find(|r| r.run_scope == Some(RunScope::Production))
            .unwrap()
            .run_id,
        production_id
    );
    assert!(
        runs.iter()
            .any(|r| r.run_scope == Some(RunScope::Shadow { schema: None }))
    );
    assert!(runs.iter().any(|r| r.run_scope
        == Some(RunScope::Shadow {
            schema: Some("scratch".into())
        })));
    assert!(runs.iter().any(|r| r.run_scope
        == Some(RunScope::Branch {
            name: "preview".into()
        })
        && r.run_id == branch_id));
    // DuckDB full-refresh executions do not report rows_affected. Supply
    // distinct recorded counts so preview diff reveals which base it chose.
    for mut run in runs {
        let count = match run.run_scope.as_ref() {
            Some(RunScope::Production) => 1,
            Some(RunScope::Shadow { .. }) => 2,
            Some(RunScope::Branch { .. }) => 3,
            None => unreachable!("all four runs were created by this binary"),
        };
        assert_eq!(run.models_executed.len(), 1);
        run.models_executed[0].rows_affected = Some(count);
        store.record_run(&run).unwrap();
    }
    drop(store);

    let cost = preview_cost(root, &state, "preview");
    assert_eq!(cost["base_run_id"], production_id);
    assert_eq!(cost["branch_run_id"], branch_id);

    for base in ["main", sha.as_str()] {
        let diff = rocky(
            root,
            &state,
            &[
                "preview", "diff", "--name", "preview", "--base", base, "--output", "json",
            ],
        );
        assert!(diff.get("base_note").is_none(), "{diff}");
        assert_eq!(diff["summary"]["total_rows_added"], 2, "{diff}");
    }

    // An ordinary run writes the production target and records that scope.
    // (`--shadow-schema` without `--shadow` is refused by the CLI, #2192.)
    rocky(
        root,
        &state,
        &["run", "--pipeline", "probe", "--output", "json"],
    );
    let latest = StateStore::open(&state).unwrap().list_runs(1).unwrap();
    assert_eq!(latest[0].run_scope, Some(RunScope::Production));
}
