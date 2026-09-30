//! The backfill CLI must reject compile errors in its resolved model scope.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};

fn project(root: &Path) {
    fs::write(
        root.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"fixture.duckdb\"\n",
    )
    .unwrap();
    fs::create_dir(root.join("models")).unwrap();
}

fn model(root: &Path, name: &str, strategy: &str, sql: &str) {
    let models = root.join("models");
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"{strategy}\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
}

fn backfill(root: &Path, seed: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .args(["--output", "json", "backfill", "--model", seed])
        .output()
        .expect("run rocky backfill")
}

#[test]
fn backfill_refuses_error_diagnostics_in_closure_without_a_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    model(root, "seed", "full_refresh", "SELECT 1 AS id");
    model(root, "refused", "ephemeral", "SELECT id FROM seed");
    model(root, "also_refused", "ephemeral", "SELECT id FROM seed");

    let output = backfill(root, "seed");
    assert!(!output.status.success(), "backfill must refuse E038");
    assert!(output.stdout.is_empty(), "no plan ID may be emitted");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr
            .lines()
            .any(|line| line.starts_with("model 'refused': E038:")),
        "diagnostic must name its model and code: {stderr}"
    );
    assert!(
        stderr
            .lines()
            .any(|line| line.starts_with("model 'also_refused': E038:")),
        "every error diagnostic needs its own line: {stderr}"
    );
    assert!(
        !root.join(".rocky/plans").exists(),
        "a refused backfill must persist no plan"
    );
}

#[test]
fn backfill_ignores_errors_outside_its_scope() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    model(root, "selected", "full_refresh", "SELECT 1 AS id");
    model(root, "unrelated", "ephemeral", "SELECT 2 AS id");

    let output = backfill(root, "selected");
    assert!(output.status.success(), "unrelated E038: {output:?}");
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan["models"], serde_json::json!(["selected"]));
    let plan_id = plan["plan_id"].as_str().expect("backfill plan id");
    assert!(root.join(format!(".rocky/plans/{plan_id}.json")).exists());
}

#[test]
fn backfill_warning_only_project_still_persists_a_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root);
    model(root, "dated", "full_refresh", "SELECT 1 AS id");
    let sidecar = root.join("models/dated.toml");
    fs::OpenOptions::new()
        .append(true)
        .open(sidecar)
        .unwrap()
        .write_all(b"\n[classification]\nid = \"unmapped\"\n")
        .unwrap();

    let compile = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .args(["--output", "json", "compile", "--models", "models"])
        .output()
        .unwrap();
    assert!(compile.status.success(), "compile fixture: {compile:?}");
    let report: serde_json::Value = serde_json::from_slice(&compile.stdout).unwrap();
    assert_eq!(report["has_errors"], false);
    assert!(
        report["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "W004"),
        "fixture must actually emit a warning: {report}"
    );

    let output = backfill(root, "dated");
    assert!(output.status.success(), "backfill warning-only: {output:?}");
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let plan_id = plan["plan_id"].as_str().expect("backfill plan id");
    assert!(root.join(format!(".rocky/plans/{plan_id}.json")).exists());
}
