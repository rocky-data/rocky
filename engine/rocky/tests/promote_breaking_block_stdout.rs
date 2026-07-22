//! Integration test for the bare `rocky branch promote` breaking-change-block
//! stdout contract.
//!
//! Regression guard: when the pre-promote breaking-change gate VETOES a bare
//! `rocky branch promote <name>` (a `Breaking`-severity column drop, no
//! `--allow-breaking`), the verb must exit non-zero AND still write the
//! `BranchPromoteOutput` block JSON to *stdout* — `success = false`, an empty
//! `targets` array, the classified findings under `breaking_changes`, and a
//! `breaking_changes_blocked` audit entry. First-party consumers (the
//! semantic-breaking-change POC / weekly smoke) `json.load` that stdout.
//!
//! The bare verb now internally chains plan + apply via `build_promote_plan_inner`,
//! which returns the block as a typed error; this pins that the stdout JSON is
//! re-emitted rather than swallowed into a stderr-only error. Spawns the real
//! `rocky` binary against a throw-away git repo whose committed model carries a
//! column the working tree drops.

use std::fs;
use std::process::Command;

const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"

[pipeline.t.target.governance]
auto_create_schemas = true
"#;

const MODEL_TOML: &str = r#"name = "fct"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "marts"
table = "fct"
"#;

fn git(dir: &std::path::Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .expect("run git");
    assert!(status.success(), "git {args:?} failed");
}

#[test]
fn bare_promote_breaking_block_writes_block_json_to_stdout() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();

    // Empty DuckDB fixture so config load / branch create never trip on a
    // missing warehouse (the gate blocks before any warehouse access anyway).
    {
        let _ = duckdb::Connection::open(dir.join("warehouse.duckdb")).expect("open duckdb");
    }

    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write rocky.toml");
    let models_dir = dir.join("models");
    fs::create_dir(&models_dir).expect("mkdir models");
    // Base (committed) model: columns (id, amount).
    fs::write(models_dir.join("fct.sql"), "SELECT 1 AS id, 2 AS amount\n")
        .expect("write model sql");
    fs::write(models_dir.join("fct.toml"), MODEL_TOML).expect("write model toml");

    // Self-contained git repo with a local identity, then commit the baseline.
    git(dir, &["init", "-q", "."]);
    git(dir, &["config", "user.email", "test@rocky.invalid"]);
    git(dir, &["config", "user.name", "Rocky Test"]);
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "base"]);

    // Register the branch in the state store.
    let create = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .args(["branch", "create", "fix"])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky branch create");
    assert!(
        create.status.success(),
        "branch create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    // Working tree drops `amount` → a Breaking-severity column drop vs HEAD.
    fs::write(models_dir.join("fct.sql"), "SELECT 1 AS id\n").expect("rewrite model sql");

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("-c")
        .arg(dir.join("rocky.toml"))
        .arg("--output")
        .arg("json")
        .args([
            "branch",
            "promote",
            "fix",
            "--base-ref",
            "HEAD",
            "--models",
            "models",
        ])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky branch promote");

    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");

    // 1. Non-zero exit — the gate VETOED the promote.
    assert!(
        !out.status.success(),
        "a breaking-change block must exit non-zero.\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );

    // 2. The block JSON is on STDOUT (not swallowed into a stderr-only error).
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not the block JSON document: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    });
    assert_eq!(
        parsed.get("success").and_then(serde_json::Value::as_bool),
        Some(false),
        "block output must report success=false, got: {stdout}"
    );
    assert!(
        parsed
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .is_some_and(std::vec::Vec::is_empty),
        "a blocked promote must carry an empty targets array, got: {stdout}"
    );
    assert!(
        parsed
            .get("breaking_changes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|f| !f.is_empty()),
        "the block must carry the breaking findings, got: {stdout}"
    );
    let audit_kinds: Vec<&str> = parsed
        .get("audit")
        .and_then(serde_json::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| e.get("kind").and_then(serde_json::Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        audit_kinds.contains(&"breaking_changes_blocked"),
        "audit must contain breaking_changes_blocked, got {audit_kinds:?}\n{stdout}"
    );
}
