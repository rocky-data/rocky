//! `rocky policy check --capability run` evaluates `run` rules (#2651).
//!
//! The `run` capability gates an agent's `rocky run` (#2610). `policy check`
//! must accept it, so an operator can ask what a run rule resolves to without
//! running anything.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.shop]
type = "transformation"
models = "models/**"

[pipeline.shop.target]

[policy]
version = 1
default_agent_effect = "deny"

[[policy.rules]]
principal = "agent"
capability = "run"
effect = "allow"
scope = { models = ["orders"] }

[[policy.rules]]
principal = "agent"
capability = "apply"
effect = "allow"
scope = { models = ["returns"] }
"#;

fn project(dir: &Path) {
    fs::write(dir.join("rocky.toml"), CONFIG).unwrap();
    let models = dir.join("models");
    fs::create_dir_all(&models).unwrap();
    for name in ["orders", "returns"] {
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
}

fn check(dir: &Path, capability: &str, model: &str) -> (Output, serde_json::Value) {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args([
            "--output",
            "json",
            "policy",
            "check",
            "--principal",
            "agent",
            "--capability",
            capability,
            "--model",
            model,
            "--models",
        ])
        .arg(dir.join("models"))
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .env_remove("ROCKY_PRINCIPAL")
        .output()
        .expect("spawn rocky");
    let json = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON ({e})\nexit {:?}\nstdout:\n{}\nstderr:\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (out, json)
}

#[test]
fn policy_check_accepts_the_run_capability_and_matches_a_run_rule() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    let (out, v) = check(tmp.path(), "run", "orders");
    assert!(out.status.success(), "{v}");
    assert_eq!(v["capability"], "run", "{v}");
    assert_eq!(v["effect"], "allow", "{v}");
    assert_eq!(v["matched_rule"], 0, "{v}");
}

#[test]
fn a_run_check_outside_the_rule_falls_to_the_default() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    let (_, v) = check(tmp.path(), "run", "returns");
    // `returns` is allowed for `apply` only: an `apply` rule never matches
    // `run`, so the agent default applies.
    assert_eq!(v["capability"], "run", "{v}");
    assert_eq!(v["effect"], "deny", "{v}");
    assert!(v["matched_rule"].is_null(), "{v}");
}
