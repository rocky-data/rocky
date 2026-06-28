//! Integration test for the `rocky compile` managed-Iceberg `format_options`
//! diagnostic (E035, FR-044).
//!
//! Proves the validation is reachable end-to-end from the real CLI — not just
//! a unit-tested helper. A model declaring `format = "iceberg_table"` with
//! both `partition_by` and `cluster_by` (a combo Databricks managed Iceberg
//! rejects at the warehouse) must surface a clear `rocky compile` diagnostic
//! that names both options, with `has_errors = true`, *before* any warehouse
//! call. A model declaring only one of the two compiles clean.
//!
//! Spawns the real `rocky` binary against a models-only fixture (no
//! `rocky.toml` needed — `rocky compile` tolerates its absence).

use std::fs;
use std::process::Command;

const BAD_SQL: &str = "SELECT 1 AS id\n";

/// Iceberg model with both partition_by and cluster_by — rejected (E035).
const BAD_TOML: &str = r#"
name = "fct_bad"
format = "iceberg_table"

[target]
catalog = "warehouse"
schema = "silver"

[format_options]
partition_by = ["event_date"]
cluster_by = ["user_id"]
"#;

const GOOD_SQL: &str = "SELECT 1 AS id\n";

/// Iceberg model with partitioning only — valid.
const GOOD_TOML: &str = r#"
name = "fct_good"
format = "iceberg_table"

[target]
catalog = "warehouse"
schema = "silver"

[format_options]
partition_by = ["event_date"]
"#;

fn compile_json(models_dir: &std::path::Path) -> serde_json::Value {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("compile")
        .arg("--models")
        .arg(models_dir)
        .arg("--output")
        .arg("json")
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky compile");

    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(out.stderr).expect("utf8 stderr");
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("stdout is not a single JSON document: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    })
}

#[test]
fn compile_rejects_iceberg_partition_plus_cluster_with_e035() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("mkdir models");
    fs::write(models.join("fct_bad.sql"), BAD_SQL).expect("write bad sql");
    fs::write(models.join("fct_bad.toml"), BAD_TOML).expect("write bad toml");

    let parsed = compile_json(&models);

    assert_eq!(
        parsed.get("command").and_then(|v| v.as_str()),
        Some("compile"),
        "expected a CompileOutput on stdout, got: {parsed}"
    );
    assert_eq!(
        parsed
            .get("has_errors")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "managed-Iceberg partition + cluster must set has_errors: {parsed}"
    );

    let diagnostics = parsed
        .get("diagnostics")
        .and_then(|v| v.as_array())
        .expect("diagnostics array");
    let e035 = diagnostics
        .iter()
        .find(|d| d.get("code").and_then(|c| c.as_str()) == Some("E035"))
        .unwrap_or_else(|| panic!("expected an E035 diagnostic, got: {diagnostics:?}"));

    let message = e035
        .get("message")
        .and_then(|m| m.as_str())
        .expect("diagnostic message");
    // The diagnostic must NAME both offending options so the fix is obvious.
    assert!(
        message.contains("partition_by"),
        "E035 message must name partition_by: {message}"
    );
    assert!(
        message.contains("cluster_by"),
        "E035 message must name cluster_by: {message}"
    );
    assert_eq!(
        e035.get("severity").and_then(|s| s.as_str()),
        Some("Error"),
        "E035 must be an Error: {e035:?}"
    );
}

#[test]
fn compile_accepts_iceberg_partition_only() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("mkdir models");
    fs::write(models.join("fct_good.sql"), GOOD_SQL).expect("write good sql");
    fs::write(models.join("fct_good.toml"), GOOD_TOML).expect("write good toml");

    let parsed = compile_json(&models);

    let diagnostics = parsed
        .get("diagnostics")
        .and_then(|v| v.as_array())
        .expect("diagnostics array");
    assert!(
        !diagnostics
            .iter()
            .any(|d| d.get("code").and_then(|c| c.as_str()) == Some("E035")),
        "a partition-only iceberg_table model must not emit E035, got: {diagnostics:?}"
    );
}
