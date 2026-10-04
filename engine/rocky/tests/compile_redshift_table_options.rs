//! Integration test for `rocky compile`'s `[redshift]` table-option checks
//! (E052 error, W052 warning).
//!
//! Spawns the real `rocky` binary against a models-only fixture (no
//! credentials, no warehouse). Proves:
//!
//! - a contradictory `[redshift]` block (`dist_style = "key"` with no
//!   `dist_key`) is an E052 error and sets `has_errors`;
//! - `[redshift]` on a `view` is an E052 error;
//! - a `dist_key` naming a column the model does not output is a W052
//!   warning only — the build stays clean;
//! - valid controls: a correct `[redshift]` block on a table model, and a
//!   model with no `[redshift]` block, emit neither code.

use std::fs;
use std::path::Path;
use std::process::Command;

fn compile_json(models_dir: &Path) -> serde_json::Value {
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
        panic!("stdout is not one JSON document: {e}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
    })
}

fn write_model(dir: &Path, name: &str, sql: &str, toml: &str) {
    fs::write(dir.join(format!("{name}.sql")), sql).expect("write sql");
    fs::write(dir.join(format!("{name}.toml")), toml).expect("write toml");
}

fn codes_for<'a>(parsed: &'a serde_json::Value, model: &str) -> Vec<(&'a str, &'a str)> {
    parsed
        .get("diagnostics")
        .and_then(|v| v.as_array())
        .expect("diagnostics array")
        .iter()
        .filter(|d| d.get("model").and_then(|m| m.as_str()) == Some(model))
        .filter_map(|d| Some((d.get("code")?.as_str()?, d.get("severity")?.as_str()?)))
        .collect()
}

const TARGET: &str = "[target]\ncatalog = \"dev\"\nschema = \"marts\"\n";

#[test]
fn contradictory_options_and_view_strategy_are_e052() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("mkdir");
    write_model(
        &models,
        "fct_bad_dist",
        "SELECT 1 AS customer_id, 2 AS amount\n",
        &format!("name = \"fct_bad_dist\"\n{TARGET}\n[redshift]\ndist_style = \"key\"\n"),
    );
    write_model(
        &models,
        "v_orders",
        "SELECT 1 AS customer_id\n",
        &format!(
            "name = \"v_orders\"\n[strategy]\ntype = \"view\"\n{TARGET}\n[redshift]\nsort_key = [\"customer_id\"]\n"
        ),
    );

    let parsed = compile_json(&models);
    assert_eq!(
        parsed
            .get("has_errors")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "{parsed}"
    );
    assert!(
        codes_for(&parsed, "fct_bad_dist").contains(&("E052", "Error")),
        "{parsed}"
    );
    assert!(
        codes_for(&parsed, "v_orders").contains(&("E052", "Error")),
        "{parsed}"
    );
}

#[test]
fn missing_key_column_warns_and_valid_controls_stay_clean() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let models = tmp.path().join("models");
    fs::create_dir(&models).expect("mkdir");
    // W052: `region` is not in the output.
    write_model(
        &models,
        "fct_typo",
        "SELECT 1 AS customer_id, 2 AS amount\n",
        &format!("name = \"fct_typo\"\n{TARGET}\n[redshift]\ndist_key = \"region\"\n"),
    );
    // Valid: key + compound sort on real output columns, merge strategy.
    write_model(
        &models,
        "fct_good",
        "SELECT 1 AS customer_id, DATE '2026-01-01' AS order_date, 2 AS amount\n",
        &format!(
            "name = \"fct_good\"\n[strategy]\ntype = \"merge\"\nunique_key = [\"customer_id\"]\n{TARGET}\n\
             [redshift]\ndist_key = \"customer_id\"\nsort_key = [\"order_date\", \"customer_id\"]\n"
        ),
    );
    // Valid: no `[redshift]` block at all.
    write_model(
        &models,
        "fct_plain",
        "SELECT 1 AS id\n",
        &format!("name = \"fct_plain\"\n{TARGET}"),
    );

    let parsed = compile_json(&models);
    assert_eq!(
        parsed
            .get("has_errors")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "W052 must not fail the build: {parsed}"
    );
    assert!(
        codes_for(&parsed, "fct_typo").contains(&("W052", "Warning")),
        "{parsed}"
    );
    for model in ["fct_good", "fct_plain"] {
        let codes = codes_for(&parsed, model);
        assert!(
            !codes.iter().any(|(c, _)| *c == "E052" || *c == "W052"),
            "{model} must stay clean: {codes:?}"
        );
    }
}
