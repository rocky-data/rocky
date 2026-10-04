//! Regression coverage for raw dbt incremental guards.

use std::fs;
use std::path::Path;
use std::process::Command;

fn write_project_files(dbt_dir: &Path) {
    fs::write(
        dbt_dir.join("dbt_project.yml"),
        "name: incremental_repro\nversion: '1.0'\nprofile: incremental_repro\n",
    )
    .expect("write dbt project");
    fs::write(
        dbt_dir.join("profiles.yml"),
        "incremental_repro:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: fixture.duckdb\n      schema: main\n",
    )
    .expect("write dbt profile");
}

#[test]
/// WP6: the standard `is_incremental()` watermark filter converts to a Rocky
/// `incremental` model on the raw path. Before, the model was refused: an
/// unfiltered append would re-insert every row. The placeholder is `TRUE` on
/// the first run, so the first load is complete.
fn no_manifest_converts_standard_incremental_filter() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dbt_dir = tmp.path().join("dbt");
    let models_dir = dbt_dir.join("models");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&models_dir).expect("create dbt models");
    write_project_files(&dbt_dir);
    fs::write(
        models_dir.join("events.sql"),
        r#"{{ config(materialized='incremental') }}
SELECT *
FROM raw.events
{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
"#,
    )
    .expect("write incremental model");

    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "import-dbt", "--dbt-project"])
        .arg(&dbt_dir)
        .arg("--output-dir")
        .arg(&out_dir)
        .arg("--no-manifest")
        .env("RUST_LOG", "error")
        .output()
        .expect("run import-dbt");

    assert!(
        output.status.success(),
        "import-dbt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("import-dbt output should be JSON");
    assert_eq!(result["imported"], 1, "{result}");
    assert_eq!(result["failed"], 0, "{result}");
    let sql = fs::read_to_string(out_dir.join("models/events.sql")).expect("emitted sql");
    assert!(sql.contains("WHERE @incremental_filter"), "{sql}");
    assert!(!sql.contains("is_incremental"), "{sql}");
    let sidecar = fs::read_to_string(out_dir.join("models/events.toml")).expect("sidecar");
    assert!(sidecar.contains("type = \"incremental\""), "{sidecar}");
    assert!(
        sidecar.contains("timestamp_column = \"event_time\""),
        "{sidecar}"
    );
}

#[test]
fn raw_manifest_refuses_aliased_incremental_macro() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dbt_dir = tmp.path().join("dbt");
    let target_dir = dbt_dir.join("target");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&target_dir).expect("create dbt target");
    write_project_files(&dbt_dir);

    let manifest = serde_json::json!({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json",
            "dbt_version": "1.10.0",
            "project_name": "incremental_repro"
        },
        "nodes": {
            "model.incremental_repro.events": {
                "unique_id": "model.incremental_repro.events",
                "name": "events",
                "resource_type": "model",
                "raw_code": r#"{% set incremental_check = is_incremental %}
SELECT * FROM raw.events
{% if incremental_check() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}"#,
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "incremental" },
                "columns": {},
                "tags": [],
                "schema": "main",
                "database": "fixture"
            }
        },
        "sources": {}
    });
    fs::write(
        target_dir.join("manifest.json"),
        serde_json::to_vec(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");

    let output = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "import-dbt", "--dbt-project"])
        .arg(&dbt_dir)
        .arg("--output-dir")
        .arg(&out_dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run import-dbt");

    assert!(
        output.status.success(),
        "import-dbt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("import-dbt output should be JSON");
    assert_eq!(result["import_method"], "Manifest");
    assert_eq!(result["imported"], 0);
    assert_eq!(result["failed"], 1);
    assert_eq!(result["failed_details"][0]["name"], "events");
    assert!(
        result["failed_details"][0]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("dbt compile --full-refresh"))
    );
    assert_eq!(result["emission"]["models_translated_count"], 0);
    assert!(!out_dir.join("models/events.sql").exists());
    assert!(!out_dir.join("models/events.toml").exists());
}
