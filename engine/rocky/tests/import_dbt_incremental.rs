//! Regression coverage for unsafe raw dbt incremental guards.

use std::fs;
use std::process::Command;

#[test]
fn no_manifest_refuses_unbounded_append_model() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dbt_dir = tmp.path().join("dbt");
    let models_dir = dbt_dir.join("models");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&models_dir).expect("create dbt models");
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
    assert_eq!(result["imported"], 0);
    assert_eq!(result["failed"], 1);
    assert_eq!(result["failed_details"][0]["name"], "events");
    assert!(
        result["failed_details"][0]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("is_incremental()"))
    );
    assert_eq!(result["emission"]["models_translated_count"], 0);
    assert!(!out_dir.join("models/events.sql").exists());
    assert!(!out_dir.join("models/events.toml").exists());
}
