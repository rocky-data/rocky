//! Digest pins for the deprecated freshness spellings.
//!
//! Plan ids hash serialized config: `config_policy_identity` hashes every
//! pipeline (so `[checks.freshness]`), and `execution_ir_fingerprint` hashes
//! each model's whole `ModelConfig` (so its `[freshness]`). An unchanged
//! project must serialize byte-identically, so these tests pin the JSON each
//! old spelling produced before `warn_after` / `error_after` existed. The
//! literals were captured from the engine before that change, not predicted.

use std::path::Path;

use rocky_core::config::{ProjectFreshnessConfig, parse_rocky_config};
use rocky_core::models::load_models_from_dir;

fn pipeline_json(checks_freshness: &str) -> String {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("rocky.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[adapter.db]
type = "duckdb"
path = "wh.duckdb"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "db"

[pipeline.silver.checks]
enabled = true

[pipeline.silver.checks.freshness]
{checks_freshness}
"#
        ),
    )
    .expect("write config");
    let cfg = parse_rocky_config(&path).expect("config parses");
    let value = serde_json::to_value(&cfg.pipelines["silver"]).expect("pipeline serializes");
    serde_json::to_string(&value["checks"]["freshness"]).expect("to string")
}

fn write_model(dir: &Path, name: &str, extra: &str) {
    std::fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ntarget = {{ catalog = \"c\", schema = \"s\", table = \"{name}\" }}\n{extra}"
        ),
    )
    .expect("write sidecar");
    std::fs::write(dir.join(format!("{name}.sql")), "SELECT 1").expect("write sql");
}

/// The `freshness` value of the one model `extra` declares, serialized the
/// way `execution_ir_fingerprint` sees it (inside the whole `ModelConfig`).
fn model_freshness_json(
    extra: &str,
    defaults: Option<&str>,
    project: Option<&ProjectFreshnessConfig>,
) -> String {
    let dir = tempfile::tempdir().expect("temp dir");
    write_model(dir.path(), "m", extra);
    if let Some(defaults) = defaults {
        std::fs::write(dir.path().join("_defaults.toml"), defaults).expect("write defaults");
    }
    let models = load_models_from_dir(dir.path(), project).expect("models load");
    let value = serde_json::to_value(&models[0].config).expect("config serializes");
    serde_json::to_string(&value["freshness"]).expect("to string")
}

#[test]
fn pipeline_checks_freshness_old_spellings_serialize_as_before() {
    assert_eq!(
        pipeline_json("threshold_seconds = 3600"),
        r#"{"severity":"error","threshold_seconds":3600}"#
    );
    assert_eq!(
        pipeline_json("threshold_seconds = 3600\nseverity = \"warning\""),
        r#"{"severity":"warning","threshold_seconds":3600}"#
    );
    assert_eq!(
        pipeline_json("threshold_seconds = 86400\nseverity = \"error\""),
        r#"{"severity":"error","threshold_seconds":86400}"#
    );
}

#[test]
fn model_sidecar_old_spellings_serialize_as_before() {
    assert_eq!(
        model_freshness_json("[freshness]\nmax_lag_seconds = 3600\n", None, None),
        r#"{"max_lag_seconds":3600}"#
    );
    assert_eq!(
        model_freshness_json(
            "[freshness]\nexpected_lag_seconds = 7200\ntime_column = \"ts\"\nseverity = \"error\"\n",
            None,
            None
        ),
        r#"{"max_lag_seconds":7200,"severity":"error","time_column":"ts"}"#
    );
    assert_eq!(
        model_freshness_json(
            "",
            Some("[freshness]\nmax_lag_seconds = 600\nseverity = \"warning\"\n"),
            None
        ),
        r#"{"max_lag_seconds":600,"severity":"warning"}"#
    );
}

#[test]
fn project_inherited_old_spelling_serializes_as_before() {
    let project = ProjectFreshnessConfig {
        expected_lag_seconds: Some(1800),
        time_column: Some("updated_at".to_string()),
        severity: Some(rocky_core::tests::TestSeverity::Error),
        ..Default::default()
    };
    assert_eq!(
        model_freshness_json("", None, Some(&project)),
        r#"{"max_lag_seconds":1800,"severity":"error","time_column":"updated_at"}"#
    );
    assert_eq!(
        serde_json::to_string(&project).expect("project serializes"),
        r#"{"expected_lag_seconds":1800,"time_column":"updated_at","severity":"error"}"#
    );
}
