//! The one freshness grammar (`warn_after` / `error_after`) at every site,
//! the deprecated spellings mapping onto it, and the refusals.

use std::path::Path;

use rocky_core::config::{
    FreshnessConfig, ProjectFreshnessConfig, deprecated_freshness_spellings, parse_rocky_config,
};
use rocky_core::models::{ModelFreshnessConfig, load_models_from_dir};
use rocky_core::source_freshness::FreshnessThresholds;

fn levels(warn: Option<u64>, error: Option<u64>) -> FreshnessThresholds {
    FreshnessThresholds {
        warn_after_seconds: warn,
        error_after_seconds: error,
    }
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

fn model_freshness(extra: &str) -> Result<ModelFreshnessConfig, String> {
    let dir = tempfile::tempdir().expect("temp dir");
    write_model(dir.path(), "m", extra);
    let models = load_models_from_dir(dir.path(), None).map_err(|e| format!("{e:#}"))?;
    Ok(models[0].config.freshness.clone().expect("freshness block"))
}

fn config_with(body: &str) -> Result<rocky_core::config::RockyConfig, String> {
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
{body}
"#
        ),
    )
    .expect("write config");
    parse_rocky_config(&path).map_err(|e| format!("{e}"))
}

// --- pipeline [checks.freshness] -----------------------------------------

#[test]
fn pipeline_check_parses_the_new_form() {
    let cfg: FreshnessConfig =
        toml::from_str("warn_after = \"12h\"\nerror_after = \"1d\"").expect("parses");
    assert_eq!(cfg.thresholds(), levels(Some(43_200), Some(86_400)));
    assert_eq!(cfg.threshold_seconds, None);
    // A new block serializes only what it declares: no defaulted severity.
    assert_eq!(
        serde_json::to_string(&cfg).unwrap(),
        r#"{"warn_after":"12h","error_after":"1d"}"#
    );
}

#[test]
fn pipeline_check_old_form_maps_by_severity_with_error_default() {
    let parse = |t: &str| toml::from_str::<FreshnessConfig>(t).expect("parses");
    assert_eq!(
        parse("threshold_seconds = 60").thresholds(),
        levels(None, Some(60))
    );
    assert_eq!(
        parse("threshold_seconds = 60\nseverity = \"warning\"").thresholds(),
        levels(Some(60), None)
    );
}

#[test]
fn pipeline_check_refuses_mixed_and_inverted_levels() {
    for bad in [
        "threshold_seconds = 60\nwarn_after = \"1h\"",
        "severity = \"warning\"\nerror_after = \"1h\"",
    ] {
        let err = toml::from_str::<FreshnessConfig>(bad)
            .unwrap_err()
            .to_string();
        assert!(err.contains("mixes the deprecated"), "{bad}: {err}");
    }
    let err = toml::from_str::<FreshnessConfig>("warn_after = \"2h\"\nerror_after = \"1h\"")
        .unwrap_err()
        .to_string();
    assert!(err.contains("shorter than `warn_after`"), "{err}");
    let err = toml::from_str::<FreshnessConfig>("warn_after = \"30m\"")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("`warn_after` = \"30m\" does not parse"),
        "{err}"
    );
    assert!(!err.contains("max_lag"), "{err}");
    let err = toml::from_str::<FreshnessConfig>("severity = \"error\"")
        .unwrap_err()
        .to_string();
    assert!(err.contains("warn_after"), "{err}");
}

// --- model sidecar / _defaults.toml ----------------------------------------

#[test]
fn model_sidecar_parses_the_new_form() {
    let f = model_freshness(
        "[freshness]\nwarn_after = \"1h\"\nerror_after = \"6h\"\ntime_column = \"ts\"\n",
    )
    .expect("loads");
    assert_eq!(f.thresholds(), levels(Some(3_600), Some(21_600)));
    assert_eq!(f.budget_seconds(), Some(3_600));
    assert_eq!(f.time_column.as_deref(), Some("ts"));
    assert!(f.legacy_keys().is_empty());
}

#[test]
fn model_sidecar_old_forms_map_by_severity_with_warning_default() {
    let f = model_freshness("[freshness]\nmax_lag_seconds = 60\n").expect("loads");
    assert_eq!(f.thresholds(), levels(Some(60), None));
    let f = model_freshness("[freshness]\nexpected_lag_seconds = 60\nseverity = \"error\"\n")
        .expect("loads");
    assert_eq!(f.thresholds(), levels(None, Some(60)));
    assert_eq!(f.legacy_keys(), vec!["expected_lag_seconds", "severity"]);
}

#[test]
fn model_sidecar_refuses_mixed_and_inverted_levels() {
    let err =
        model_freshness("[freshness]\nmax_lag_seconds = 60\nwarn_after = \"1h\"\n").unwrap_err();
    assert!(err.contains("mixes the deprecated"), "{err}");
    let err =
        model_freshness("[freshness]\nwarn_after = \"2d\"\nerror_after = \"1d\"\n").unwrap_err();
    assert!(err.contains("shorter than `warn_after`"), "{err}");
    let err = model_freshness("[freshness]\ntime_column = \"ts\"\n").unwrap_err();
    assert!(
        err.contains("neither `warn_after` nor `error_after`"),
        "{err}"
    );
}

#[test]
fn defaults_file_parses_the_new_form() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_model(dir.path(), "m", "");
    std::fs::write(
        dir.path().join("_defaults.toml"),
        "[freshness]\nerror_after = \"2h\"\n",
    )
    .unwrap();
    let models = load_models_from_dir(dir.path(), None).expect("loads");
    let f = models[0].config.freshness.as_ref().expect("inherited");
    assert_eq!(f.thresholds(), levels(None, Some(7_200)));
}

// --- project [freshness] ----------------------------------------------------

#[test]
fn project_block_parses_the_new_form_and_is_inherited() {
    let cfg =
        config_with("\n[freshness]\nwarn_after = \"1h\"\ntime_column = \"ts\"\n").expect("parses");
    assert_eq!(cfg.freshness.budget_seconds(), Some(3_600));
    assert!(cfg.freshness.has_default());
    let dir = tempfile::tempdir().expect("temp dir");
    write_model(dir.path(), "m", "");
    let models = load_models_from_dir(dir.path(), Some(&cfg.freshness)).expect("loads");
    let f = models[0].config.freshness.as_ref().expect("inherited");
    assert_eq!(f.thresholds(), levels(Some(3_600), None));
    assert_eq!(f.time_column.as_deref(), Some("ts"));
    assert!(!f.declared_in_sidecar);
}

#[test]
fn project_old_form_inherits_exactly_as_before() {
    for (severity, expected) in [
        (None, levels(Some(900), None)),
        (
            Some(rocky_core::tests::TestSeverity::Warning),
            levels(Some(900), None),
        ),
        (
            Some(rocky_core::tests::TestSeverity::Error),
            levels(None, Some(900)),
        ),
    ] {
        let project = ProjectFreshnessConfig {
            expected_lag_seconds: Some(900),
            severity,
            ..Default::default()
        };
        assert_eq!(project.thresholds(), expected);
        let inherited = ModelFreshnessConfig::from_project_default(&project).expect("inherits");
        assert_eq!(inherited.thresholds(), expected);
    }
}

#[test]
fn project_block_without_a_level_still_parses_and_supplies_nothing() {
    let cfg = config_with("\n[freshness]\ntime_column = \"ts\"\n").expect("parses");
    assert!(!cfg.freshness.has_default());
    assert!(ModelFreshnessConfig::from_project_default(&cfg.freshness).is_none());
}

#[test]
fn project_block_refuses_mixed_and_inverted_levels() {
    let err = config_with("\n[freshness]\nexpected_lag_seconds = 60\nerror_after = \"1h\"\n")
        .unwrap_err();
    assert!(err.contains("mixes the deprecated"), "{err}");
    let err =
        config_with("\n[freshness]\nwarn_after = \"7d\"\nerror_after = \"1d\"\n").unwrap_err();
    assert!(err.contains("shorter than `warn_after`"), "{err}");
}

// --- deprecation report ------------------------------------------------------

#[test]
fn rocky_toml_deprecations_name_the_block_and_the_replacement() {
    let cfg = config_with(
        "\n[pipeline.silver.checks]\nenabled = true\n\n[pipeline.silver.checks.freshness]\nthreshold_seconds = 86400\n\n[freshness]\nexpected_lag_seconds = 3600\nseverity = \"error\"\n",
    )
    .expect("parses");
    let found = deprecated_freshness_spellings(&cfg);
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0].field, "freshness");
    assert_eq!(found[0].keys, vec!["expected_lag_seconds", "severity"]);
    assert_eq!(found[0].replacement, "error_after = \"1h\"");
    assert_eq!(found[1].field, "pipeline.silver.checks.freshness");
    assert_eq!(found[1].keys, vec!["threshold_seconds"]);
    assert_eq!(found[1].replacement, "error_after = \"1d\"");

    let clean = config_with(
        "\n[pipeline.silver.checks]\nenabled = true\n\n[pipeline.silver.checks.freshness]\nerror_after = \"1d\"\n\n[freshness]\nwarn_after = \"1h\"\n",
    )
    .expect("parses");
    assert!(deprecated_freshness_spellings(&clean).is_empty());
}

// --- product spec -------------------------------------------------------------

#[test]
fn spec_grammar_follows_the_shared_rules() {
    use rocky_core::product::spec::FreshnessSpec;
    let spec =
        |max_lag: Option<&str>, severity, warn: Option<&str>, error: Option<&str>| FreshnessSpec {
            max_lag: max_lag.map(str::to_string),
            time_column: "date".to_string(),
            severity,
            warn_after: warn.map(str::to_string),
            error_after: error.map(str::to_string),
        };
    use rocky_core::product::spec::FreshnessSeverity;
    assert_eq!(
        spec(Some("24h"), None, None, None).thresholds().unwrap(),
        levels(Some(86_400), None)
    );
    assert_eq!(
        spec(Some("24h"), Some(FreshnessSeverity::Error), None, None)
            .thresholds()
            .unwrap(),
        levels(None, Some(86_400))
    );
    let new = spec(None, None, Some("12h"), Some("1d"));
    assert_eq!(
        new.thresholds().unwrap(),
        levels(Some(43_200), Some(86_400))
    );
    assert_eq!(new.budget_seconds().unwrap(), 43_200);
    assert!(new.legacy_keys().is_empty());

    // A bad value names the key the user wrote, never `max_lag`.
    let bad = spec(None, None, None, Some("30m"))
        .thresholds()
        .unwrap_err();
    assert!(bad.to_string().contains("`error_after` = \"30m\""), "{bad}");
    assert!(!bad.to_string().contains("max_lag"), "{bad}");
}
