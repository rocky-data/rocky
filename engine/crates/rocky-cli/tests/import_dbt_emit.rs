//! Integration test for `rocky import-dbt --output-dir <out>`.
//!
//! Drives the importer end-to-end against the rich dbt fixture under
//! `tests/fixtures/dbt-rich/` and asserts that the emitted directory layout
//! is what the CLI promises:
//!
//! - `rocky.toml` exists and parses as a valid Rocky config.
//! - `models/` contains a `_defaults.toml` and one sidecar pair per dbt model.
//! - `seeds/` mirrors the source `seeds/` verbatim.
//! - `MIGRATION-NOTES.md` exists and lists the known limitations.
//! - The translated model sidecars use the documented mapping
//!   (`view → ephemeral`, `incremental → merge|incremental`, default → full_refresh).
//! - `dbt_packages/` and `snapshots/` trees are left untouched (no models, no failures).
//! - A model with `{% if target.name %}` lands as imported + JinjaControlFlow warning
//!   with the `dbt-jinja-not-translated` marker in the emitted SQL.
//! - The translated models pass `rocky-core` model loading (the closest "compiles"
//!   signal we get without spinning up DuckDB, and what `rocky compile` walks first).

use std::collections::BTreeSet;
use std::path::PathBuf;

use rocky_compiler::import::{
    dbt::{self, ImportResult},
    dbt_profiles,
    emit::{self, EmitInputs, OverwritePolicy},
};
use rocky_core::config;
use rocky_core::models::{StrategyConfig, TargetConfig};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt-rich")
}

fn collect_view_flattened(result: &ImportResult) -> BTreeSet<String> {
    result
        .warnings
        .iter()
        .filter(|w| {
            matches!(w.category, dbt::WarningCategory::UnsupportedMaterialization)
                && w.message.contains("'view'")
        })
        .map(|w| w.model.clone())
        .collect()
}

#[test]
fn emit_runnable_repo_from_rich_fixture() {
    let dbt_dir = fixture_root();
    let out_dir = tempfile::TempDir::new().unwrap();

    // Resolve adapter from profiles.yml.
    let profile =
        dbt_profiles::resolve_from_project(&dbt_dir).expect("dbt-rich fixture ships profiles.yml");
    assert_eq!(profile.kind, dbt_profiles::AdapterKind::DuckDb);

    let default_target = TargetConfig {
        catalog: profile
            .database
            .clone()
            .unwrap_or_else(|| "warehouse".to_string()),
        schema: profile.schema.clone().unwrap_or_else(|| "main".to_string()),
        table: String::new(),
    };

    // Run regex-based importer (the rich fixture deliberately has no manifest.json).
    let result = dbt::import_dbt_project(&dbt_dir, &default_target).expect("importer runs cleanly");
    assert!(
        result.imported.iter().any(|m| m.name == "stg_customers"),
        "stg_customers should be in imported models"
    );
    assert!(
        result.imported.iter().any(|m| m.name == "stg_orders"),
        "stg_orders should be in imported models"
    );
    assert!(
        result.imported.iter().any(|m| m.name == "fct_orders"),
        "fct_orders should be in imported models"
    );

    let view_models = collect_view_flattened(&result);
    assert!(
        view_models.contains("stg_customers"),
        "stg_customers materialized='view' should be flagged for ephemeral rewrite"
    );

    let emission = emit::emit_repo(&EmitInputs {
        dbt_project_dir: &dbt_dir,
        out_dir: out_dir.path(),
        overwrite: OverwritePolicy::ReplaceContents,
        profile: &profile,
        default_catalog: &default_target.catalog,
        default_schema: &default_target.schema,
        import: &result,
        view_models_to_make_ephemeral: view_models,
        adapter_override_label: None,
    })
    .expect("emit_repo writes a runnable repo");

    // Layout assertions.
    let rocky_toml = out_dir.path().join("rocky.toml");
    let models_dir = out_dir.path().join("models");
    let defaults = models_dir.join("_defaults.toml");
    let migration_notes = out_dir.path().join("MIGRATION-NOTES.md");
    let seeds_dir = out_dir.path().join("seeds");

    assert!(rocky_toml.exists(), "rocky.toml must exist");
    assert!(defaults.exists(), "_defaults.toml must exist");
    assert!(migration_notes.exists(), "MIGRATION-NOTES.md must exist");
    assert!(
        seeds_dir.join("regions.csv").exists(),
        "seeds copied verbatim"
    );
    assert_eq!(emission.seeds_copied, 1);
    assert!(emission.models_translated >= 4);

    // GA regression: `dbt_packages/` and `snapshots/` trees sit outside the
    // walked `models/` directory; the importer must leave them alone — no
    // ghost model imports, no failure entries.
    assert!(
        !result.imported.iter().any(|m| m.name == "some_macro"),
        "dbt_packages/.../some_macro.sql must not surface as an imported model"
    );
    assert!(
        !result.failed.iter().any(|f| f.name == "some_macro"),
        "dbt_packages/.../some_macro.sql must not surface as a failure"
    );
    assert!(
        !result.imported.iter().any(|m| m.name == "orders_snapshot"),
        "snapshots/orders_snapshot.sql must not surface as an imported model"
    );
    assert!(
        !result.failed.iter().any(|f| f.name == "orders_snapshot"),
        "snapshots/orders_snapshot.sql must not surface as a failure"
    );

    // GA regression: a model with `{% if target.name == 'prod' %}` must
    // import cleanly, surface a JinjaControlFlow warning, and carry the
    // documented TODO marker in the emitted SQL.
    assert!(
        result.imported.iter().any(|m| m.name == "env_branched"),
        "env_branched.sql must be in imported models"
    );
    assert!(
        result.warnings.iter().any(|w| w.model == "env_branched"
            && matches!(w.category, dbt::WarningCategory::JinjaControlFlow)),
        "env_branched must surface a JinjaControlFlow warning for `{{% if target.name %}}`"
    );
    let env_branched_sql = std::fs::read_to_string(models_dir.join("env_branched.sql")).unwrap();
    assert!(
        env_branched_sql.contains("TODO: dbt-jinja-not-translated"),
        "env_branched.sql must carry the dbt-jinja-not-translated marker"
    );

    // Each model has a sidecar pair.
    for name in ["stg_customers", "stg_orders", "fct_orders", "env_branched"] {
        let sql = models_dir.join(format!("{name}.sql"));
        let toml = models_dir.join(format!("{name}.toml"));
        assert!(sql.exists(), "{name}.sql exists");
        assert!(toml.exists(), "{name}.toml exists");
    }

    // Strategy mapping check.
    let stg_customers_toml =
        std::fs::read_to_string(models_dir.join("stg_customers.toml")).unwrap();
    assert!(
        stg_customers_toml.contains("type = \"ephemeral\""),
        "view → ephemeral mapping must apply"
    );
    let stg_orders_toml = std::fs::read_to_string(models_dir.join("stg_orders.toml")).unwrap();
    // incremental + unique_key → merge in the existing importer
    assert!(
        stg_orders_toml.contains("type = \"merge\"")
            || stg_orders_toml.contains("type = \"incremental\""),
        "incremental → merge|incremental mapping must apply, got: {stg_orders_toml}"
    );
    let fct_orders_toml = std::fs::read_to_string(models_dir.join("fct_orders.toml")).unwrap();
    assert!(
        fct_orders_toml.contains("type = \"full_refresh\""),
        "table → full_refresh mapping must apply"
    );

    // rocky.toml must parse via the same loader the engine uses.
    let parsed = config::load_rocky_config(&rocky_toml)
        .expect("emitted rocky.toml parses via the engine config loader");
    let _ = parsed; // silence unused

    // Translated models must load via the canonical model loader. This is the
    // closest analog to "compiles" without booting DuckDB — model loading is
    // the first phase of `rocky compile` and what catches sidecar shape errors.
    let models = rocky_core::models::load_models_from_dir(&models_dir)
        .expect("translated models load via the standard sidecar loader");
    let names: Vec<&str> = models.iter().map(|m| m.config.name.as_str()).collect();
    assert!(names.contains(&"stg_customers"));
    assert!(names.contains(&"stg_orders"));
    assert!(names.contains(&"fct_orders"));

    // Concrete strategy assertions on the loaded representation.
    let stg_customers = models
        .iter()
        .find(|m| m.config.name == "stg_customers")
        .unwrap();
    assert!(matches!(
        stg_customers.config.strategy,
        StrategyConfig::Ephemeral
    ));
    let fct_orders = models
        .iter()
        .find(|m| m.config.name == "fct_orders")
        .unwrap();
    assert!(matches!(
        fct_orders.config.strategy,
        StrategyConfig::FullRefresh
    ));

    // MIGRATION-NOTES content checks.
    let notes = std::fs::read_to_string(&migration_notes).unwrap();
    assert!(
        notes.contains("Known limitations"),
        "GA framing: MIGRATION-NOTES must use 'Known limitations' heading"
    );
    assert!(
        !notes.to_lowercase().contains("v0"),
        "GA framing: MIGRATION-NOTES must not reference 'v0'"
    );
    assert!(notes.contains("Required env vars"));
    assert!(notes.contains("dbt generic tests"));

    // Generic-test mapping: the four canonical built-ins land as
    // `[[tests]]` blocks on each model sidecar. Non-canonical tests
    // (e.g. `dbt_utils.accepted_range`) must NOT appear in the toml —
    // they are surfaced as structured import warnings instead.
    let stg_orders_toml = std::fs::read_to_string(models_dir.join("stg_orders.toml")).unwrap();
    assert!(
        stg_orders_toml.contains("[[tests]]"),
        "stg_orders.toml should carry generic-test [[tests]] blocks"
    );
    assert!(stg_orders_toml.contains("type = \"unique\""));
    assert!(stg_orders_toml.contains("type = \"not_null\""));
    assert!(stg_orders_toml.contains("type = \"relationships\""));
    assert!(stg_orders_toml.contains("to_column = \"customer_id\""));
    // Relationships FQN should resolve via the imported-target map for
    // stg_customers (which lives in the default schema).
    assert!(
        stg_orders_toml.contains("to_table = \""),
        "relationships test must include a fully-qualified to_table"
    );
    // Non-canonical test must NOT be stubbed in the emitted toml.
    assert!(
        !stg_orders_toml.contains("dbt_utils"),
        "non-canonical tests must not be stubbed as TODO entries; emitted toml should be clean"
    );
    assert!(
        !stg_orders_toml.to_lowercase().contains("accepted_range"),
        "non-canonical tests must not be stubbed as TODO entries"
    );

    let stg_customers_toml_again =
        std::fs::read_to_string(models_dir.join("stg_customers.toml")).unwrap();
    assert!(stg_customers_toml_again.contains("type = \"accepted_values\""));
    assert!(stg_customers_toml_again.contains("\"active\""));

    // Loaded via the canonical model loader, the tests round-trip into the
    // typed TestDecl surface — confirming the emitted [[tests]] toml is
    // wire-compatible with rocky-core.
    let stg_orders = models
        .iter()
        .find(|m| m.config.name == "stg_orders")
        .unwrap();
    assert!(
        !stg_orders.config.tests.is_empty(),
        "stg_orders should carry at least one TestDecl after loading"
    );

    // The non-canonical test must surface as a structured warning whose
    // category is `UnsupportedTest` (not silently dropped).
    assert!(
        result.warnings.iter().any(
            |w| matches!(w.category, dbt::WarningCategory::UnsupportedTest)
                && w.message.contains("dbt_utils.accepted_range")
        ),
        "expected an UnsupportedTest warning for dbt_utils.accepted_range"
    );

    // Counter check: the four canonical mappings flow through `tests_converted`.
    // Total tests in the fixture: order_id × {unique, not_null}, customer_id ×
    // {not_null, relationships}, customer_id × {unique, not_null}, status ×
    // accepted_values, amount × dbt_utils.accepted_range.
    assert_eq!(result.tests_found, 8, "all schema.yml tests are counted");
    assert_eq!(result.tests_skipped, 1, "one non-canonical test skipped");
    assert_eq!(
        result.tests_converted, 7,
        "seven canonical tests converted to TestDecls"
    );
}
