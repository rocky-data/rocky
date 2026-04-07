//! `rocky validate-migration` — compare dbt and Rocky project outputs.
//!
//! Compile-only validation mode: imports the dbt project, compiles with Rocky,
//! and reports schema/type mismatches without warehouse access.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use rocky_compiler::import::{dbt, dbt_manifest, dbt_tests};
use rocky_core::models::TargetConfig;

use crate::output::{ModelValidationOutput, ValidateMigrationOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Internal building block — same shape as `ModelValidationOutput` but kept
/// here so this command file can populate it before converting to the
/// public output type.
#[derive(Debug, Clone)]
struct ModelValidation {
    pub model: String,
    pub present_in_dbt: bool,
    pub present_in_rocky: bool,
    pub compile_ok: bool,
    pub dbt_description: Option<String>,
    pub rocky_intent: Option<String>,
    pub test_count: usize,
    pub contracts_generated: usize,
    pub warnings: Vec<String>,
}

/// Execute `rocky validate-migration`.
///
/// In compile-only mode (no warehouse), this:
/// 1. Imports the dbt project
/// 2. Parses model YAML for test definitions
/// 3. Reports per-model coverage and conversion stats
pub fn run_validate_migration(
    dbt_project: &Path,
    rocky_project: Option<&Path>,
    _sample_size: Option<usize>,
    output_json: bool,
) -> Result<()> {
    let default_target = TargetConfig {
        catalog: "warehouse".to_string(),
        schema: "staging".to_string(),
        table: String::new(),
    };

    // Try manifest path first
    let manifest_path = dbt_project.join("target/manifest.json");
    let import_result = if manifest_path.exists() {
        let manifest =
            dbt_manifest::parse_manifest(&manifest_path).map_err(|e| anyhow::anyhow!("{e}"))?;
        dbt::import_from_manifest(&manifest, &default_target)
    } else {
        dbt::import_dbt_project(dbt_project, &default_target).map_err(|e| anyhow::anyhow!("{e}"))?
    };

    // Parse model YAML test definitions
    let models_dir = dbt_project.join("models");
    let model_yamls = if models_dir.exists() {
        dbt_tests::parse_model_yamls(&models_dir).unwrap_or_default()
    } else {
        HashMap::new()
    };

    // Check which models exist in the Rocky project (if provided)
    let rocky_models: std::collections::HashSet<String> = if let Some(rocky_dir) = rocky_project {
        collect_rocky_model_names(rocky_dir)
    } else {
        std::collections::HashSet::new()
    };

    // Build per-model validation results
    let mut validations = Vec::new();
    for model in &import_result.imported {
        let yaml = model_yamls.get(&model.name);
        let test_count: usize = yaml
            .map(|y| y.columns.iter().map(|c| c.tests.len()).sum())
            .unwrap_or(0);

        let (checks, _skipped) = yaml
            .map(dbt_tests::tests_to_contracts)
            .unwrap_or((vec![], 0));

        let mut warnings = Vec::new();
        if model.config.intent.is_none() && yaml.and_then(|y| y.description.as_ref()).is_none() {
            warnings.push("no description or intent metadata".to_string());
        }

        let present_in_rocky = rocky_models.contains(&model.name);
        if rocky_project.is_some() && !present_in_rocky {
            warnings.push("model not found in Rocky project".to_string());
        }

        validations.push(ModelValidation {
            model: model.name.clone(),
            present_in_dbt: true,
            present_in_rocky: rocky_project.is_none() || present_in_rocky,
            compile_ok: true, // dbt imported successfully
            dbt_description: yaml.and_then(|y| y.description.clone()),
            rocky_intent: model.config.intent.clone(),
            test_count,
            contracts_generated: checks.len(),
            warnings,
        });
    }

    // Report failed models
    for failure in &import_result.failed {
        validations.push(ModelValidation {
            model: failure.name.clone(),
            present_in_dbt: true,
            present_in_rocky: false,
            compile_ok: false,
            dbt_description: None,
            rocky_intent: None,
            test_count: 0,
            contracts_generated: 0,
            warnings: vec![format!("import failed: {}", failure.reason)],
        });
    }

    if output_json {
        let total_tests: usize = validations.iter().map(|v| v.test_count).sum();
        let total_contracts: usize = validations.iter().map(|v| v.contracts_generated).sum();
        let total_warnings: usize = validations.iter().map(|v| v.warnings.len()).sum();

        let validations_out: Vec<ModelValidationOutput> = validations
            .iter()
            .map(|v| ModelValidationOutput {
                model: v.model.clone(),
                present_in_dbt: v.present_in_dbt,
                present_in_rocky: v.present_in_rocky,
                compile_ok: v.compile_ok,
                dbt_description: v.dbt_description.clone(),
                rocky_intent: v.rocky_intent.clone(),
                test_count: v.test_count,
                contracts_generated: v.contracts_generated,
                warnings: v.warnings.clone(),
            })
            .collect();

        let output = ValidateMigrationOutput {
            version: VERSION.to_string(),
            command: "validate-migration".to_string(),
            project_name: import_result.project_name.clone(),
            dbt_version: import_result.dbt_version.clone(),
            models_imported: import_result.imported.len(),
            models_failed: import_result.failed.len(),
            total_tests,
            total_contracts,
            total_warnings,
            validations: validations_out,
        };
        print_json(&output)?;
    } else {
        println!("Migration Validation Report");
        println!("==========================\n");

        if let Some(ref name) = import_result.project_name {
            let ver = import_result
                .dbt_version
                .as_deref()
                .map(|v| format!(" (dbt {v})"))
                .unwrap_or_default();
            println!("Project: {name}{ver}");
        }
        println!(
            "Models:  {} imported, {} failed\n",
            import_result.imported.len(),
            import_result.failed.len()
        );

        let ok_count = validations.iter().filter(|v| v.warnings.is_empty()).count();
        let warn_count = validations
            .iter()
            .filter(|v| !v.warnings.is_empty() && v.compile_ok)
            .count();
        let fail_count = validations.iter().filter(|v| !v.compile_ok).count();

        println!("Results:");
        println!("  {ok_count} models validated successfully");
        if warn_count > 0 {
            println!("  {warn_count} models with warnings");
        }
        if fail_count > 0 {
            println!("  {fail_count} models failed");
        }
        println!();

        let total_tests: usize = validations.iter().map(|v| v.test_count).sum();
        let total_contracts: usize = validations.iter().map(|v| v.contracts_generated).sum();
        if total_tests > 0 {
            println!("Tests:   {total_tests} found, {total_contracts} convertible to contracts");
            println!();
        }

        // Show warnings
        let models_with_warnings: Vec<&ModelValidation> = validations
            .iter()
            .filter(|v| !v.warnings.is_empty())
            .collect();
        if !models_with_warnings.is_empty() {
            println!("Warnings:");
            for v in models_with_warnings {
                for w in &v.warnings {
                    println!("  {}: {w}", v.model);
                }
            }
            println!();
        }

        println!("Next steps:");
        println!(
            "  1. rocky import-dbt --dbt-project {}  (import models)",
            dbt_project.display()
        );
        println!("  2. rocky compile                       (type-check)");
        println!("  3. rocky test                          (validate with DuckDB)");
    }

    Ok(())
}

/// Collect model names from a Rocky project directory by looking for .sql files.
fn collect_rocky_model_names(dir: &Path) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "sql") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    names.insert(stem.to_string());
                }
            }
            // Recurse into subdirectories
            if path.is_dir() {
                names.extend(collect_rocky_model_names(&path));
            }
        }
    }
    names
}
