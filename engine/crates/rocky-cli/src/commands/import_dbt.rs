//! `rocky import-dbt` — import a dbt project as Rocky models.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use rocky_compiler::import::{dbt, dbt_manifest, dbt_tests, report};
use rocky_core::models::TargetConfig;

use crate::output::{ImportDbtFailure, ImportDbtOutput, ImportDbtWarning, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky import-dbt`.
pub fn run_import_dbt(
    dbt_project: &Path,
    output_dir: &Path,
    manifest_path: Option<&Path>,
    no_manifest: bool,
    output_json: bool,
) -> Result<()> {
    let default_target = TargetConfig {
        catalog: "warehouse".to_string(),
        schema: "staging".to_string(),
        table: String::new(),
    };

    // Determine import path: manifest vs regex
    let result = if no_manifest {
        dbt::import_dbt_project(dbt_project, &default_target).map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        // Try manifest path
        let manifest_file = match manifest_path {
            Some(p) => {
                if p.exists() {
                    Some(p.to_path_buf())
                } else {
                    anyhow::bail!("manifest file not found: {}", p.display());
                }
            }
            None => {
                // Auto-detect: check <dbt_project>/target/manifest.json
                let default = dbt_project.join("target/manifest.json");
                if default.exists() {
                    Some(default)
                } else {
                    None
                }
            }
        };

        if let Some(ref mf) = manifest_file {
            let manifest = dbt_manifest::parse_manifest(mf).map_err(|e| anyhow::anyhow!("{e}"))?;
            dbt::import_from_manifest(&manifest, &default_target)
        } else {
            dbt::import_dbt_project(dbt_project, &default_target)
                .map_err(|e| anyhow::anyhow!("{e}"))?
        }
    };

    // Write imported models
    if !result.imported.is_empty() {
        dbt::write_imported_models(&result.imported, output_dir)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // Phase 2: Parse model YAMLs and write contracts
    let models_dir = dbt_project.join("models");
    let mut test_stats = report::TestConversionStats {
        found: result.tests_found,
        converted: result.tests_converted,
        converted_custom: result.tests_converted_custom,
        skipped: result.tests_skipped,
        by_type: HashMap::new(),
    };

    if models_dir.exists() {
        if let Ok(model_yamls) = dbt_tests::parse_model_yamls(&models_dir) {
            for (model_name, model_yaml) in &model_yamls {
                // Count test types for reporting
                for col in &model_yaml.columns {
                    for test in &col.tests {
                        let type_name = match test {
                            dbt_tests::DbtTestDef::Simple(s) => s.clone(),
                            dbt_tests::DbtTestDef::Configured { name, .. } => name.clone(),
                        };
                        *test_stats.by_type.entry(type_name).or_default() += 1;
                    }
                }

                // Convert and write contracts
                let (checks, _) = dbt_tests::tests_to_contracts(model_yaml);
                if !checks.is_empty() {
                    if let Err(e) = dbt_tests::write_contracts(model_name, &checks, output_dir) {
                        tracing::warn!("failed to write contracts for {model_name}: {e}");
                    }
                }
            }
        }
    }

    if output_json {
        let warning_details: Vec<ImportDbtWarning> = result
            .warnings
            .iter()
            .map(|w| ImportDbtWarning {
                model: w.model.clone(),
                category: format!("{:?}", w.category),
                message: w.message.clone(),
                suggestion: w.suggestion.clone(),
            })
            .collect();

        let failed_details: Vec<ImportDbtFailure> = result
            .failed
            .iter()
            .map(|f| ImportDbtFailure {
                name: f.name.clone(),
                reason: f.reason.clone(),
            })
            .collect();

        let migration_report = report::generate_report(&result, Some(&test_stats), None);

        let output = ImportDbtOutput {
            version: VERSION.to_string(),
            command: "import-dbt".to_string(),
            import_method: format!("{:?}", result.import_method),
            project_name: result.project_name.clone(),
            dbt_version: result.dbt_version.clone(),
            imported: result.imported.len(),
            warnings: result.warnings.len(),
            failed: result.failed.len(),
            sources_found: result.sources_found,
            sources_mapped: result.sources_mapped,
            tests_found: result.tests_found,
            tests_converted: result.tests_converted,
            tests_converted_custom: result.tests_converted_custom,
            tests_skipped: result.tests_skipped,
            macros_detected: result.macros_detected,
            imported_models: result.imported.iter().map(|m| m.name.clone()).collect(),
            warning_details,
            failed_details,
            report: serde_json::to_value(&migration_report).unwrap_or(serde_json::Value::Null),
        };
        print_json(&output)?;
    } else {
        // Use the migration report formatter for human-readable output
        let migration_report = report::generate_report(&result, Some(&test_stats), None);
        print!("{}", report::format_report(&migration_report));

        if !result.imported.is_empty() {
            println!(
                "Output:  {} models written to {}",
                result.imported.len(),
                output_dir.display()
            );
            println!();
        }

        if !result.warnings.is_empty() {
            println!("Warnings:");
            for w in &result.warnings {
                println!("  {}: {}", w.model, w.message);
                if let Some(ref s) = w.suggestion {
                    println!("    -> {s}");
                }
            }
            println!();
        }

        if !result.failed.is_empty() {
            println!("Failed:");
            for f in &result.failed {
                println!("  {}: {}", f.name, f.reason);
            }
            println!();
        }
    }

    Ok(())
}
