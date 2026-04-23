//! dbt migration report generation.
//!
//! Produces a comprehensive summary of a dbt-to-Rocky import, including model/source/test
//! conversion stats, macro resolution results, and actionable recommendations.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::dbt::{ImportMethod, ImportResult, WarningCategory};
use super::dbt_macros::MacroResolution;

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// Comprehensive migration report covering all aspects of a dbt import.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
    pub project: ProjectSummary,
    pub models: ModelsSummary,
    pub sources: SourcesSummary,
    pub tests: TestsSummary,
    pub macros: MacrosSummary,
    pub recommendations: Vec<Recommendation>,
}

/// Project-level metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub name: String,
    pub dbt_version: Option<String>,
    pub import_method: String,
    pub model_count: usize,
    pub source_count: usize,
    pub test_count: usize,
}

/// Summary of model import results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelsSummary {
    pub imported: usize,
    pub warnings: usize,
    pub failed: usize,
    pub by_materialization: HashMap<String, usize>,
    pub by_directory: HashMap<String, usize>,
}

/// Summary of source mapping results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesSummary {
    pub defined: usize,
    pub tables: usize,
    pub mapped: usize,
    pub unmapped: usize,
}

/// Summary of test conversion results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestsSummary {
    pub found: usize,
    pub converted: usize,
    pub converted_custom: usize,
    pub skipped: usize,
    pub by_type: HashMap<String, usize>,
}

/// Summary of macro resolution results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacrosSummary {
    pub total_usages: usize,
    pub expanded: usize,
    pub manifest_resolved: usize,
    pub unsupported: usize,
    pub by_package: HashMap<String, usize>,
}

/// An actionable recommendation for the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Recommendation {
    /// Run `rocky compile` to type-check imported models.
    RunCompile,
    /// Run `rocky ai explain --all --save` to add intent metadata.
    GenerateIntent,
    /// Run `rocky test` to validate with DuckDB.
    RunTests,
    /// Manually review specific models.
    ManualReview { models: Vec<String>, reason: String },
    /// Install missing packages or configure additional adapters.
    InstallMissing { packages: Vec<String> },
}

// ---------------------------------------------------------------------------
// Report generation
// ---------------------------------------------------------------------------

/// Test conversion stats passed into report generation.
pub struct TestConversionStats {
    pub found: usize,
    pub converted: usize,
    pub converted_custom: usize,
    pub skipped: usize,
    pub by_type: HashMap<String, usize>,
}

/// Macro resolution stats passed into report generation.
pub struct MacroStats {
    pub resolutions: Vec<(String, MacroResolution)>,
    pub by_package: HashMap<String, usize>,
}

/// Generate a full migration report from import results and optional test/macro stats.
pub fn generate_report(
    import_result: &ImportResult,
    test_stats: Option<&TestConversionStats>,
    macro_stats: Option<&MacroStats>,
) -> MigrationReport {
    let project = ProjectSummary {
        name: import_result
            .project_name
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        dbt_version: import_result.dbt_version.clone(),
        import_method: match import_result.import_method {
            ImportMethod::Manifest => "manifest.json".to_string(),
            ImportMethod::Regex => "regex".to_string(),
        },
        model_count: import_result.imported.len() + import_result.failed.len(),
        source_count: import_result.sources_found,
        test_count: test_stats.map(|t| t.found).unwrap_or(0),
    };

    // Count models with warnings
    let warned_models: std::collections::HashSet<&str> = import_result
        .warnings
        .iter()
        .map(|w| w.model.as_str())
        .collect();

    // Build materialization breakdown from model configs
    let mut by_materialization: HashMap<String, usize> = HashMap::new();
    let mut by_directory: HashMap<String, usize> = HashMap::new();

    for model in &import_result.imported {
        let mat = match &model.config.strategy {
            rocky_core::models::StrategyConfig::Incremental { .. } => "incremental",
            rocky_core::models::StrategyConfig::FullRefresh => "full_refresh",
            rocky_core::models::StrategyConfig::Merge { .. } => "merge",
            rocky_core::models::StrategyConfig::TimeInterval { .. } => "time_interval",
            rocky_core::models::StrategyConfig::Ephemeral => "ephemeral",
            rocky_core::models::StrategyConfig::DeleteInsert { .. } => "delete_insert",
            rocky_core::models::StrategyConfig::Microbatch { .. } => "microbatch",
        };
        *by_materialization.entry(mat.to_string()).or_default() += 1;

        // Extract directory from model target schema (best approximation)
        let dir = model.config.target.schema.clone();
        if !dir.is_empty() {
            *by_directory.entry(dir).or_default() += 1;
        }
    }

    let models = ModelsSummary {
        imported: import_result.imported.len(),
        warnings: warned_models.len(),
        failed: import_result.failed.len(),
        by_materialization,
        by_directory,
    };

    let sources = SourcesSummary {
        defined: import_result.sources_found,
        tables: import_result.sources_found,
        mapped: import_result.sources_mapped,
        unmapped: import_result
            .sources_found
            .saturating_sub(import_result.sources_mapped),
    };

    let tests = test_stats
        .map(|t| TestsSummary {
            found: t.found,
            converted: t.converted,
            converted_custom: t.converted_custom,
            skipped: t.skipped,
            by_type: t.by_type.clone(),
        })
        .unwrap_or(TestsSummary {
            found: 0,
            converted: 0,
            converted_custom: 0,
            skipped: 0,
            by_type: HashMap::new(),
        });

    let macros = macro_stats
        .map(|m| {
            let mut expanded = 0;
            let mut manifest_resolved = 0;
            let mut unsupported = 0;

            for (_, res) in &m.resolutions {
                match res {
                    MacroResolution::Expanded(_) => expanded += 1,
                    MacroResolution::ManifestFallback => manifest_resolved += 1,
                    MacroResolution::Unsupported(_) => unsupported += 1,
                }
            }

            MacrosSummary {
                total_usages: m.resolutions.len(),
                expanded,
                manifest_resolved,
                unsupported,
                by_package: m.by_package.clone(),
            }
        })
        .unwrap_or(MacrosSummary {
            total_usages: 0,
            expanded: 0,
            manifest_resolved: 0,
            unsupported: 0,
            by_package: HashMap::new(),
        });

    // Generate recommendations
    let recommendations = generate_recommendations(import_result, &tests, &macros);

    MigrationReport {
        project,
        models,
        sources,
        tests,
        macros,
        recommendations,
    }
}

fn generate_recommendations(
    import_result: &ImportResult,
    tests: &TestsSummary,
    macros: &MacrosSummary,
) -> Vec<Recommendation> {
    let mut recs = Vec::new();

    // Always recommend compile
    recs.push(Recommendation::RunCompile);

    // Recommend generating intent
    recs.push(Recommendation::GenerateIntent);

    // Recommend running tests if we converted any
    if tests.converted > 0 || tests.converted_custom > 0 {
        recs.push(Recommendation::RunTests);
    }

    // Recommend manual review for models with warnings
    let warning_models: Vec<String> = import_result
        .warnings
        .iter()
        .filter(|w| {
            matches!(
                w.category,
                WarningCategory::UnsupportedMacro | WarningCategory::JinjaControlFlow
            )
        })
        .map(|w| w.model.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    if !warning_models.is_empty() {
        recs.push(Recommendation::ManualReview {
            models: warning_models,
            reason: "models contain unsupported Jinja or macros".to_string(),
        });
    }

    // Recommend installing missing packages for unsupported macros
    if macros.unsupported > 0 {
        let packages: Vec<String> = macros
            .by_package
            .keys()
            .filter(|k| *k != "project")
            .cloned()
            .collect();
        if !packages.is_empty() {
            recs.push(Recommendation::InstallMissing { packages });
        }
    }

    recs
}

// ---------------------------------------------------------------------------
// Report formatting
// ---------------------------------------------------------------------------

/// Format a migration report as a human-readable string.
pub fn format_report(report: &MigrationReport) -> String {
    let mut out = String::new();

    // Header
    out.push_str("dbt Migration Report\n");
    out.push_str("====================\n\n");

    // Project info
    let version_str = report
        .project
        .dbt_version
        .as_ref()
        .map(|v| format!(" (dbt {v})"))
        .unwrap_or_default();
    out.push_str(&format!(
        "Project: {}{}\n",
        report.project.name, version_str
    ));
    out.push_str(&format!("Method:  {}\n\n", report.project.import_method));

    // Models section
    let total = report.models.imported + report.models.failed;
    out.push_str(&format!("Models:  {total} total\n"));

    if report.models.imported > 0 {
        let clean = report
            .models
            .imported
            .saturating_sub(report.models.warnings);
        if clean > 0 {
            out.push_str(&format!("  {clean} imported successfully"));
            if !report.models.by_materialization.is_empty() {
                let mats: Vec<String> = report
                    .models
                    .by_materialization
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect();
                out.push_str(&format!("  ({})", mats.join(", ")));
            }
            out.push('\n');
        }
        if report.models.warnings > 0 {
            out.push_str(&format!("  {} with warnings\n", report.models.warnings));
        }
    }
    if report.models.failed > 0 {
        out.push_str(&format!("  {} failed\n", report.models.failed));
    }
    out.push('\n');

    // Sources section
    if report.sources.defined > 0 {
        out.push_str(&format!(
            "Sources: {} tables from {} sources\n",
            report.sources.tables, report.sources.defined
        ));
        out.push_str(&format!("  {} mapped to Rocky\n", report.sources.mapped));
        if report.sources.unmapped > 0 {
            out.push_str(&format!("  {} unmapped\n", report.sources.unmapped));
        }
        out.push('\n');
    }

    // Tests section
    if report.tests.found > 0 {
        out.push_str(&format!("Tests:   {} total\n", report.tests.found));
        if report.tests.converted > 0 {
            out.push_str(&format!(
                "  {} converted to contracts\n",
                report.tests.converted
            ));
        }
        if report.tests.converted_custom > 0 {
            out.push_str(&format!(
                "  {} converted as custom SQL\n",
                report.tests.converted_custom
            ));
        }
        if report.tests.skipped > 0 {
            out.push_str(&format!("  {} skipped\n", report.tests.skipped));
        }
        if !report.tests.by_type.is_empty() {
            let types: Vec<String> = report
                .tests
                .by_type
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect();
            out.push_str(&format!("  By type: {}\n", types.join(", ")));
        }
        out.push('\n');
    }

    // Macros section
    if report.macros.total_usages > 0 {
        out.push_str(&format!("Macros:  {} usages\n", report.macros.total_usages));
        if report.macros.expanded > 0 {
            out.push_str(&format!("  {} expanded\n", report.macros.expanded));
        }
        if report.macros.manifest_resolved > 0 {
            out.push_str(&format!(
                "  {} resolved via manifest\n",
                report.macros.manifest_resolved
            ));
        }
        if report.macros.unsupported > 0 {
            out.push_str(&format!("  {} unsupported\n", report.macros.unsupported));
        }
        out.push('\n');
    }

    // Recommendations
    if !report.recommendations.is_empty() {
        out.push_str("Next Steps:\n");
        for (i, rec) in report.recommendations.iter().enumerate() {
            let step = i + 1;
            match rec {
                Recommendation::RunCompile => {
                    out.push_str(&format!("  {step}. rocky compile\n"));
                }
                Recommendation::GenerateIntent => {
                    out.push_str(&format!("  {step}. rocky ai explain --all --save\n"));
                }
                Recommendation::RunTests => {
                    out.push_str(&format!("  {step}. rocky test\n"));
                }
                Recommendation::ManualReview { models, reason } => {
                    out.push_str(&format!(
                        "  {step}. Review {} model(s): {reason}\n",
                        models.len()
                    ));
                    for m in models.iter().take(5) {
                        out.push_str(&format!("     - {m}\n"));
                    }
                    if models.len() > 5 {
                        out.push_str(&format!("     ... and {} more\n", models.len() - 5));
                    }
                }
                Recommendation::InstallMissing { packages } => {
                    out.push_str(&format!(
                        "  {step}. Check macro packages: {}\n",
                        packages.join(", ")
                    ));
                }
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::dbt::{ImportFailure, ImportWarning, ImportedModel};
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    #[allow(clippy::too_many_arguments)]
    fn make_result(
        imported: Vec<ImportedModel>,
        warnings: Vec<ImportWarning>,
        failed: Vec<ImportFailure>,
        import_method: ImportMethod,
        project_name: Option<String>,
        dbt_version: Option<String>,
        sources_found: usize,
        sources_mapped: usize,
    ) -> ImportResult {
        ImportResult {
            imported,
            warnings,
            failed,
            sources_found,
            sources_mapped,
            import_method,
            project_name,
            dbt_version,
            tests_found: 0,
            tests_converted: 0,
            tests_converted_custom: 0,
            tests_skipped: 0,
            macros_detected: 0,
            macros_expanded: 0,
            macros_manifest_resolved: 0,
            macros_unsupported: 0,
        }
    }

    fn make_imported(name: &str, strategy: StrategyConfig) -> ImportedModel {
        ImportedModel {
            name: name.to_string(),
            sql: "SELECT 1".to_string(),
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy,
                target: TargetConfig {
                    catalog: "cat".to_string(),
                    schema: "staging".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
            },
        }
    }

    #[test]
    fn test_empty_report() {
        let result = make_result(
            vec![],
            vec![],
            vec![],
            ImportMethod::Regex,
            Some("empty_project".to_string()),
            None,
            0,
            0,
        );

        let report = generate_report(&result, None, None);
        assert_eq!(report.project.name, "empty_project");
        assert_eq!(report.models.imported, 0);
        assert_eq!(report.models.failed, 0);
        assert_eq!(report.tests.found, 0);
        assert_eq!(report.macros.total_usages, 0);
    }

    #[test]
    fn test_full_report() {
        let result = make_result(
            vec![
                make_imported("stg_orders", StrategyConfig::FullRefresh),
                make_imported(
                    "fct_revenue",
                    StrategyConfig::Merge {
                        unique_key: vec!["id".to_string()],
                        update_columns: None,
                    },
                ),
            ],
            vec![ImportWarning {
                model: "stg_events".to_string(),
                category: WarningCategory::JinjaControlFlow,
                message: "contains Jinja".to_string(),
                suggestion: None,
            }],
            vec![ImportFailure {
                name: "py_model".to_string(),
                reason: "Python model".to_string(),
            }],
            ImportMethod::Manifest,
            Some("analytics".to_string()),
            Some("1.7.4".to_string()),
            5,
            4,
        );

        let test_stats = TestConversionStats {
            found: 10,
            converted: 6,
            converted_custom: 3,
            skipped: 1,
            by_type: HashMap::from([
                ("unique".to_string(), 3),
                ("not_null".to_string(), 5),
                ("accepted_values".to_string(), 2),
            ]),
        };

        let macro_stats = MacroStats {
            resolutions: vec![
                (
                    "dbt_utils.star".to_string(),
                    MacroResolution::Expanded("a, b".to_string()),
                ),
                (
                    "custom.func".to_string(),
                    MacroResolution::Unsupported("n/a".to_string()),
                ),
            ],
            by_package: HashMap::from([("dbt_utils".to_string(), 1), ("custom".to_string(), 1)]),
        };

        let report = generate_report(&result, Some(&test_stats), Some(&macro_stats));

        assert_eq!(report.project.name, "analytics");
        assert_eq!(report.project.dbt_version, Some("1.7.4".to_string()));
        assert_eq!(report.models.imported, 2);
        assert_eq!(report.models.failed, 1);
        assert_eq!(report.sources.mapped, 4);
        assert_eq!(report.sources.unmapped, 1);
        assert_eq!(report.tests.found, 10);
        assert_eq!(report.tests.converted, 6);
        assert_eq!(report.macros.total_usages, 2);
        assert_eq!(report.macros.expanded, 1);
        assert_eq!(report.macros.unsupported, 1);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_format_report_output() {
        let result = make_result(
            vec![make_imported("model_a", StrategyConfig::FullRefresh)],
            vec![],
            vec![],
            ImportMethod::Manifest,
            Some("test_proj".to_string()),
            Some("1.7.0".to_string()),
            2,
            2,
        );

        let report = generate_report(&result, None, None);
        let formatted = format_report(&report);

        assert!(formatted.contains("dbt Migration Report"));
        assert!(formatted.contains("test_proj"));
        assert!(formatted.contains("dbt 1.7.0"));
        assert!(formatted.contains("manifest.json"));
        assert!(formatted.contains("rocky compile"));
    }

    #[test]
    fn test_recommendations_include_manual_review() {
        let result = make_result(
            vec![],
            vec![ImportWarning {
                model: "bad_model".to_string(),
                category: WarningCategory::UnsupportedMacro,
                message: "unsupported macro".to_string(),
                suggestion: None,
            }],
            vec![],
            ImportMethod::Regex,
            Some("proj".to_string()),
            None,
            0,
            0,
        );

        let report = generate_report(&result, None, None);
        let has_manual = report.recommendations.iter().any(|r| {
            matches!(r, Recommendation::ManualReview { models, .. } if models.contains(&"bad_model".to_string()))
        });
        assert!(has_manual);
    }

    #[test]
    fn test_report_json_serialization() {
        let result = make_result(
            vec![make_imported("m", StrategyConfig::FullRefresh)],
            vec![],
            vec![],
            ImportMethod::Regex,
            Some("p".to_string()),
            None,
            0,
            0,
        );

        let report = generate_report(&result, None, None);
        let json = serde_json::to_string(&report).unwrap();
        let deser: MigrationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.project.name, "p");
    }
}
