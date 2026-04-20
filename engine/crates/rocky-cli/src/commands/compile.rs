//! `rocky compile` — type-check models, resolve dependencies, validate contracts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::diagnostic::{self, Diagnostic, Severity};
use rocky_compiler::incrementality;
use rocky_core::config as rocky_config;
use rocky_core::macros::{expand_macros, load_macros_from_dir};
use rocky_sql::portability::{self, PortabilityIssue};
use rocky_sql::pragma;
use rocky_sql::transpile::Dialect;

use crate::output::{CompileOutput, CostHint, ModelDetail, print_json};

/// Execute `rocky compile`.
pub fn run_compile(
    config_path: Option<&Path>,
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
) -> Result<()> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let mut result = compile::compile(&config)?;

    // Portability lint. Effective target_dialect = CLI flag > [portability]
    // config > unset. Project-wide allow list and per-model `-- rocky-allow:`
    // pragmas suppress matching constructs before they become diagnostics.
    let portability_cfg = match config_path {
        Some(path) => rocky_config::load_rocky_config(path)
            .ok()
            .map(|c| c.portability),
        None => None,
    };
    let effective_dialect =
        target_dialect.or_else(|| portability_cfg.as_ref().and_then(|p| p.target_dialect));
    let project_allow: std::collections::HashSet<String> = portability_cfg
        .as_ref()
        .map(|p| {
            p.allow
                .iter()
                .map(|c| c.trim().to_ascii_uppercase())
                .collect()
        })
        .unwrap_or_default();

    if let Some(dialect) = effective_dialect {
        let mut portability_errors = false;
        for model in &result.project.models {
            let model_pragmas = pragma::parse_pragmas(&model.sql);
            for issue in portability::detect_portability_issues(&model.sql, dialect) {
                let upper = issue.construct.to_ascii_uppercase();
                if project_allow.contains(&upper) || model_pragmas.allows(&upper) {
                    continue;
                }
                result.diagnostics.push(build_p001_diagnostic(
                    &model.config.name,
                    &model.file_path,
                    &issue,
                ));
                portability_errors = true;
            }
        }
        if portability_errors {
            result.has_errors = true;
        }
    }

    // Load macros and expand model SQL when --expand-macros is set.
    let expanded_sql = if do_expand_macros {
        let macros_dir = models_dir.join("../macros");
        let macro_defs = if macros_dir.is_dir() {
            load_macros_from_dir(&macros_dir)?
        } else {
            vec![]
        };

        let mut expanded = HashMap::new();
        for model in &result.project.models {
            if let Some(filter) = model_filter {
                if model.config.name != filter {
                    continue;
                }
            }
            let sql = expand_macros(&model.sql, &macro_defs)?;
            expanded.insert(model.config.name.clone(), sql);
        }
        expanded
    } else {
        HashMap::new()
    };

    // Filter diagnostics by model if requested. We clone here so that the
    // typed CompileOutput owns the diagnostics; this matches the previous
    // serde_json::json!() behavior (which serialized references) but lets
    // us hand the data to schemars-driven codegen consumers.
    let diagnostics: Vec<_> = if let Some(filter) = model_filter {
        result
            .diagnostics
            .iter()
            .filter(|d| d.model == filter)
            .cloned()
            .collect()
    } else {
        result.diagnostics.clone()
    };

    if output_json {
        // Compute DAG-propagated cost estimates for all models.
        let cost_estimates = {
            use rocky_core::cost::{TableStats, WarehouseType, propagate_costs};
            let dag_nodes = &result.project.dag_nodes;
            let mut base_stats = std::collections::HashMap::new();
            for node in dag_nodes {
                if node.depends_on.is_empty() {
                    base_stats.insert(
                        node.name.clone(),
                        TableStats {
                            row_count: 10_000,
                            avg_row_bytes: 256,
                        },
                    );
                }
            }
            propagate_costs(dag_nodes, &base_stats, WarehouseType::Databricks).unwrap_or_default()
        };

        let models_detail: Vec<ModelDetail> = result
            .project
            .models
            .iter()
            .map(|model| {
                let typed_cols = result
                    .type_check
                    .typed_models
                    .get(&model.config.name)
                    .map(std::vec::Vec::as_slice)
                    .unwrap_or_default();
                let incrementality_hint = incrementality::infer_incrementality(
                    &model.config.name,
                    typed_cols,
                    &model.sql,
                    &model.config.strategy,
                );
                let cost_hint = cost_estimates.get(&model.config.name).map(|est| CostHint {
                    estimated_rows: est.estimated_rows,
                    estimated_bytes: est.estimated_bytes,
                    estimated_cost_usd: est.estimated_compute_cost_usd,
                    confidence: match est.confidence {
                        rocky_core::cost::Confidence::High => "high".to_string(),
                        rocky_core::cost::Confidence::Medium => "medium".to_string(),
                        rocky_core::cost::Confidence::Low => "low".to_string(),
                    },
                });
                ModelDetail {
                    name: model.config.name.clone(),
                    strategy: model.config.strategy.clone(),
                    target: model.config.target.clone(),
                    freshness: model.config.freshness.clone(),
                    contract_source: model.contract_path.as_ref().map(|_| "auto".to_string()),
                    incrementality_hint,
                    cost_hint,
                    depends_on: model.config.depends_on.clone(),
                }
            })
            .collect();
        let output = CompileOutput::new(
            result.project.model_count(),
            result.project.layers.len(),
            diagnostics.clone(),
            result.has_errors,
            result.timings.clone(),
        )
        .with_models_detail(models_detail)
        .with_expanded_sql(expanded_sql);
        print_json(&output)?;
    } else {
        let error_count = diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .count();
        let warning_count = diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Warning)
            .count();

        // Print model status
        for model_name in &result.project.execution_order {
            let model_diags: Vec<_> = diagnostics
                .iter()
                .filter(|d| d.model == *model_name)
                .collect();
            let has_model_errors = model_diags.iter().any(|d| d.severity == Severity::Error);

            if has_model_errors {
                println!("  \u{2717} {model_name}");
            } else {
                let col_count = result
                    .type_check
                    .typed_models
                    .get(model_name)
                    .map(std::vec::Vec::len)
                    .unwrap_or(0);
                println!("  \u{2713} {model_name} ({col_count} columns)");
            }
        }

        // Print expanded SQL when --expand-macros is set (text mode).
        if !expanded_sql.is_empty() {
            println!();
            for model_name in &result.project.execution_order {
                if let Some(sql) = expanded_sql.get(model_name) {
                    println!("  -- {model_name} (expanded)");
                    for line in sql.lines() {
                        println!("  {line}");
                    }
                    println!();
                }
            }
        }

        // Build a source map from model files so miette can render source spans.
        let source_map: HashMap<String, String> = result
            .project
            .models
            .iter()
            .map(|m| (m.file_path.clone(), m.sql.clone()))
            .collect();

        // Render diagnostics with miette (rich source spans when available)
        if !diagnostics.is_empty() {
            let rendered = diagnostic::render_diagnostics(&diagnostics, &source_map);
            print!("{rendered}");
        }

        println!(
            "  Compiled: {} models, {} errors, {} warnings",
            result.project.model_count(),
            error_count,
            warning_count,
        );
    }

    if result.has_errors {
        anyhow::bail!("compilation failed with errors");
    }

    Ok(())
}

/// Build an error-severity P001 diagnostic from a portability issue.
///
/// `file_path` is threaded into `SourceSpan` so the diagnostic shows up
/// against the model file. We don't yet track per-construct byte offsets,
/// so the span defaults to line 1 — wave 2 can sharpen this.
fn build_p001_diagnostic(
    model_name: &str,
    file_path: &str,
    issue: &PortabilityIssue,
) -> Diagnostic {
    let supported = issue
        .supported_by
        .iter()
        .map(Dialect::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "{} is not portable to {} (supported by: {})",
        issue.construct, issue.target, supported,
    );
    Diagnostic::error("P001", model_name, message)
        .with_span(diagnostic::SourceSpan {
            file: file_path.to_string(),
            line: 1,
            col: 1,
        })
        .with_suggestion(issue.suggestion.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_model(dir: &Path, name: &str, sql: &str) {
        let sql_path = dir.join(format!("{name}.sql"));
        let toml_path = dir.join(format!("{name}.toml"));
        fs::write(&sql_path, sql).unwrap();
        fs::write(
            &toml_path,
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    /// Write a minimal `rocky.toml` next to a models dir, with optional
    /// `[portability]` section content. Returns the path so tests can pass
    /// it as the new `config_path` argument. Uses the shape committed in
    /// `examples/playground/pocs/00-foundations/00-playground-default/rocky.toml`.
    fn write_rocky_toml(dir: &Path, portability_block: &str) -> std::path::PathBuf {
        let path = dir.join("rocky.toml");
        let body = format!(
            r#"[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.p]
strategy = "full_refresh"

[pipeline.p.source.discovery]
adapter = "default"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "c"
schema_template = "s"

{portability_block}
"#
        );
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn build_p001_has_error_severity_and_code() {
        let issue = PortabilityIssue {
            construct: "NVL".to_string(),
            supported_by: vec![Dialect::Snowflake, Dialect::Databricks],
            target: Dialect::BigQuery,
            suggestion: "use COALESCE".to_string(),
        };
        let diag = build_p001_diagnostic("m", "m.sql", &issue);
        assert_eq!(&*diag.code, "P001");
        assert_eq!(diag.severity, Severity::Error);
        assert_eq!(diag.model, "m");
        assert!(diag.message.contains("NVL"));
        assert!(diag.message.contains("BigQuery"));
        assert_eq!(diag.suggestion.as_deref(), Some("use COALESCE"));
    }

    #[test]
    fn compile_with_bigquery_target_flags_nvl_as_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // With target_dialect = BigQuery, NVL should trigger P001 and the
        // compile should bail with the generic "compilation failed" error.
        let err = run_compile(
            None,
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::BigQuery),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "expected bail, got: {err}"
        );
    }

    #[test]
    fn compile_without_target_dialect_does_not_run_lint() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // No target_dialect → no P001, compile succeeds.
        run_compile(None, &models_dir, None, None, true, false, None)
            .expect("compile should succeed without lint");
    }

    #[test]
    fn compile_with_snowflake_target_accepts_nvl() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // NVL is native to Snowflake, so the lint should produce no issues.
        run_compile(
            None,
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::Snowflake),
        )
        .expect("snowflake target should accept NVL");
    }

    // ---- Wave 2: [portability] block + pragma ----

    #[test]
    fn config_target_dialect_drives_lint_when_no_flag() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        let err =
            run_compile(Some(&config), &models_dir, None, None, true, false, None).unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "config target_dialect should fire P001: {err}",
        );
    }

    #[test]
    fn flag_overrides_config_target_dialect() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        // Config says snowflake (NVL native), flag overrides to bigquery
        // (NVL not portable). The flag must win and the lint must fire.
        let config = write_rocky_toml(
            dir.path(),
            "[portability]\ntarget_dialect = \"snowflake\"\n",
        );

        let err = run_compile(
            Some(&config),
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::BigQuery),
        )
        .unwrap_err();
        assert!(err.to_string().contains("compilation failed"));
    }

    #[test]
    fn config_allow_list_suppresses_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let config = write_rocky_toml(
            dir.path(),
            "[portability]\ntarget_dialect = \"bigquery\"\nallow = [\"NVL\"]\n",
        );

        // Project-wide allow-list of NVL → no P001 → compile succeeds.
        run_compile(Some(&config), &models_dir, None, None, true, false, None)
            .expect("allow-listed NVL should not trip the lint");
    }

    #[test]
    fn per_model_pragma_suppresses_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(
            &models_dir,
            "m1",
            "-- rocky-allow: NVL\nSELECT NVL(a, b) AS c FROM t",
        );
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        // Pragma exempts this model, so the lint should not fire.
        run_compile(Some(&config), &models_dir, None, None, true, false, None)
            .expect("pragma-exempted model should not trip the lint");
    }

    #[test]
    fn pragma_is_per_model_not_project_wide() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // m1 has the pragma; m2 does not. m2 should still trip the lint.
        write_model(
            &models_dir,
            "m1",
            "-- rocky-allow: NVL\nSELECT NVL(a, b) AS c FROM t",
        );
        write_model(&models_dir, "m2", "SELECT NVL(d, e) AS f FROM t");
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        let err =
            run_compile(Some(&config), &models_dir, None, None, true, false, None).unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "m2 should still trip the lint: {err}",
        );
    }

    #[test]
    fn missing_config_file_falls_through_to_flag_only() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let nonexistent = dir.path().join("nope.toml");

        // Config file doesn't exist → portability config silently None →
        // flag-only behavior remains. With no flag, no lint fires.
        run_compile(
            Some(&nonexistent),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
        )
        .expect("missing config should fall through, not error");
    }
}
