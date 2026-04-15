//! `rocky compile` — type-check models, resolve dependencies, validate contracts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::diagnostic::{self, Severity};
use rocky_compiler::incrementality;
use rocky_core::macros::{expand_macros, load_macros_from_dir};

use crate::output::{CompileOutput, CostHint, ModelDetail, print_json};

/// Execute `rocky compile`.
pub fn run_compile(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    do_expand_macros: bool,
) -> Result<()> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile::compile(&config)?;

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
