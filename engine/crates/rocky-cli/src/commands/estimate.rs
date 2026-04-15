//! `rocky estimate` — dry-run cost estimation for transformation models.
//!
//! Compiles models, generates their SQL, and runs EXPLAIN against the
//! configured warehouse adapter to produce cost estimates (bytes scanned,
//! row counts, raw plan output). When the adapter returns byte counts, a
//! USD cost estimate is computed using the per-warehouse pricing model.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, info};

use rocky_core::cost::WarehouseCostModel;
use rocky_core::models;
use rocky_core::traits::WarehouseAdapter;

use crate::output::{EstimateOutput, ModelEstimate, print_json};
use crate::registry::{self, AdapterRegistry};

/// Execute `rocky estimate`: compile models, generate SQL, and run EXPLAIN.
pub async fn run_estimate(
    config_path: &Path,
    models_dir: &Path,
    pipeline_name: Option<&str>,
    model_filter: Option<&str>,
    output_json: bool,
) -> Result<()> {
    // 1. Load config + adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_, pipeline) = registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;
    let adapter_registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(pipeline.target_adapter())?;

    // Determine warehouse pricing model from the pipeline's target adapter.
    let pricing = resolve_pricing(&rocky_cfg, pipeline.target_adapter());

    // 2. Load all models.
    let all_models = load_all_models(models_dir)?;

    let models_to_estimate: Vec<_> = all_models
        .iter()
        .filter(|m| {
            if let Some(filter) = model_filter {
                m.config.name == filter
            } else {
                true
            }
        })
        .collect();

    if models_to_estimate.is_empty() {
        info!("no models found to estimate");
        if output_json {
            print_json(&EstimateOutput::new(vec![]))?;
        } else {
            println!("No models found.");
        }
        return Ok(());
    }

    // 3. For each model, generate SQL and run EXPLAIN.
    let mut estimates = Vec::new();
    for model in &models_to_estimate {
        let plan = model.to_plan();
        let dialect = warehouse_adapter.dialect();

        let sql_result = rocky_core::sql_gen::generate_transformation_sql(&plan, dialect);

        let sql = match sql_result {
            Ok(stmts) => stmts.join(";\n"),
            Err(e) => {
                debug!(model = %model.config.name, "skipping estimate — SQL gen error: {e}");
                continue;
            }
        };

        match run_explain(&warehouse_adapter, &model.config.name, &sql, &pricing).await {
            Ok(estimate) => estimates.push(estimate),
            Err(e) => {
                debug!(model = %model.config.name, "explain failed: {e}");
                if !output_json {
                    println!("  ! {} — explain failed: {e}", model.config.name);
                }
            }
        }
    }

    // 4. Report.
    if output_json {
        print_json(&EstimateOutput::new(estimates))?;
    } else {
        println!("Estimated {} model(s):", estimates.len());
        println!();
        for est in &estimates {
            println!("  {}", est.model_name);
            if let Some(bytes) = est.estimated_bytes_scanned {
                println!("    Bytes scanned: {bytes}");
            }
            if let Some(rows) = est.estimated_rows {
                println!("    Rows: {rows}");
            }
            if let Some(cost) = est.estimated_cost_usd {
                println!("    Est. cost: ${cost:.6}");
            }
            if !est.raw_explain.is_empty() {
                // Show first 3 lines of EXPLAIN for human-readable summary.
                let preview: Vec<&str> = est.raw_explain.lines().take(3).collect();
                for line in preview {
                    println!("    {line}");
                }
                let total_lines = est.raw_explain.lines().count();
                if total_lines > 3 {
                    println!(
                        "    ... ({} more lines, use --output json for full plan)",
                        total_lines - 3
                    );
                }
            }
            println!();
        }

        // Print total cost if any model had a cost estimate.
        let total: f64 = estimates.iter().filter_map(|e| e.estimated_cost_usd).sum();
        if total > 0.0 {
            println!("  Total estimated cost: ${total:.6}");
        }
    }

    Ok(())
}

/// Run EXPLAIN for a single model's SQL and return the estimate.
async fn run_explain(
    adapter: &Arc<dyn WarehouseAdapter>,
    model_name: &str,
    sql: &str,
    pricing: &WarehouseCostModel,
) -> Result<ModelEstimate> {
    let explain_result = adapter
        .explain(sql)
        .await
        .context(format!("EXPLAIN failed for model '{model_name}'"))?;

    // Compute USD cost from bytes scanned using the warehouse pricing model.
    let cost_usd = explain_result.estimated_bytes_scanned.map(|bytes| {
        let row_estimate = explain_result.estimated_rows.unwrap_or(0);
        (row_estimate as f64 * pricing.per_row_scan_cost)
            + (bytes as f64 * pricing.per_byte_io_cost)
    });

    Ok(ModelEstimate {
        model_name: model_name.to_string(),
        estimated_bytes_scanned: explain_result.estimated_bytes_scanned,
        estimated_rows: explain_result.estimated_rows,
        estimated_cost_usd: cost_usd,
        raw_explain: explain_result.raw_explain,
    })
}

/// Load all models from a directory including one level of subdirectories.
fn load_all_models(models_dir: &Path) -> Result<Vec<models::Model>> {
    let mut all = models::load_models_from_dir(models_dir).context(format!(
        "failed to load models from {}",
        models_dir.display()
    ))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(sub) = models::load_models_from_dir(&entry.path()) {
                    all.extend(sub);
                }
            }
        }
    }
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Resolve a warehouse pricing model from the pipeline's target adapter.
///
/// Looks up the specific adapter used by the pipeline (not just the first
/// adapter in the config) and returns the appropriate pricing model. Falls
/// back to Databricks pricing when the adapter type is not recognized.
fn resolve_pricing(
    config: &rocky_core::config::RockyConfig,
    target_adapter: &str,
) -> WarehouseCostModel {
    let adapter_type = config
        .adapters
        .get(target_adapter)
        .map(|a| a.adapter_type.as_str())
        .unwrap_or("");

    match adapter_type {
        t if t.contains("snowflake") => WarehouseCostModel::snowflake(),
        t if t.contains("bigquery") => WarehouseCostModel::bigquery(),
        t if t.contains("duckdb") => WarehouseCostModel::duckdb(),
        _ => WarehouseCostModel::databricks(),
    }
}
