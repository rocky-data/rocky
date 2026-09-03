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
///
/// `verbose` affects the TEXT output only — the JSON output is byte-identical
/// either way, so a `--output json` consumer cannot tell the flag was passed.
pub async fn run_estimate(
    config_path: &Path,
    models_dir: &Path,
    pipeline_name: Option<&str>,
    model_filter: Option<&str>,
    output_json: bool,
    verbose: bool,
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
    let (pricing_source, pricing) = resolve_pricing(&rocky_cfg, pipeline.target_adapter());

    // 2. Load all models.
    let all_models = load_all_models(models_dir, Some(&rocky_cfg.freshness))?;

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

    // An unknown `--model` is a mistake, not an empty result: refuse it the
    // way `rocky compile --model` does, rather than reporting zero estimates
    // at exit 0 (#1428).
    if let Some(filter) = model_filter
        && !all_models.iter().any(|m| m.config.name == filter)
    {
        return Err(anyhow::Error::new(super::ModelNotFound(filter.to_string())));
    }

    if models_to_estimate.is_empty() {
        info!("no models found to estimate");
        if output_json {
            // Carries the same explanation the text path prints. Previously
            // the human was told "No models found." and the JSON consumer got
            // a bare empty array with no way to tell "nothing to estimate"
            // from "the command did not run" (#1428).
            print_json(&EstimateOutput::empty("no models found to estimate"))?;
        } else {
            println!("No models found.");
        }
        return Ok(());
    }

    // 3. For each model, generate SQL and run EXPLAIN.
    let mut estimates = Vec::new();
    // Models dropped before EXPLAIN ever ran. Without these the text report's
    // "Estimated N model(s)" is a count of what SUCCEEDED, not of what was
    // asked for, and the difference is otherwise only visible at debug log
    // level. Surfaced under `--verbose` (the default output is unchanged).
    let mut skipped: Vec<(String, String)> = Vec::new();
    for model in &models_to_estimate {
        let model_ir = model.to_model_ir();
        let dialect = warehouse_adapter.dialect();

        let sql_result = rocky_core::sql_gen::generate_transformation_sql(&model_ir, dialect);

        let sql = match sql_result {
            Ok(stmts) => stmts.join(";\n"),
            Err(e) => {
                debug!(model = %model.config.name, "skipping estimate — SQL gen error: {e}");
                skipped.push((model.config.name.clone(), format!("SQL generation: {e}")));
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
        // Models were selected but none produced an estimate — every SQL-gen
        // or EXPLAIN failed (both are `debug!`-only above, and the per-model
        // "explain failed" line is printed on the text path only). Without
        // this, the payload is `{"estimates": [], "total_models": 0}`: the
        // same silent-empty shape as a project with nothing to estimate, and
        // a contract violation of `EstimateOutput::message`, which promises
        // to say why an empty result is empty (#1428).
        let output = if estimates.is_empty() {
            EstimateOutput::empty("no model produced an estimate")
        } else {
            EstimateOutput::new(estimates)
        };
        print_json(&output)?;
    } else {
        print!(
            "{}",
            render_estimates_text(&estimates, &skipped, pricing_source, &pricing, verbose)
        );
    }

    Ok(())
}

/// Render the text report. Returns the whole report as a string rather than
/// printing, so the `--verbose` and default shapes are both testable without a
/// warehouse — including the promise that passing the flag ADDS lines and
/// changes none of the existing ones.
fn render_estimates_text(
    estimates: &[ModelEstimate],
    skipped: &[(String, String)],
    pricing_source: &str,
    pricing: &WarehouseCostModel,
    verbose: bool,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();

    // `write!` to a String is infallible; the returned Result is discarded
    // deliberately rather than propagated to the caller's `Result<()>`.
    let _ = writeln!(out, "Estimated {} model(s):", estimates.len());
    let _ = writeln!(out);
    for est in estimates {
        let _ = writeln!(out, "  {}", est.model_name);
        if let Some(bytes) = est.estimated_bytes_scanned {
            let _ = writeln!(out, "    Bytes scanned: {bytes}");
        }
        if let Some(rows) = est.estimated_rows {
            let _ = writeln!(out, "    Rows: {rows}");
        }
        if let Some(cost) = est.estimated_cost_usd {
            let _ = writeln!(out, "    Est. cost: ${cost:.6}");
        }
        if !est.raw_explain.is_empty() {
            if verbose {
                // The whole plan — the reason someone reaches for the flag.
                for line in est.raw_explain.lines() {
                    let _ = writeln!(out, "    {line}");
                }
            } else {
                // Show first 3 lines of EXPLAIN for human-readable summary.
                let preview: Vec<&str> = est.raw_explain.lines().take(3).collect();
                for line in preview {
                    let _ = writeln!(out, "    {line}");
                }
                let total_lines = est.raw_explain.lines().count();
                if total_lines > 3 {
                    let _ = writeln!(
                        out,
                        "    ... ({} more lines, use --verbose or --output json for full plan)",
                        total_lines - 3
                    );
                }
            }
        }
        let _ = writeln!(out);
    }

    // Print total cost if any model had a cost estimate.
    let total: f64 = estimates.iter().filter_map(|e| e.estimated_cost_usd).sum();
    if total > 0.0 {
        let _ = writeln!(out, "  Total estimated cost: ${total:.6}");
    }

    if verbose {
        // The two rates that actually enter the arithmetic in `run_explain`.
        // `per_row_compute_cost` exists on the model but is NOT applied, so
        // printing it here would suggest the estimate accounts for compute.
        let _ = writeln!(out);
        let _ = writeln!(out, "  Pricing model: {pricing_source}");
        let _ = writeln!(
            out,
            "    per row scanned: ${:.3e}",
            pricing.per_row_scan_cost
        );
        let _ = writeln!(
            out,
            "    per byte of I/O: ${:.3e}",
            pricing.per_byte_io_cost
        );

        if !skipped.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "  Skipped before EXPLAIN ({}):", skipped.len());
            for (name, reason) in skipped {
                let _ = writeln!(out, "    {name} — {reason}");
            }
        }
    }

    out
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

/// Load all models from a directory including one level of subdirectories
/// (including `.rocky` DSL files).
fn load_all_models(
    models_dir: &Path,
    project_freshness: Option<&rocky_core::config::ProjectFreshnessConfig>,
) -> Result<Vec<models::Model>> {
    let mut all = crate::models_loader::load_project_models(models_dir, project_freshness)?;
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Resolve a warehouse pricing model from the pipeline's target adapter.
///
/// Looks up the specific adapter used by the pipeline (not just the first
/// adapter in the config) and returns the appropriate pricing model. Falls
/// back to Databricks pricing when the adapter type is not recognized.
///
/// The label and the rates come out of ONE match so `--verbose` cannot name a
/// pricing model different from the one that produced the numbers. The
/// fallback is labelled as a fallback: an unrecognized adapter type is priced
/// at Databricks rates, and a surprising cost is otherwise unexplainable.
fn resolve_pricing(
    config: &rocky_core::config::RockyConfig,
    target_adapter: &str,
) -> (&'static str, WarehouseCostModel) {
    let adapter_type = config
        .adapters
        .get(target_adapter)
        .map(|a| a.adapter_type.as_str())
        .unwrap_or("");

    match adapter_type {
        t if t.contains("snowflake") => ("snowflake", WarehouseCostModel::snowflake()),
        t if t.contains("bigquery") => ("bigquery", WarehouseCostModel::bigquery()),
        t if t.contains("duckdb") => ("duckdb", WarehouseCostModel::duckdb()),
        t if t.contains("databricks") => ("databricks", WarehouseCostModel::databricks()),
        _ => (
            "databricks (fallback — adapter type not recognized)",
            WarehouseCostModel::databricks(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn est(name: &str, plan_lines: usize, cost: Option<f64>) -> ModelEstimate {
        ModelEstimate {
            model_name: name.to_string(),
            estimated_bytes_scanned: Some(1_024),
            estimated_rows: Some(7),
            estimated_cost_usd: cost,
            raw_explain: (1..=plan_lines)
                .map(|i| format!("plan line {i}"))
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    /// #480: the flag is additive. Every line the default report emits is
    /// still emitted, in order, when `--verbose` is passed — the verbose
    /// report only APPENDS. Asserted as a subsequence rather than by
    /// restating the expected text, so a future edit to the default shape
    /// cannot pass this test while silently dropping a line under verbose.
    #[test]
    fn verbose_only_adds_lines_it_never_removes_or_reorders_them() {
        let estimates = vec![est("orders", 2, Some(0.5))];
        let pricing = WarehouseCostModel::duckdb();
        let plain = render_estimates_text(&estimates, &[], "duckdb", &pricing, false);
        let verbose = render_estimates_text(&estimates, &[], "duckdb", &pricing, true);

        let mut v = verbose.lines();
        for line in plain.lines() {
            assert!(
                v.any(|candidate| candidate == line),
                "verbose report dropped or reordered {line:?}\n--- plain ---\n{plain}\n--- verbose ---\n{verbose}"
            );
        }
        assert!(
            verbose.lines().count() > plain.lines().count(),
            "verbose added nothing"
        );
    }

    /// #480 acceptance criterion: `rocky estimate` with NO flag is unchanged.
    /// Pinned as the exact report rather than as spot-checks — a mutation
    /// that drops one line (say `Rows:`) from the default path passes every
    /// other test here, including the additive-verbose one, because dropping
    /// a line from the default keeps it a subsequence of verbose.
    #[test]
    fn the_default_report_shape_is_pinned_line_for_line() {
        let estimates = vec![est("orders", 5, Some(0.5))];
        let pricing = WarehouseCostModel::duckdb();

        assert_eq!(
            render_estimates_text(&estimates, &[], "duckdb", &pricing, false),
            "Estimated 1 model(s):\n\
             \n\
             \u{20}\u{20}orders\n\
             \u{20}\u{20}\u{20}\u{20}Bytes scanned: 1024\n\
             \u{20}\u{20}\u{20}\u{20}Rows: 7\n\
             \u{20}\u{20}\u{20}\u{20}Est. cost: $0.500000\n\
             \u{20}\u{20}\u{20}\u{20}plan line 1\n\
             \u{20}\u{20}\u{20}\u{20}plan line 2\n\
             \u{20}\u{20}\u{20}\u{20}plan line 3\n\
             \u{20}\u{20}\u{20}\u{20}... (2 more lines, use --verbose or --output json for full plan)\n\
             \n\
             \u{20}\u{20}Total estimated cost: $0.500000\n"
        );
    }

    /// #480: a plan longer than the 3-line preview is truncated by default
    /// and printed WHOLE under `--verbose`. The truncation hint names the
    /// flag, so the report's own advice is reachable.
    #[test]
    fn verbose_prints_the_whole_plan_where_the_default_truncates() {
        let estimates = vec![est("orders", 9, None)];
        let pricing = WarehouseCostModel::duckdb();
        let plain = render_estimates_text(&estimates, &[], "duckdb", &pricing, false);
        let verbose = render_estimates_text(&estimates, &[], "duckdb", &pricing, true);

        assert!(plain.contains("plan line 3"), "{plain}");
        assert!(!plain.contains("plan line 4"), "{plain}");
        assert!(plain.contains("6 more lines"), "{plain}");
        assert!(plain.contains("--verbose"), "the hint must name the flag");

        assert!(verbose.contains("plan line 9"), "{verbose}");
        assert!(
            !verbose.contains("more lines"),
            "nothing left to truncate: {verbose}"
        );
    }

    /// #480: `--verbose` reports the two rates that actually enter the cost
    /// arithmetic in `run_explain`, and NOT `per_row_compute_cost`, which the
    /// model carries but the estimate never applies. Printing it would imply
    /// the estimate accounts for compute.
    #[test]
    fn verbose_reports_only_the_rates_the_estimate_actually_applies() {
        let pricing = WarehouseCostModel::duckdb();
        let verbose = render_estimates_text(&[], &[], "duckdb", &pricing, true);

        assert!(verbose.contains("per row scanned"), "{verbose}");
        assert!(verbose.contains("per byte of I/O"), "{verbose}");
        assert!(
            !verbose.contains("compute"),
            "per_row_compute_cost is not applied by run_explain: {verbose}"
        );
    }

    /// #480: models dropped before EXPLAIN are invisible in the default
    /// report — "Estimated N model(s)" counts successes, not requests — and
    /// `--verbose` names them with the reason.
    #[test]
    fn verbose_names_models_dropped_before_explain_ran() {
        let skipped = vec![("broken".to_string(), "SQL generation: bad ref".to_string())];
        let pricing = WarehouseCostModel::duckdb();
        let plain = render_estimates_text(&[], &skipped, "duckdb", &pricing, false);
        let verbose = render_estimates_text(&[], &skipped, "duckdb", &pricing, true);

        assert!(
            !plain.contains("broken"),
            "default output must be unchanged: {plain}"
        );
        assert!(verbose.contains("broken"), "{verbose}");
        assert!(verbose.contains("SQL generation: bad ref"), "{verbose}");
    }

    /// The `--verbose` pricing label and the rates come from one match, so a
    /// genuine Databricks adapter is not labelled a fallback and an
    /// unrecognized one is — the fallback is the case that makes a surprising
    /// cost explicable.
    #[test]
    fn pricing_label_distinguishes_a_real_databricks_adapter_from_the_fallback() {
        let cfg: rocky_core::config::RockyConfig = toml::from_str(
            r#"
[adapter.dbx]
type = "databricks"
host = "example.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"

[adapter.odd]
type = "some-future-warehouse"
"#,
        )
        .expect("config");

        let (real, real_rates) = resolve_pricing(&cfg, "dbx");
        assert_eq!(real, "databricks");

        let (fallback, fallback_rates) = resolve_pricing(&cfg, "odd");
        assert!(fallback.contains("fallback"), "{fallback}");
        assert_eq!(
            real_rates.per_byte_io_cost, fallback_rates.per_byte_io_cost,
            "the fallback is Databricks pricing — only the label differs"
        );
    }
}
