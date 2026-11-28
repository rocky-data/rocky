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

use crate::output::{EstimateGap, EstimateOutput, ModelEstimate, print_json};
use crate::registry::{self, AdapterRegistry, WarehouseUnavailable};

/// What `rocky estimate` computed, before rendering: the typed document a
/// JSON consumer gets, plus what only the text report shows (the pricing
/// table).
#[derive(Debug)]
pub struct EstimateReport {
    /// The `--output json` document.
    pub output: EstimateOutput,
    /// Which pricing table applied, named by adapter type.
    pub pricing_source: &'static str,
    /// The pricing table itself.
    pub pricing: WarehouseCostModel,
    /// How many models the filter matched. Zero is "No models found."
    pub matched: usize,
}

/// Compile models, generate SQL, and run EXPLAIN against the warehouse.
///
/// A `model_filter` naming no model is an error. No models at all is a
/// report with `matched == 0` and the JSON document's `message` set; so is
/// a project where every model was skipped or failed EXPLAIN, with the
/// other message.
///
/// An estimate changes nothing. `EXPLAIN` receives one statement per model or
/// none: an adapter runs the statements after the first, so a model whose SQL
/// is more than one statement, or cannot be parsed to prove it is one, is
/// listed in the output's `skipped`. A DuckDB database is opened read-only and
/// never created; when it cannot be opened that way, every model is skipped
/// with the reason. Only the pipeline's target adapter is built.
pub async fn compute_estimate(
    config_path: &Path,
    models_dir: &Path,
    pipeline_name: Option<&str>,
    model_filter: Option<&str>,
) -> Result<EstimateReport> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_, pipeline) = registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;

    let (pricing_source, pricing) = resolve_pricing(&rocky_cfg, pipeline.target_adapter());

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

    if let Some(filter) = model_filter
        && !all_models.iter().any(|m| m.config.name == filter)
    {
        return Err(anyhow::Error::new(super::ModelNotFound(filter.to_string())));
    }

    let matched = models_to_estimate.len();
    if matched == 0 {
        info!("no models found to estimate");
        return Ok(EstimateReport {
            output: EstimateOutput::empty("no models found to estimate"),
            pricing_source,
            pricing,
            matched,
        });
    }

    let touched = std::collections::BTreeSet::from([pipeline.target_adapter().to_string()]);
    let adapter_registry = match AdapterRegistry::from_config_read_only(&rocky_cfg, &touched) {
        Ok(registry) => registry,
        Err(error) => {
            let unavailable = error.downcast::<WarehouseUnavailable>()?;
            let mut output = EstimateOutput::empty("no model produced an estimate");
            output.skipped = models_to_estimate
                .iter()
                .map(|model| EstimateGap {
                    model_name: model.config.name.clone(),
                    reason: format!("not estimated: {unavailable}"),
                })
                .collect();
            return Ok(EstimateReport {
                output,
                pricing_source,
                pricing,
                matched,
            });
        }
    };
    let warehouse_adapter = adapter_registry.warehouse_adapter(pipeline.target_adapter())?;

    let mut estimates = Vec::new();
    let mut skipped: Vec<EstimateGap> = Vec::new();
    let mut explain_failed: Vec<EstimateGap> = Vec::new();
    let is_duckdb = rocky_cfg
        .adapters
        .get(pipeline.target_adapter())
        .is_some_and(|adapter| adapter.adapter_type.as_str() == "duckdb");
    for model in &models_to_estimate {
        let model_ir = model.to_model_ir();
        let dialect = warehouse_adapter.dialect();

        // A DuckDB database is open read-only, and DuckDB refuses to plan a
        // write there, so the model's input query is what gets explained.
        // DuckDB's partition replacement is several statements by design;
        // every other generated string is checked first.
        let checked = rocky_core::sql_gen::generate_transformation_sql(&model_ir, dialect)
            .map_err(|e| format!("SQL generation: {e}"))
            .and_then(|statements| {
                let replaces_partitions = matches!(
                    model_ir.materialization,
                    rocky_ir::MaterializationStrategy::DeleteInsert { .. }
                );
                if !(is_duckdb && replaces_partitions) {
                    let sql = single_explainable_statement(&statements, &model_ir.sql)?;
                    if !is_duckdb {
                        return Ok(sql);
                    }
                }
                single_explainable_statement(&[input_query(&model_ir.sql)], &model_ir.sql)
            });
        let sql = match checked {
            Ok(sql) => sql,
            Err(reason) => {
                debug!(model = %model.config.name, "skipping estimate: {reason}");
                skipped.push(EstimateGap {
                    model_name: model.config.name.clone(),
                    reason,
                });
                continue;
            }
        };

        match run_explain(&warehouse_adapter, &model.config.name, &sql, &pricing).await {
            Ok(estimate) => estimates.push(estimate),
            Err(e) => {
                debug!(model = %model.config.name, "explain failed: {e:#}");
                explain_failed.push(EstimateGap {
                    model_name: model.config.name.clone(),
                    reason: format!("{e:#}"),
                });
            }
        }
    }

    let mut output = if estimates.is_empty() {
        EstimateOutput::empty("no model produced an estimate")
    } else {
        EstimateOutput::new(estimates)
    };
    output.skipped = skipped;
    output.explain_failed = explain_failed;
    Ok(EstimateReport {
        output,
        pricing_source,
        pricing,
        matched,
    })
}

/// Execute `rocky estimate`: [`compute_estimate`], rendered.
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
    let report = compute_estimate(config_path, models_dir, pipeline_name, model_filter).await?;

    if output_json {
        print_json(&report.output)?;
    } else if report.matched == 0 {
        println!("No models found.");
    } else {
        // `--verbose` lists the skips in its own section.
        if !verbose {
            for gap in &report.output.skipped {
                println!("  ! {} — {}", gap.model_name, gap.reason);
            }
        }
        for gap in &report.output.explain_failed {
            println!("  ! {} — explain failed: {}", gap.model_name, gap.reason);
        }
        print!(
            "{}",
            render_estimates_text(
                &report.output.estimates,
                &report.output.skipped,
                report.pricing_source,
                &report.pricing,
                verbose
            )
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
    skipped: &[EstimateGap],
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
            for gap in skipped {
                let _ = writeln!(out, "    {} — {}", gap.model_name, gap.reason);
            }
        }
    }

    out
}

/// A query that reads what a model reads and writes nothing.
fn input_query(model_sql: &str) -> String {
    let body = model_sql.trim_end_matches(|c: char| c == ';' || c.is_whitespace());
    format!("SELECT * FROM (\n{body}\n) AS _rocky_estimate_input")
}

/// The one statement `EXPLAIN` may receive for a model, or why there is none.
///
/// The length of `statements` is not enough: a dialect can put several
/// statements in one element, and a model body can hold its own `;`. So the
/// string is parsed. When the parser cannot read the warehouse's own write
/// syntax (DuckDB `INSERT BY NAME`, Spark `USING DELTA`), the model's input
/// query stands in for it, under the same check.
fn single_explainable_statement(
    statements: &[String],
    model_sql: &str,
) -> std::result::Result<String, String> {
    use rocky_sql::parser::{SingleStatementError, require_single_statement};
    let [sql] = statements else {
        return Err(format!(
            "not explained: the strategy generates {} statements, and EXPLAIN covers only one",
            statements.len()
        ));
    };
    let refused = |e: SingleStatementError| format!("not explained: {e}");
    match require_single_statement(sql) {
        Ok(sql) => Ok(sql.to_string()),
        Err(e @ SingleStatementError::Unparseable(_)) => {
            require_single_statement(&input_query(model_sql))
                .map(str::to_string)
                .map_err(|_| refused(e))
        }
        Err(
            e @ (SingleStatementError::Several(_) | SingleStatementError::AmbiguousSemicolon(_)),
        ) => Err(refused(e)),
    }
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
    #[cfg(feature = "duckdb")]
    use crate::registry::AdapterRegistry;

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
        let skipped = vec![EstimateGap {
            model_name: "broken".to_string(),
            reason: "SQL generation: bad ref".to_string(),
        }];
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

    /// `run_estimate --output json` is `compute_estimate` plus one
    /// `print_json`. With no models the report says so in both shapes, and a
    /// filter naming no model is an error, not an empty report.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn compute_estimate_reports_no_models_in_both_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("x.duckdb");
        let config = dir.path().join("rocky.toml");
        // A TOML literal string, so a Windows path's backslashes survive.
        std::fs::write(
            &config,
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = '{}'\n\n\
                 [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.p.target.governance]\nauto_create_schemas = true\n",
                db.display()
            ),
        )
        .unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();

        let report = compute_estimate(&config, &models, None, None)
            .await
            .unwrap();
        assert_eq!(report.matched, 0);
        assert_eq!(report.output.command, "estimate");
        assert_eq!(report.output.total_models, 0);
        assert_eq!(
            report.output.message.as_deref(),
            Some("no models found to estimate")
        );
        assert_eq!(report.pricing_source, "duckdb");
        assert!(report.output.skipped.is_empty() && report.output.explain_failed.is_empty());

        let err = compute_estimate(&config, &models, None, Some("ghost"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ghost"), "{err}");
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_delete_insert_estimates_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();
        std::fs::write(
            &config,
            format!(
                "[adapter]\ntype = \"duckdb\"\npath = '{}'\n\n\
                 [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                 [pipeline.p.target]\n",
                dir.path().join("warehouse.duckdb").display()
            ),
        )
        .unwrap();
        std::fs::write(
            models_dir.join("orders.sql"),
            "SELECT region, amount FROM warehouse.main.incoming",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("orders.toml"),
            "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"region\"]\n\n\
             [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"orders\"\n",
        )
        .unwrap();
        let cfg = rocky_core::config::load_rocky_config(&config).unwrap();
        {
            let registry = AdapterRegistry::from_config(&cfg).unwrap();
            let adapter = registry.warehouse_adapter("default").unwrap();
            adapter
                .execute_statement(
                    "CREATE TABLE incoming AS SELECT 7 AS region, 20 AS amount;
                     CREATE TABLE orders AS SELECT 7 AS region, 10 AS amount;",
                )
                .await
                .unwrap();
        }

        let report = compute_estimate(&config, &models_dir, Some("p"), Some("orders"))
            .await
            .unwrap();
        assert!(report.output.explain_failed.is_empty(), "{report:?}");
        assert_eq!(report.output.total_models, 1);
        assert!(report.output.estimates[0].raw_explain.contains("incoming"));

        let registry = AdapterRegistry::from_config(&cfg).unwrap();
        let adapter = registry.warehouse_adapter("default").unwrap();
        let rows = adapter
            .execute_query("SELECT amount FROM orders")
            .await
            .unwrap()
            .rows;
        assert_eq!(rows, vec![vec![serde_json::json!("10")]]);
    }

    /// A DuckDB project with one model `t`, a `victim` table and an
    /// `incoming` table, all in `warehouse.duckdb`.
    #[cfg(feature = "duckdb")]
    struct OneModel {
        dir: tempfile::TempDir,
    }

    #[cfg(feature = "duckdb")]
    impl OneModel {
        async fn new(sql: &str, strategy: &str) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let project = Self { dir };
            std::fs::write(
                project.config(),
                format!(
                    "[adapter]\ntype = \"duckdb\"\npath = '{}'\n\n\
                     [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
                     [pipeline.p.target]\n",
                    project.dir.path().join("warehouse.duckdb").display()
                ),
            )
            .unwrap();
            std::fs::create_dir(project.models()).unwrap();
            std::fs::write(project.models().join("t.sql"), sql).unwrap();
            std::fs::write(
                project.models().join("t.toml"),
                format!(
                    "[strategy]\n{strategy}\n\n\
                     [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"t\"\n"
                ),
            )
            .unwrap();
            project
                .adapter()
                .execute_statement(
                    "CREATE TABLE victim AS SELECT 'victim-row-value' AS v;
                     CREATE TABLE incoming AS SELECT 1 AS a, now() AS ts;
                     CREATE TABLE t AS SELECT 1 AS a, now() AS ts;",
                )
                .await
                .unwrap();
            project
        }

        fn config(&self) -> std::path::PathBuf {
            self.dir.path().join("rocky.toml")
        }

        fn models(&self) -> std::path::PathBuf {
            self.dir.path().join("models")
        }

        fn adapter(&self) -> Arc<dyn WarehouseAdapter> {
            let cfg = rocky_core::config::load_rocky_config(&self.config()).unwrap();
            let registry = crate::registry::AdapterRegistry::from_config(&cfg).unwrap();
            registry.warehouse_adapter("default").unwrap()
        }

        async fn victim_rows(&self) -> Vec<Vec<serde_json::Value>> {
            self.adapter()
                .execute_query("SELECT v FROM victim")
                .await
                .expect("the victim table is still there")
                .rows
        }

        async fn estimate(&self) -> EstimateReport {
            compute_estimate(&self.config(), &self.models(), None, Some("t"))
                .await
                .unwrap()
        }

        fn database(&self) -> std::path::PathBuf {
            self.dir.path().join("warehouse.duckdb")
        }

        /// Every file beside the config, with its bytes.
        fn files(&self) -> std::collections::BTreeMap<String, Vec<u8>> {
            std::fs::read_dir(self.dir.path())
                .unwrap()
                .map(|entry| entry.unwrap())
                .filter(|entry| entry.file_type().unwrap().is_file())
                .map(|entry| {
                    (
                        entry.file_name().to_string_lossy().into_owned(),
                        std::fs::read(entry.path()).unwrap(),
                    )
                })
                .collect()
        }
    }

    /// The hazard these tests guard against, shown on the adapter itself:
    /// `EXPLAIN` of a two-statement string runs the second statement.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_explain_runs_every_statement_after_the_first() {
        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE TABLE victim AS SELECT 1 AS v")
            .await
            .unwrap();
        adapter
            .explain("SELECT 1; DROP TABLE victim")
            .await
            .unwrap();
        assert!(adapter.execute_query("SELECT v FROM victim").await.is_err());
    }

    /// A model body holding a second statement is skipped with a reason, and
    /// the statement does not run, on every strategy that carried it into the
    /// `EXPLAIN` string, the DuckDB `delete_insert` wrapper included.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn estimate_never_runs_a_second_statement() {
        let full = "type = \"full_refresh\"";
        for (sql, strategy) in [
            ("SELECT 1 AS a; DROP TABLE victim", full),
            ("SELECT 1 AS a; DELETE FROM victim; SELECT 2", full),
            ("SELECT 1 AS a; SELECT 1, v FROM victim", full),
            ("SELECT 1 AS a; DROP TABLE victim", "type = \"view\""),
            (
                "SELECT 1 AS a) AS x; DROP TABLE victim; SELECT (1",
                "type = \"delete_insert\"\npartition_by = [\"a\"]",
            ),
            ("SELECT 1 AS a /* /* */ ; DROP TABLE victim; /* */ */", full),
            ("SELECT 1 AS a -- x\r; DROP TABLE victim", full),
        ] {
            let project = OneModel::new(sql, strategy).await;
            let report = project.estimate().await;
            assert_eq!(
                project.victim_rows().await,
                vec![vec![serde_json::json!("victim-row-value")]],
                "{sql}"
            );
            assert!(report.output.estimates.is_empty(), "{sql}: {report:?}");
            assert!(report.output.explain_failed.is_empty(), "{sql}: {report:?}");
            assert_eq!(report.output.skipped.len(), 1, "{sql}: {report:?}");
            assert!(
                report.output.skipped[0]
                    .reason
                    .starts_with("not explained:"),
                "{sql}: {}",
                report.output.skipped[0].reason
            );
        }
    }

    /// The guard does not turn ordinary models into skips.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn ordinary_duckdb_models_still_estimate() {
        let select = "SELECT a, ts FROM warehouse.main.incoming";
        for (sql, strategy) in [
            (select.to_string(), "type = \"full_refresh\""),
            (format!("{select};\n"), "type = \"full_refresh\""),
            (
                format!("-- one; two\n{select} WHERE 'a;b' <> ''"),
                "type = \"full_refresh\"",
            ),
            (select.to_string(), "type = \"view\""),
            (
                select.to_string(),
                "type = \"incremental\"\ntimestamp_column = \"ts\"",
            ),
            (
                select.to_string(),
                "type = \"merge\"\nunique_key = [\"a\"]\nupdate_columns = [\"ts\"]",
            ),
            (
                select.to_string(),
                "type = \"delete_insert\"\npartition_by = [\"a\"]",
            ),
        ] {
            let project = OneModel::new(&sql, strategy).await;
            let before = project.files();
            let report = project.estimate().await;
            assert_eq!(report.output.estimates.len(), 1, "{strategy}: {report:?}");
            assert!(!report.output.estimates[0].raw_explain.is_empty());
            assert!(report.output.skipped.is_empty(), "{report:?}");
            assert!(
                project.files() == before,
                "{strategy}: the estimate changed a file"
            );
        }
    }

    /// The in-place overwrite of an attached Iceberg catalog is
    /// `BEGIN; ...; DELETE; INSERT; COMMIT`. Joined and explained, only
    /// `BEGIN` is explained, so the `DELETE` and `INSERT` rewrote the target.
    /// Attaching a real Iceberg catalog needs a server, so this uses the
    /// statement list the dialect generates.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn the_in_place_overwrite_statements_are_never_explained() {
        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory().unwrap();
        adapter
            .execute_statement("CREATE TABLE t AS SELECT 1 AS a UNION ALL SELECT 2")
            .await
            .unwrap();
        let statements = rocky_core::sql_gen::in_place_overwrite_statements(
            adapter.dialect(),
            "memory.main.t",
            "SELECT 9 AS a",
        );
        assert!(single_explainable_statement(&statements, "SELECT 9 AS a").is_err());
        assert!(single_explainable_statement(&[statements.join(";\n")], "SELECT 9 AS a").is_err());

        // What the joined form did.
        let _ = adapter.explain(&statements.join(";\n")).await;
        let rows = adapter.execute_query("SELECT a FROM t").await.unwrap().rows;
        assert_eq!(rows, vec![vec![serde_json::json!("9")]]);
    }

    /// PostgreSQL and Redshift return their `delete_insert` as ONE vector
    /// element holding two statements, and their connector runs both.
    #[test]
    fn a_joined_postgres_delete_insert_is_refused() {
        for adapter_type in ["postgres", "redshift"] {
            let Some(dialect) = crate::registry::warehouse_dialect_for_type(adapter_type) else {
                panic!("no dialect for {adapter_type}");
            };
            let statements = dialect.delete_insert_sql("db.s.t", &["a".into()], "SELECT 1 AS a");
            assert_eq!(statements.len(), 1, "{adapter_type}: {statements:?}");
            let error = single_explainable_statement(&statements, "SELECT 1 AS a").unwrap_err();
            assert!(error.contains("2 statements"), "{adapter_type}: {error}");
        }
    }

    /// An absent DuckDB database is not created. Each model is reported as
    /// not estimated, with the reason.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn estimate_does_not_create_an_absent_database() {
        let project = OneModel::new("SELECT 1 AS a", "type = \"full_refresh\"").await;
        std::fs::remove_file(project.database()).unwrap();
        let before = project.files();

        let report = project.estimate().await;
        assert!(project.files() == before, "the estimate created a file");
        assert!(report.output.estimates.is_empty());
        assert_eq!(report.output.skipped.len(), 1, "{report:?}");
        assert_eq!(report.output.skipped[0].model_name, "t");
        assert!(
            report.output.skipped[0].reason.contains("does not exist"),
            "{}",
            report.output.skipped[0].reason
        );
        assert_eq!(
            report.output.message.as_deref(),
            Some("no model produced an estimate")
        );
    }

    /// A file DuckDB cannot open read-only is a reason, not a path.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn estimate_reports_an_unopenable_database_without_its_path() {
        let project = OneModel::new("SELECT 1 AS a", "type = \"full_refresh\"").await;
        std::fs::write(project.database(), "not a database").unwrap();
        let report = project.estimate().await;
        assert_eq!(report.output.skipped.len(), 1, "{report:?}");
        let reason = &report.output.skipped[0].reason;
        assert!(reason.contains("could not be opened read-only"), "{reason}");
        assert!(!reason.contains("warehouse.duckdb"), "{reason}");
        assert_eq!(
            std::fs::read_to_string(project.database()).unwrap(),
            "not a database"
        );
    }

    /// `rocky plan --cost-estimate adapter` estimates in a process that may
    /// already hold the database open read-write.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn estimate_works_beside_a_read_write_connection_in_the_process() {
        let project = OneModel::new("SELECT 1 AS a", "type = \"full_refresh\"").await;
        let held = project.adapter();
        let report = project.estimate().await;
        assert_eq!(report.output.estimates.len(), 1, "{report:?}");
        drop(held);
    }
}
