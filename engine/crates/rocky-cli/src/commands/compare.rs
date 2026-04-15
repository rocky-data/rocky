//! `rocky compare` — compare shadow tables against production targets.
//!
//! Connects to the warehouse, finds table pairs (shadow + production),
//! compares row counts and schemas, and outputs per-table verdicts.

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::compare::{self, ComparisonResult, ComparisonThresholds, ComparisonVerdict};
use rocky_core::ir::TargetRef;
use rocky_core::shadow::ShadowConfig;
use rocky_core::traits::WarehouseAdapter;

use crate::output::{CompareOutput, TableCompareResult, print_json};
use crate::registry::{self, AdapterRegistry};

use super::{matches_filter, parse_filter};

const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn compare(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    shadow_config: &ShadowConfig,
    thresholds: &ComparisonThresholds,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_pipeline_name, pipeline) =
        registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name_arg)?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let adapter = registry.warehouse_adapter(&pipeline.target.adapter)?;

    let pattern = pipeline.schema_pattern()?;
    let parsed_filter = filter.map(parse_filter).transpose()?;

    // Discover connectors to find table pairs
    let connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        anyhow::bail!("no discovery adapter configured — compare requires source discovery");
    };

    let target_catalog_template = &pipeline.target.catalog_template;
    let target_schema_template = &pipeline.target.schema_template;

    let mut output = CompareOutput {
        version: VERSION.to_string(),
        command: "compare".to_string(),
        filter: filter.unwrap_or("").to_string(),
        tables_compared: 0,
        tables_passed: 0,
        tables_warned: 0,
        tables_failed: 0,
        results: vec![],
        overall_verdict: "pass".to_string(),
    };

    for conn in &connectors {
        let parsed = match pattern.parse(&conn.schema) {
            Ok(p) => p,
            Err(_) => continue,
        };

        if let Some((ref filter_key, ref filter_value)) = parsed_filter {
            if !matches_filter(conn, &parsed, filter_key, filter_value) {
                continue;
            }
        }

        let target_sep = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        let target_catalog = parsed.resolve_template(target_catalog_template, target_sep);
        let target_schema = parsed.resolve_template(target_schema_template, target_sep);

        for table in &conn.tables {
            // Build production and shadow target refs
            let prod_target = TargetRef {
                catalog: target_catalog.clone(),
                schema: target_schema.clone(),
                table: table.name.clone(),
            };
            let shadow_target = rocky_core::shadow::shadow_target(&prod_target, shadow_config);

            // Get row counts
            let prod_count = get_row_count(&*adapter, &prod_target).await.unwrap_or(0);
            let shadow_count = get_row_count(&*adapter, &shadow_target).await.unwrap_or(0);

            let (row_count_match, row_count_diff, row_count_diff_pct) =
                compare::compare_row_counts(shadow_count, prod_count);

            // Get schemas
            let prod_table_ref = rocky_core::ir::TableRef {
                catalog: prod_target.catalog.clone(),
                schema: prod_target.schema.clone(),
                table: prod_target.table.clone(),
            };
            let shadow_table_ref = rocky_core::ir::TableRef {
                catalog: shadow_target.catalog.clone(),
                schema: shadow_target.schema.clone(),
                table: shadow_target.table.clone(),
            };

            let prod_cols = adapter
                .describe_table(&prod_table_ref)
                .await
                .unwrap_or_default();
            let shadow_cols = adapter
                .describe_table(&shadow_table_ref)
                .await
                .unwrap_or_default();

            let schema_diffs = compare::compare_schemas(&shadow_cols, &prod_cols);
            let schema_match = schema_diffs.is_empty();

            // Build comparison result and evaluate
            let comparison = ComparisonResult {
                table: prod_target.full_name(),
                row_count_match,
                shadow_count,
                production_count: prod_count,
                row_count_diff,
                row_count_diff_pct,
                schema_match,
                schema_diffs: schema_diffs.clone(),
                sample_match: None,
                sample_mismatches: vec![],
            };

            let verdict = compare::evaluate_comparison(&comparison, thresholds);

            let verdict_str = match &verdict {
                ComparisonVerdict::Pass => "pass",
                ComparisonVerdict::Warn(_) => "warn",
                ComparisonVerdict::Fail(_) => "fail",
            };

            match verdict {
                ComparisonVerdict::Pass => output.tables_passed += 1,
                ComparisonVerdict::Warn(_) => output.tables_warned += 1,
                ComparisonVerdict::Fail(_) => output.tables_failed += 1,
            }

            let schema_diff_strs: Vec<String> =
                schema_diffs.iter().map(|d| format!("{d:?}")).collect();

            output.results.push(TableCompareResult {
                production_table: prod_target.full_name(),
                shadow_table: shadow_target.full_name(),
                row_count_match,
                production_count: prod_count,
                shadow_count,
                row_count_diff_pct,
                schema_match,
                schema_diffs: schema_diff_strs,
                verdict: verdict_str.to_string(),
            });

            output.tables_compared += 1;

            info!(
                production = prod_target.full_name(),
                shadow = shadow_target.full_name(),
                verdict = verdict_str,
                "compared table"
            );
        }
    }

    // Overall verdict
    output.overall_verdict = if output.tables_failed > 0 {
        "fail".to_string()
    } else if output.tables_warned > 0 {
        "warn".to_string()
    } else {
        "pass".to_string()
    };

    if output_json {
        print_json(&output)?;
    } else {
        println!("  Rocky Compare");
        println!();
        println!(
            "  Tables: {} compared, {} passed, {} warned, {} failed",
            output.tables_compared,
            output.tables_passed,
            output.tables_warned,
            output.tables_failed
        );
        println!("  Overall: {}", output.overall_verdict.to_uppercase());
        println!();
        for result in &output.results {
            let icon = match result.verdict.as_str() {
                "pass" => "  OK",
                "warn" => "WARN",
                "fail" => "FAIL",
                _ => "  ??",
            };
            println!(
                "  [{icon}] {} (prod={}, shadow={}, diff={:.2}%)",
                result.production_table,
                result.production_count,
                result.shadow_count,
                result.row_count_diff_pct * 100.0,
            );
        }
    }

    if output.tables_failed > 0 {
        anyhow::bail!("{} table(s) failed comparison", output.tables_failed);
    }

    Ok(())
}

/// Get row count for a target table, returning 0 if table doesn't exist.
async fn get_row_count(adapter: &dyn WarehouseAdapter, target: &TargetRef) -> Result<u64> {
    let table_name = target
        .validated_full_name()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let sql = format!("SELECT COUNT(*) FROM {table_name}");
    let result = adapter
        .execute_query(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let count = result.rows[0][0]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(count)
}
