//! `rocky optimize` — cost model analysis and materialization recommendations.

use std::path::Path;

use anyhow::Result;

use rocky_core::optimize::{CostConfig, MaterializationCost, ModelStats, recommend_strategy};
use rocky_core::state::StateStore;

use crate::output::{OptimizeOutput, OptimizeRecommendation, print_json};

/// Execute `rocky optimize`.
pub fn run_optimize(
    state_path: &Path,
    model_filter: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let store = StateStore::open(state_path)?;
    let config = CostConfig::default();

    // Get all runs to extract model names and compute stats
    let runs = store.list_runs(100)?;

    if runs.is_empty() {
        if output_json {
            print_json(&OptimizeOutput::empty("no run history available"))?;
        } else {
            println!("No run history available. Run `rocky run` first to collect execution data.");
        }
        return Ok(());
    }

    // Collect unique model names across all runs
    let mut model_names: Vec<String> = runs
        .iter()
        .flat_map(|r| r.models_executed.iter().map(|m| m.model_name.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    model_names.sort();

    // Apply filter
    if let Some(filter) = model_filter {
        model_names.retain(|name| name.contains(filter));
    }

    // Build stats and recommendations for each model
    let mut recommendations: Vec<MaterializationCost> = Vec::new();
    let total_runs = runs.len();

    for model_name in &model_names {
        let history = store.get_model_history(model_name, 100)?;
        if history.is_empty() {
            continue;
        }

        let avg_duration_seconds = history.iter().map(|m| m.duration_ms as f64).sum::<f64>()
            / history.len() as f64
            / 1000.0;

        // Estimate size from bytes_written if available
        let estimated_size_gb = history
            .iter()
            .filter_map(|m| m.bytes_written)
            .next_back()
            .map(|b| b as f64 / 1_073_741_824.0)
            .unwrap_or(0.1); // default 100MB estimate

        // Rough estimate of runs per month
        let runs_per_month = if total_runs > 1 {
            let first = runs.last().unwrap().started_at;
            let last = runs.first().unwrap().started_at;
            let span_days = (last - first).num_days().max(1) as f64;
            (history.len() as f64 / span_days) * 30.0
        } else {
            30.0 // assume daily
        };

        let stats = ModelStats {
            model_name: model_name.clone(),
            current_strategy: "table".to_string(), // default assumption
            avg_duration_seconds,
            estimated_size_gb,
            downstream_references: 0, // would need DAG info
            history_runs: history.len(),
            runs_per_month,
        };

        recommendations.push(recommend_strategy(&stats, &config));
    }

    if output_json {
        let typed_recs: Vec<OptimizeRecommendation> = recommendations
            .iter()
            .map(|r| OptimizeRecommendation {
                model_name: r.model_name.clone(),
                current_strategy: r.current_strategy.clone(),
                recommended_strategy: r.recommended_strategy.clone(),
                estimated_monthly_savings: r.estimated_monthly_savings,
                reasoning: r.reasoning.clone(),
            })
            .collect();
        let output = OptimizeOutput::new(typed_recs);
        print_json(&output)?;
    } else {
        println!(
            "{:<30} {:<12} {:<14} {:<12} {:<10}",
            "MODEL", "CURRENT", "RECOMMENDED", "SAVINGS/MO", "REASONING"
        );
        println!("{}", "-".repeat(90));

        for rec in &recommendations {
            println!(
                "{:<30} {:<12} {:<14} ${:<11.4} {}",
                truncate(&rec.model_name, 29),
                rec.current_strategy,
                rec.recommended_strategy,
                rec.estimated_monthly_savings,
                truncate(&rec.reasoning, 40),
            );
        }

        let total_savings: f64 = recommendations
            .iter()
            .map(|r| r.estimated_monthly_savings)
            .sum();
        println!();
        println!("Total estimated monthly savings: ${total_savings:.2}");
        println!("Models analyzed: {}", recommendations.len());
        println!();
        println!(
            "Tip: Run `rocky compile --output json` for inferred incrementality hints on full_refresh models."
        );
    }

    Ok(())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}
