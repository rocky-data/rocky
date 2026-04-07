//! Cost model and materialization strategy recommendations.
//!
//! Analyzes model execution history to recommend optimal materialization
//! strategies (view, table, ephemeral) based on compute cost, storage cost,
//! and downstream consumer patterns.

use serde::{Deserialize, Serialize};

/// Cost estimate and strategy recommendation for a single model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationCost {
    /// Name of the model being analyzed.
    pub model_name: String,
    /// Current materialization strategy (e.g., "table", "view", "incremental").
    pub current_strategy: String,
    /// Estimated compute cost per run in dollars.
    pub compute_cost_per_run: f64,
    /// Estimated storage cost per month in dollars.
    pub storage_cost_per_month: f64,
    /// Number of downstream models that reference this one.
    pub downstream_references: usize,
    /// Recommended strategy after analysis.
    pub recommended_strategy: String,
    /// Estimated monthly savings if recommendation is adopted.
    pub estimated_monthly_savings: f64,
    /// Human-readable reasoning for the recommendation.
    pub reasoning: String,
}

/// Configuration for the cost model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostConfig {
    /// Cost per GB of storage per month (default: $0.023 for S3/DBFS).
    pub storage_cost_per_gb_month: f64,
    /// Cost per second of compute (default: $0.002 for DBU).
    pub compute_cost_per_second: f64,
    /// Minimum number of historical runs required before making recommendations.
    pub min_history_runs: usize,
}

impl Default for CostConfig {
    fn default() -> Self {
        CostConfig {
            storage_cost_per_gb_month: 0.023,
            compute_cost_per_second: 0.002,
            min_history_runs: 5,
        }
    }
}

impl From<crate::config::CostSection> for CostConfig {
    fn from(section: crate::config::CostSection) -> Self {
        CostConfig {
            storage_cost_per_gb_month: section.storage_cost_per_gb_month,
            compute_cost_per_second: section.compute_cost_per_dbu / 3600.0, // DBU per hour -> per second
            min_history_runs: section.min_history_runs,
        }
    }
}

/// Execution statistics for a model, used as input for cost analysis.
#[derive(Debug, Clone)]
pub struct ModelStats {
    /// Name of the model.
    pub model_name: String,
    /// Current materialization strategy.
    pub current_strategy: String,
    /// Average execution duration in seconds across recent runs.
    pub avg_duration_seconds: f64,
    /// Estimated size of materialized output in GB.
    pub estimated_size_gb: f64,
    /// Number of downstream models that reference this model.
    pub downstream_references: usize,
    /// Number of historical runs available.
    pub history_runs: usize,
    /// Average number of runs per month.
    pub runs_per_month: f64,
}

/// Recommends the optimal materialization strategy based on cost analysis.
///
/// Decision logic:
/// - **View**: Cheaper to recompute each time than to store. Single or no
///   downstream consumers, fast execution (< 10s).
/// - **Table**: Multiple downstream consumers benefit from pre-materialized data.
///   Higher storage cost justified by reduced total compute across consumers.
/// - **Ephemeral**: Very fast execution (< 2s), single downstream consumer.
///   Inlined into the consumer's query to avoid materialization overhead.
pub fn recommend_strategy(stats: &ModelStats, config: &CostConfig) -> MaterializationCost {
    let compute_cost_per_run = stats.avg_duration_seconds * config.compute_cost_per_second;
    let storage_cost_per_month = stats.estimated_size_gb * config.storage_cost_per_gb_month;
    let monthly_compute = compute_cost_per_run * stats.runs_per_month;

    // Not enough history to make a recommendation
    if stats.history_runs < config.min_history_runs {
        return MaterializationCost {
            model_name: stats.model_name.clone(),
            current_strategy: stats.current_strategy.clone(),
            compute_cost_per_run,
            storage_cost_per_month,
            downstream_references: stats.downstream_references,
            recommended_strategy: stats.current_strategy.clone(),
            estimated_monthly_savings: 0.0,
            reasoning: format!(
                "insufficient history: {} runs (need {})",
                stats.history_runs, config.min_history_runs
            ),
        };
    }

    let (recommended, reasoning, savings) =
        if stats.avg_duration_seconds < 2.0 && stats.downstream_references <= 1 {
            // Ephemeral: fast single-consumer model, inline it
            let savings = storage_cost_per_month;
            (
                "ephemeral".to_string(),
                format!(
                    "fast execution ({:.1}s) with {} downstream consumer(s); \
                 inline into consumer query to eliminate materialization overhead",
                    stats.avg_duration_seconds, stats.downstream_references
                ),
                savings,
            )
        } else if stats.downstream_references >= 2 {
            // Table: multiple consumers benefit from pre-materialization
            // Cost of recomputing for each consumer vs. materializing once
            let recompute_cost = monthly_compute * stats.downstream_references as f64;
            let table_cost = monthly_compute + storage_cost_per_month;
            if recompute_cost > table_cost {
                let savings = recompute_cost - table_cost;
                (
                    "table".to_string(),
                    format!(
                        "{} downstream consumers; materializing once (${:.4}/mo) \
                     is cheaper than recomputing for each (${:.4}/mo)",
                        stats.downstream_references, table_cost, recompute_cost
                    ),
                    savings,
                )
            } else {
                // Even with multiple consumers, view is cheaper
                let savings = table_cost - recompute_cost;
                (
                    "view".to_string(),
                    format!(
                        "{} downstream consumers but compute is cheap enough \
                     that a view (${:.4}/mo) beats table (${:.4}/mo)",
                        stats.downstream_references, recompute_cost, table_cost
                    ),
                    savings,
                )
            }
        } else if monthly_compute < storage_cost_per_month {
            // View: cheaper to recompute than to store
            let savings = storage_cost_per_month - monthly_compute;
            (
                "view".to_string(),
                format!(
                    "compute cost (${:.4}/mo) is less than storage (${:.4}/mo); \
                 recompute on read instead of materializing",
                    monthly_compute, storage_cost_per_month
                ),
                savings,
            )
        } else {
            // Table: storage is cheap relative to compute
            let savings = monthly_compute - storage_cost_per_month;
            (
                "table".to_string(),
                format!(
                    "storage (${:.4}/mo) is less than recompute cost (${:.4}/mo); \
                 materialize to avoid repeated computation",
                    storage_cost_per_month, monthly_compute
                ),
                savings,
            )
        };

    // If the recommended strategy matches the current one, no savings
    let actual_savings = if recommended == stats.current_strategy {
        0.0
    } else {
        savings
    };

    MaterializationCost {
        model_name: stats.model_name.clone(),
        current_strategy: stats.current_strategy.clone(),
        compute_cost_per_run,
        storage_cost_per_month,
        downstream_references: stats.downstream_references,
        recommended_strategy: recommended,
        estimated_monthly_savings: actual_savings,
        reasoning,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> CostConfig {
        CostConfig::default()
    }

    #[test]
    fn test_ephemeral_fast_single_consumer() {
        let stats = ModelStats {
            model_name: "staging_orders".into(),
            current_strategy: "table".into(),
            avg_duration_seconds: 1.5,
            estimated_size_gb: 0.5,
            downstream_references: 1,
            history_runs: 10,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "ephemeral");
        assert!(result.estimated_monthly_savings > 0.0);
        assert!(result.reasoning.contains("fast execution"));
    }

    #[test]
    fn test_table_multiple_consumers() {
        let stats = ModelStats {
            model_name: "dim_customers".into(),
            current_strategy: "view".into(),
            avg_duration_seconds: 30.0,
            estimated_size_gb: 2.0,
            downstream_references: 5,
            history_runs: 20,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "table");
        assert!(result.estimated_monthly_savings > 0.0);
        assert!(result.reasoning.contains("downstream consumers"));
    }

    #[test]
    fn test_view_cheap_recompute() {
        let stats = ModelStats {
            model_name: "simple_transform".into(),
            current_strategy: "table".into(),
            avg_duration_seconds: 5.0,
            estimated_size_gb: 50.0,
            downstream_references: 1,
            history_runs: 10,
            runs_per_month: 4.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "view");
        // monthly compute = 5 * 0.002 * 4 = 0.04
        // storage = 50 * 0.023 = 1.15
        assert!(result.estimated_monthly_savings > 1.0);
    }

    #[test]
    fn test_insufficient_history() {
        let stats = ModelStats {
            model_name: "new_model".into(),
            current_strategy: "table".into(),
            avg_duration_seconds: 10.0,
            estimated_size_gb: 1.0,
            downstream_references: 3,
            history_runs: 2,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "table"); // stays the same
        assert_eq!(result.estimated_monthly_savings, 0.0);
        assert!(result.reasoning.contains("insufficient history"));
    }

    #[test]
    fn test_no_savings_when_already_optimal() {
        let stats = ModelStats {
            model_name: "already_optimal".into(),
            current_strategy: "ephemeral".into(),
            avg_duration_seconds: 0.5,
            estimated_size_gb: 0.01,
            downstream_references: 1,
            history_runs: 10,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "ephemeral");
        assert_eq!(result.estimated_monthly_savings, 0.0);
    }

    #[test]
    fn test_table_when_storage_cheap() {
        // High compute, low storage — table wins
        let stats = ModelStats {
            model_name: "heavy_compute".into(),
            current_strategy: "view".into(),
            avg_duration_seconds: 120.0,
            estimated_size_gb: 0.1,
            downstream_references: 1,
            history_runs: 10,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "table");
        // compute = 120 * 0.002 * 30 = 7.2, storage = 0.1 * 0.023 = 0.0023
        assert!(result.estimated_monthly_savings > 7.0);
    }

    #[test]
    fn test_custom_config() {
        let config = CostConfig {
            storage_cost_per_gb_month: 0.10,
            compute_cost_per_second: 0.001,
            min_history_runs: 3,
        };
        let stats = ModelStats {
            model_name: "custom".into(),
            current_strategy: "table".into(),
            avg_duration_seconds: 5.0,
            estimated_size_gb: 10.0,
            downstream_references: 1,
            history_runs: 5,
            runs_per_month: 10.0,
        };
        let result = recommend_strategy(&stats, &config);
        // compute = 5 * 0.001 * 10 = 0.05, storage = 10 * 0.10 = 1.0
        assert_eq!(result.recommended_strategy, "view");
    }

    #[test]
    fn test_cost_section_defaults() {
        let section = crate::config::CostSection::default();
        assert_eq!(section.warehouse_size, "Medium");
        let config: CostConfig = section.into();
        assert!((config.storage_cost_per_gb_month - 0.023).abs() < f64::EPSILON);
        assert!(config.compute_cost_per_second > 0.0);
        // 0.40 DBU/hour = 0.40/3600 per second
        let expected = 0.40 / 3600.0;
        assert!((config.compute_cost_per_second - expected).abs() < 1e-10);
        assert_eq!(config.min_history_runs, 5);
    }

    #[test]
    fn test_ephemeral_zero_consumers() {
        let stats = ModelStats {
            model_name: "leaf_model".into(),
            current_strategy: "table".into(),
            avg_duration_seconds: 0.3,
            estimated_size_gb: 0.01,
            downstream_references: 0,
            history_runs: 10,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "ephemeral");
    }
}
