//! Cost model and materialization strategy recommendations.
//!
//! Analyzes model execution history to recommend optimal materialization
//! strategies (view, table) based on compute cost, storage cost,
//! and downstream consumer patterns.

use serde::{Deserialize, Serialize};

/// Sentinel [`ModelStats::current_strategy`] for a model whose configured
/// strategy could not be determined — e.g. it appears only in run history
/// and is absent from the compiled project. [`recommend_strategy`] treats
/// this as a hard "make no recommendation" signal rather than comparing it
/// against an assumed baseline (#2056).
pub const UNKNOWN_STRATEGY: &str = "unknown";

/// Cost estimate and strategy recommendation for a single model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationCost {
    /// Name of the model being analyzed.
    pub model_name: String,
    /// Current materialization strategy (e.g., "table", "view", "incremental";
    /// [`UNKNOWN_STRATEGY`] when the caller could not determine it).
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
    /// Cost per GB of storage per month (default: $0.023 for S3/DBFS —
    /// [`crate::config::CostSection`]'s own default).
    pub storage_cost_per_gb_month: f64,
    /// Cost per second of compute (default: ~$0.0027, i.e. $0.40/DBU-hour at
    /// the "Medium" warehouse size's 24 DBU/hour — see
    /// [`crate::cost::warehouse_size_to_dbu_per_hour`]).
    pub compute_cost_per_second: f64,
    /// Minimum number of historical runs required before making recommendations.
    pub min_history_runs: usize,
}

impl Default for CostConfig {
    /// Deliberately delegates to [`crate::config::CostSection::default`]'s own
    /// conversion rather than restating separate literals: the two used to
    /// disagree (a project whose `rocky.toml` declared no `[cost]` block, or
    /// one that explicitly restated the default values, got a different
    /// price than a config-less project — #2056, found in Codex review).
    /// Deriving this default FROM `CostSection`'s makes the two identical by
    /// construction, for every caller, not just the ones a value-equality
    /// guard happens to catch.
    fn default() -> Self {
        crate::config::CostSection::default().into()
    }
}

impl From<crate::config::CostSection> for CostConfig {
    fn from(section: crate::config::CostSection) -> Self {
        // `compute_cost_per_dbu` is dollars per DBU-*hour* (Databricks'/
        // Snowflake's own billing unit — see `CostSection`'s doc comment
        // and the $0.40 default, which is a per-hour DBU rate). Converting
        // it to a per-second compute price therefore needs the warehouse's
        // DBU/hour throughput, not just a division by 3600: dividing the
        // per-DBU-hour rate straight by 3600 silently assumes 1 DBU/hour,
        // underpricing every other size (24x for the "Medium" default,
        // `rocky cost`'s `adapter_pricing` already folds in this same
        // factor via `warehouse_size_to_dbu_per_hour` — this conversion
        // must match it (found in Codex review of #2056: this impl was
        // unused in production before this PR wired it into
        // `rocky optimize`, so the mismatch was never observed).
        let dbu_per_hour = crate::cost::warehouse_size_to_dbu_per_hour(&section.warehouse_size);
        CostConfig {
            storage_cost_per_gb_month: section.storage_cost_per_gb_month,
            compute_cost_per_second: section.compute_cost_per_dbu * dbu_per_hour / 3600.0,
            min_history_runs: section.min_history_runs,
        }
    }
}

/// Execution statistics for a model, used as input for cost analysis.
#[derive(Debug, Clone)]
pub struct ModelStats {
    /// Name of the model.
    pub model_name: String,
    /// Current materialization strategy, or [`UNKNOWN_STRATEGY`] when the
    /// caller could not resolve it (e.g. a history-only model absent from
    /// the compiled project).
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
///   A view is also what a very fast (< 2s) single-consumer model gets: it
///   stores nothing, and recomputing it on read costs little.
/// - **[`UNKNOWN_STRATEGY`]**: neither. `stats.current_strategy ==
///   UNKNOWN_STRATEGY` short-circuits before either branch, echoing the
///   sentinel back as both current and recommended strategy with zero
///   savings, rather than comparing it against a guess.
pub fn recommend_strategy(stats: &ModelStats, config: &CostConfig) -> MaterializationCost {
    let compute_cost_per_run = stats.avg_duration_seconds * config.compute_cost_per_second;
    let storage_cost_per_month = stats.estimated_size_gb * config.storage_cost_per_gb_month;
    let monthly_compute = compute_cost_per_run * stats.runs_per_month;

    // The model's real strategy is unknown to the caller (#2056) — comparing
    // it against a recommended "table"/"view" would silently pass off a
    // guess as a considered recommendation. Report the cost inputs, make no
    // recommendation.
    if stats.current_strategy == UNKNOWN_STRATEGY {
        return MaterializationCost {
            model_name: stats.model_name.clone(),
            current_strategy: stats.current_strategy.clone(),
            compute_cost_per_run,
            storage_cost_per_month,
            downstream_references: stats.downstream_references,
            recommended_strategy: stats.current_strategy.clone(),
            estimated_monthly_savings: 0.0,
            reasoning: "current strategy is unknown (model not found in the compiled project); \
                        no recommendation"
                .to_string(),
        };
    }

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
            // A view: recomputing a fast model for one consumer beats
            // storing it. `ephemeral` used to be recommended here and is now
            // a compile error — it is never inlined (E038, #1996).
            let savings = storage_cost_per_month;
            (
                "view".to_string(),
                format!(
                    "fast execution ({:.1}s) with {} downstream consumer(s); \
                 recompute on read instead of storing a table",
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

    /// A fast single-consumer model gets `view`, never `ephemeral`:
    /// `ephemeral` is a compile error (E038, #1996), so recommending it would
    /// point at a strategy the project cannot compile.
    #[test]
    fn test_view_for_a_fast_single_consumer_model() {
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
        assert_eq!(result.recommended_strategy, "view");
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
        // monthly compute = 5 * ~0.0026667 * 4 ≈ 0.053
        // storage = 50 * 0.023 = 1.15
        assert!(result.estimated_monthly_savings > 1.0);
    }

    /// An unknown current strategy (#2056: a model absent from the compiled
    /// project) must never be told to "switch" — the recommender has no real
    /// baseline to compare against, so it reports the sentinel back and
    /// zero savings rather than comparing it against a guessed "table".
    #[test]
    fn test_unknown_strategy_gets_no_recommendation() {
        let stats = ModelStats {
            model_name: "history_only_model".into(),
            current_strategy: UNKNOWN_STRATEGY.into(),
            avg_duration_seconds: 30.0,
            estimated_size_gb: 2.0,
            downstream_references: 5,
            history_runs: 20,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.current_strategy, UNKNOWN_STRATEGY);
        assert_eq!(result.recommended_strategy, UNKNOWN_STRATEGY);
        assert_eq!(result.estimated_monthly_savings, 0.0);
        assert!(result.reasoning.contains("unknown"));
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
            current_strategy: "view".into(),
            avg_duration_seconds: 0.5,
            estimated_size_gb: 0.01,
            downstream_references: 1,
            history_runs: 10,
            runs_per_month: 30.0,
        };
        let result = recommend_strategy(&stats, &default_config());
        assert_eq!(result.recommended_strategy, "view");
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
        // compute = 120 * ~0.0026667 * 30 ≈ 9.6, storage = 0.1 * 0.023 = 0.0023
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

    /// The "Medium" default carries a real DBU/hour throughput (24, per
    /// `warehouse_size_to_dbu_per_hour`), so the per-second price is
    /// `$/DBU-hour * DBU/hour / 3600`, not `$/DBU-hour / 3600` — the latter
    /// silently assumes 1 DBU/hour and underprices every warehouse size by
    /// that size's DBU/hour factor (found by Codex review of #2056).
    #[test]
    fn test_cost_section_defaults() {
        let section = crate::config::CostSection::default();
        assert_eq!(section.warehouse_size, "Medium");
        let config: CostConfig = section.into();
        assert!((config.storage_cost_per_gb_month - 0.023).abs() < f64::EPSILON);
        assert!(config.compute_cost_per_second > 0.0);
        // 0.40 $/DBU-hour * 24 DBU/hour (Medium) / 3600 s/hour
        let expected = 0.40 * 24.0 / 3600.0;
        assert!((config.compute_cost_per_second - expected).abs() < 1e-10);
        assert_eq!(config.min_history_runs, 5);
    }

    /// A non-default warehouse size changes the per-second price
    /// proportionally to its DBU/hour throughput — the field is not
    /// decorative.
    #[test]
    fn test_cost_section_scales_with_warehouse_size() {
        let section = crate::config::CostSection {
            warehouse_size: "Large".to_string(), // 40 DBU/hour
            ..crate::config::CostSection::default()
        };
        let config: CostConfig = section.into();
        let expected = 0.40 * 40.0 / 3600.0;
        assert!((config.compute_cost_per_second - expected).abs() < 1e-10);
    }

    #[test]
    fn test_view_for_a_model_with_no_consumers() {
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
        assert_eq!(result.recommended_strategy, "view");
    }

    /// No branch may recommend a strategy `rocky compile` refuses. `ephemeral`
    /// is E038 (#1996) and `incremental` is E037 (#1990).
    #[test]
    fn no_recommendation_names_a_refused_strategy() {
        for (duration, size, refs, runs) in [
            (0.3_f64, 0.01_f64, 0_usize, 10_usize),
            (1.5, 0.5, 1, 10),
            (5.0, 0.1, 3, 10),
            (120.0, 0.1, 1, 10),
            (0.5, 50.0, 1, 10),
            (1.0, 1.0, 2, 2),
        ] {
            let stats = ModelStats {
                model_name: "m".into(),
                current_strategy: "table".into(),
                avg_duration_seconds: duration,
                estimated_size_gb: size,
                downstream_references: refs,
                history_runs: runs,
                runs_per_month: 30.0,
            };
            let result = recommend_strategy(&stats, &default_config());
            assert!(
                !matches!(
                    result.recommended_strategy.as_str(),
                    "ephemeral" | "incremental"
                ),
                "{duration}s/{size}GB/{refs} consumers recommended a refused strategy: {}",
                result.recommended_strategy
            );
        }
    }
}
