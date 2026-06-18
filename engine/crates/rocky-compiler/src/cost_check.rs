//! Pre-execution budget enforcement.
//!
//! [`check_cost_ceilings`] compares DAG-propagated cost estimates against each
//! model's declared `[budget]` sidecar block and returns an [`E027`] diagnostic
//! for every model whose projected cost exceeds its ceiling.
//!
//! The function is pure and synchronous — it takes only what the compiler
//! already has in memory (loaded models + propagated estimates) and produces
//! diagnostics with no I/O. Callers in `rocky-cli` wire in real catalog stats
//! when available; when running offline the estimate uses hardcoded stub
//! statistics and the function still fires when those stubs exceed a tight
//! ceiling.
//!
//! [`E027`]: crate::diagnostic::E027

use std::collections::HashMap;

use rocky_core::config::BudgetBreachAction;

use crate::diagnostic::Diagnostic;

/// Check every model's declared cost ceiling against its propagated estimate.
///
/// For each model that has a `[budget]` block with `max_usd` or
/// `max_bytes_scanned`, the function looks up the model's entry in
/// `estimates` and emits an [`E027`] diagnostic when either ceiling is
/// exceeded.  Models without a budget sidecar, or models whose names are
/// absent from `estimates`, are silently skipped.
///
/// The returned `Vec` contains one diagnostic per ceiling breach.  A model
/// with both `max_usd` and `max_bytes_scanned` can produce two diagnostics
/// when both are exceeded.
///
/// # Arguments
///
/// * `models`    — slice of loaded [`rocky_core::models::Model`] values.
/// * `estimates` — map from model name to [`rocky_core::cost::CostEstimate`],
///   typically produced by [`rocky_core::cost::propagate_costs`].
#[must_use]
pub fn check_cost_ceilings(
    models: &[rocky_core::models::Model],
    estimates: &HashMap<String, rocky_core::cost::CostEstimate>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for model in models {
        let budget = match model.config.budget.as_ref() {
            Some(b) => b,
            None => continue,
        };
        let estimate = match estimates.get(&model.config.name) {
            Some(e) => e,
            None => continue,
        };

        if let Some(ceiling_usd) = budget.max_usd {
            let projected = estimate.estimated_compute_cost_usd;
            if projected > ceiling_usd {
                diagnostics.push(Diagnostic::budget_exceeded(
                    &model.config.name,
                    projected,
                    ceiling_usd,
                ));
            }
        }

        if let Some(ceiling_bytes) = budget.max_bytes_scanned {
            let projected = estimate.estimated_bytes;
            if projected > ceiling_bytes {
                diagnostics.push(Diagnostic::budget_exceeded_bytes(
                    &model.config.name,
                    projected,
                    ceiling_bytes,
                ));
            }
        }
    }

    diagnostics
}

/// Plan-time budget ceiling check that honours each model's `on_breach` policy.
///
/// Identical to [`check_cost_ceilings`] in breach detection, but emits
/// **warning**-severity diagnostics when the model's `on_breach` field is
/// `"warn"` (the default) and **error**-severity diagnostics when it is
/// `"error"`.  This respects the user's declared intent: at plan time a
/// `warn` model merely surfaces an advisory; an `error` model will set
/// `PlanOutput::has_budget_errors = true`, signalling to CI/orchestration
/// that this plan should not proceed.
///
/// At compile time (offline, no real stats) [`check_cost_ceilings`] is used
/// unchanged — stub estimates deliberately err on the side of always-error so
/// tests remain deterministic.  Plan time uses this variant because the
/// estimates come from real catalog data and the user's policy should govern
/// whether that constitutes a blocking signal.
#[must_use]
pub fn check_cost_ceilings_plan(
    models: &[rocky_core::models::Model],
    estimates: &HashMap<String, rocky_core::cost::CostEstimate>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for model in models {
        let budget = match model.config.budget.as_ref() {
            Some(b) => b,
            None => continue,
        };
        let estimate = match estimates.get(&model.config.name) {
            Some(e) => e,
            None => continue,
        };

        // Resolve breach action: per-model `on_breach` when set, otherwise default (Warn).
        let breach_action = budget.on_breach.unwrap_or(BudgetBreachAction::Warn);

        if let Some(ceiling_usd) = budget.max_usd {
            let projected = estimate.estimated_compute_cost_usd;
            if projected > ceiling_usd {
                let d = match breach_action {
                    BudgetBreachAction::Error => {
                        Diagnostic::budget_exceeded(&model.config.name, projected, ceiling_usd)
                    }
                    BudgetBreachAction::Warn => {
                        Diagnostic::budget_exceeded_warn(&model.config.name, projected, ceiling_usd)
                    }
                };
                diagnostics.push(d);
            }
        }

        if let Some(ceiling_bytes) = budget.max_bytes_scanned {
            let projected = estimate.estimated_bytes;
            if projected > ceiling_bytes {
                let d = match breach_action {
                    BudgetBreachAction::Error => Diagnostic::budget_exceeded_bytes(
                        &model.config.name,
                        projected,
                        ceiling_bytes,
                    ),
                    BudgetBreachAction::Warn => Diagnostic::budget_exceeded_bytes_warn(
                        &model.config.name,
                        projected,
                        ceiling_bytes,
                    ),
                };
                diagnostics.push(d);
            }
        }
    }

    diagnostics
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::E027;
    use rocky_core::config::ModelBudgetConfig;
    use rocky_core::cost::{Confidence, CostEstimate};
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, max_usd: Option<f64>, max_bytes: Option<u64>) -> Model {
        let budget = if max_usd.is_some() || max_bytes.is_some() {
            Some(ModelBudgetConfig {
                max_usd,
                max_bytes_scanned: max_bytes,
                ..Default::default()
            })
        } else {
            None
        };

        Model {
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "s".to_string(),
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
                tags: Default::default(),
                retention: None,
                budget,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: "SELECT 1 AS x".to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    fn make_estimate(rows: u64, bytes: u64, cost_usd: f64) -> CostEstimate {
        CostEstimate {
            estimated_rows: rows,
            estimated_bytes: bytes,
            estimated_compute_cost_usd: cost_usd,
            confidence: Confidence::Low,
        }
    }

    #[test]
    fn no_budget_no_diagnostics() {
        let models = vec![make_model("m1", None, None)];
        let mut estimates = HashMap::new();
        estimates.insert("m1".to_string(), make_estimate(1_000, 256_000, 100.0));
        let diags = check_cost_ceilings(&models, &estimates);
        assert!(diags.is_empty());
    }

    #[test]
    fn usd_ceiling_exceeded_emits_e027() {
        let models = vec![make_model("m1", Some(0.01), None)];
        let mut estimates = HashMap::new();
        estimates.insert(
            "m1".to_string(),
            make_estimate(100_000_000, 25_000_000_000, 1.50),
        );
        let diags = check_cost_ceilings(&models, &estimates);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code.as_ref(), E027);
        assert_eq!(diags[0].model, "m1");
        assert!(
            diags[0].message.contains("1.5000"),
            "got: {}",
            diags[0].message
        );
        assert!(
            diags[0].message.contains("0.0100"),
            "got: {}",
            diags[0].message
        );
    }

    #[test]
    fn usd_ceiling_not_exceeded_no_diagnostic() {
        let models = vec![make_model("m1", Some(10.0), None)];
        let mut estimates = HashMap::new();
        estimates.insert("m1".to_string(), make_estimate(100, 10_000, 0.001));
        let diags = check_cost_ceilings(&models, &estimates);
        assert!(diags.is_empty());
    }

    #[test]
    fn bytes_ceiling_exceeded_emits_e027() {
        let models = vec![make_model("m1", None, Some(1_000_000))];
        let mut estimates = HashMap::new();
        estimates.insert("m1".to_string(), make_estimate(50_000, 5_000_000, 0.0));
        let diags = check_cost_ceilings(&models, &estimates);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code.as_ref(), E027);
        assert!(
            diags[0].message.contains("5000000"),
            "got: {}",
            diags[0].message
        );
        assert!(
            diags[0].message.contains("1000000"),
            "got: {}",
            diags[0].message
        );
    }

    #[test]
    fn both_ceilings_exceeded_emits_two_diagnostics() {
        let models = vec![make_model("m1", Some(0.001), Some(100_000))];
        let mut estimates = HashMap::new();
        estimates.insert("m1".to_string(), make_estimate(1_000_000, 5_000_000, 0.5));
        let diags = check_cost_ceilings(&models, &estimates);
        assert_eq!(diags.len(), 2);
        assert!(diags.iter().all(|d| d.code.as_ref() == E027));
    }

    #[test]
    fn model_missing_from_estimates_skipped() {
        let models = vec![make_model("m1", Some(0.01), None)];
        let estimates = HashMap::new(); // empty
        let diags = check_cost_ceilings(&models, &estimates);
        assert!(diags.is_empty());
    }

    #[test]
    fn usd_exactly_at_ceiling_no_diagnostic() {
        // Equality (projected == ceiling) must NOT emit a diagnostic.
        let models = vec![make_model("m1", Some(1.0), None)];
        let mut estimates = HashMap::new();
        estimates.insert("m1".to_string(), make_estimate(1_000, 10_000, 1.0));
        let diags = check_cost_ceilings(&models, &estimates);
        assert!(diags.is_empty(), "exact equality must not trip the ceiling");
    }
}
