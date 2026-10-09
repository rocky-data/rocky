//! `rocky plan` cost preview: rebuild scope, bytes and cost, before a run.
//!
//! ```text
//!   default                      --cost-estimate adapter
//!   ───────                      ───────────────────────
//!   compile (offline)            compile (offline)
//!   heuristic over the DAG       EXPLAIN each planned model (warehouse)
//!   no warehouse contact         heuristic for any model EXPLAIN misses
//!            │                              │
//!            └──────────► PlanCostPreview ◄─┘
//!                         + previous observed cost from the state store
//! ```
//!
//! The preview is report-only. It never changes which models the plan
//! rebuilds, never skips one, and never changes the exit code.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use rocky_core::cost::{
    CostEstimate, TableStats, WarehouseType, compute_observed_cost_usd, propagate_costs,
};
use rocky_core::state::{ModelExecution, StateStore, UnrecordedScope};
use rocky_ir::DagNode;

use crate::output::{CostEstimateSource, PlanCostPreview, PlanModelCost};

/// How `rocky plan` estimates cost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum CostEstimateMode {
    /// Offline heuristic over the compiled DAG. Never contacts the warehouse.
    #[default]
    Heuristic,
    /// Ask the warehouse (`EXPLAIN` or a dry run) for each planned model.
    Adapter,
}

/// Placeholder statistics for a model with no upstream model: the source
/// table it reads is unknown offline.
const HEURISTIC_SOURCE_STATS: TableStats = TableStats {
    row_count: 10_000,
    avg_row_bytes: 256,
};

/// The offline cost estimate for every model in `dag_nodes`.
///
/// Every model with no upstream model starts from
/// [`HEURISTIC_SOURCE_STATS`]; the rest are propagated through the DAG.
/// `rocky compile` uses the same estimate for `models[].cost_hint`.
/// Returns an empty map when the DAG cannot be sorted.
pub(crate) fn heuristic_cost_estimates(
    dag_nodes: &[DagNode],
    warehouse_type: WarehouseType,
) -> HashMap<String, CostEstimate> {
    let base_stats: HashMap<String, TableStats> = dag_nodes
        .iter()
        .filter(|node| node.depends_on.is_empty())
        .map(|node| (node.name.clone(), HEURISTIC_SOURCE_STATS))
        .collect();
    propagate_costs(dag_nodes, &base_stats, warehouse_type).unwrap_or_default()
}

/// One model's adapter estimate, already priced.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AdapterEstimate {
    pub rows: Option<u64>,
    pub bytes_scanned: Option<u64>,
    pub cost_usd: Option<f64>,
}

/// Inputs to [`build_cost_preview`], gathered by the caller.
pub(crate) struct CostPreviewInputs<'a> {
    /// The compiled DAG of the project.
    pub dag_nodes: &'a [DagNode],
    /// The models the plan rebuilds, in plan order.
    pub models: &'a [String],
    /// The billed warehouse type of the plan's target adapter, if any.
    pub warehouse_type: Option<WarehouseType>,
    /// Adapter estimates by model, when `--cost-estimate adapter` ran.
    pub adapter: Option<&'a BTreeMap<String, AdapterEstimate>>,
    /// Observed cost in USD of each model in the last successful production
    /// run that built it.
    pub previous: &'a BTreeMap<String, f64>,
    /// Notes the caller already has (for example an adapter failure).
    pub notes: Vec<String>,
}

/// Build the cost preview from already-gathered inputs. Pure.
pub(crate) fn build_cost_preview(inputs: CostPreviewInputs<'_>) -> PlanCostPreview {
    let mut notes = inputs.notes;
    // Cost needs a billed warehouse type; rows and bytes do not.
    let heuristic = heuristic_cost_estimates(
        inputs.dag_nodes,
        inputs.warehouse_type.unwrap_or(WarehouseType::Databricks),
    );
    let deps: HashMap<&str, &[String]> = inputs
        .dag_nodes
        .iter()
        .map(|node| (node.name.as_str(), node.depends_on.as_slice()))
        .collect();

    let mut rows_out = Vec::with_capacity(inputs.models.len());
    for model in inputs.models {
        let previous_cost_usd = inputs.previous.get(model).copied();
        if let Some(est) = inputs.adapter.and_then(|a| a.get(model)) {
            rows_out.push(PlanModelCost {
                model: model.clone(),
                source: CostEstimateSource::Adapter,
                estimated_rows: est.rows,
                estimated_bytes_scanned: est.bytes_scanned,
                estimated_cost_usd: est.cost_usd,
                confidence: None,
                previous_cost_usd,
            });
            continue;
        }
        let prefix = format!("model '{model}':");
        if inputs.adapter.is_some() && !notes.iter().any(|n| n.starts_with(&prefix)) {
            notes.push(format!(
                "model '{model}': the adapter returned no estimate; the heuristic is used"
            ));
        }
        let Some(own) = heuristic.get(model) else {
            notes.push(format!("model '{model}': no estimate is available"));
            rows_out.push(PlanModelCost {
                model: model.clone(),
                source: CostEstimateSource::Heuristic,
                estimated_rows: None,
                estimated_bytes_scanned: None,
                estimated_cost_usd: None,
                confidence: None,
                previous_cost_usd,
            });
            continue;
        };
        let upstream: Vec<&CostEstimate> = deps
            .get(model.as_str())
            .copied()
            .unwrap_or(&[])
            .iter()
            .filter_map(|dep| heuristic.get(dep))
            .collect();
        // The propagated cost of a model includes its upstreams' cost. A
        // plan rebuilds each model once, so count only this model's share.
        let (bytes_scanned, own_cost) = if upstream.is_empty() {
            (own.estimated_bytes, own.estimated_compute_cost_usd)
        } else {
            let read: u64 = upstream.iter().map(|e| e.estimated_bytes).sum();
            let upstream_cost: f64 = upstream.iter().map(|e| e.estimated_compute_cost_usd).sum();
            (
                read,
                (own.estimated_compute_cost_usd - upstream_cost).max(0.0),
            )
        };
        let estimated_cost_usd = match inputs.warehouse_type {
            // DuckDB runs locally and bills nothing.
            Some(WarehouseType::DuckDb) => Some(0.0),
            Some(
                WarehouseType::Databricks | WarehouseType::Snowflake | WarehouseType::BigQuery,
            ) => Some(own_cost),
            None => None,
        };
        rows_out.push(PlanModelCost {
            model: model.clone(),
            source: CostEstimateSource::Heuristic,
            estimated_rows: Some(own.estimated_rows),
            estimated_bytes_scanned: Some(bytes_scanned),
            estimated_cost_usd,
            // The source statistics are placeholders, so a heuristic estimate is
            // never better than low confidence, whatever the propagation says.
            confidence: Some("low".to_string()),
            previous_cost_usd,
        });
    }

    let source = match (
        rows_out
            .iter()
            .all(|m| m.source == CostEstimateSource::Adapter),
        rows_out
            .iter()
            .any(|m| m.source == CostEstimateSource::Adapter),
    ) {
        (true, _) if !rows_out.is_empty() => CostEstimateSource::Adapter,
        (_, true) => CostEstimateSource::Mixed,
        _ => CostEstimateSource::Heuristic,
    };
    let estimated_bytes_scanned = sum_present(rows_out.iter().map(|m| m.estimated_bytes_scanned));
    let estimated_cost_usd = sum_present(rows_out.iter().map(|m| m.estimated_cost_usd));
    let previous_cost_usd = sum_present(rows_out.iter().map(|m| m.previous_cost_usd));
    let every_model_comparable = !rows_out.is_empty()
        && rows_out.iter().all(|m| {
            m.source == CostEstimateSource::Adapter
                && m.estimated_cost_usd.is_some()
                && m.previous_cost_usd.is_some()
        });
    let cost_delta_usd = match (
        every_model_comparable,
        estimated_cost_usd,
        previous_cost_usd,
    ) {
        (true, Some(estimated), Some(previous)) => Some(estimated - previous),
        _ => None,
    };

    PlanCostPreview {
        is_estimate: true,
        source,
        models_to_rebuild: rows_out.len(),
        estimated_bytes_scanned,
        estimated_cost_usd,
        previous_cost_usd,
        cost_delta_usd,
        models: rows_out,
        notes,
    }
}

/// Sum the present values, or `None` when none is present.
fn sum_present<T: std::iter::Sum<T> + Copy>(values: impl Iterator<Item = Option<T>>) -> Option<T> {
    let present: Vec<T> = values.flatten().collect();
    if present.is_empty() {
        None
    } else {
        Some(present.into_iter().sum())
    }
}

/// Where [`compute_plan_cost_preview`] reads its inputs.
pub(crate) struct PlanCostContext<'a> {
    pub config_path: &'a Path,
    pub models_dir: &'a Path,
    pub state_path: &'a Path,
    /// The plan's pipeline, used to resolve the adapter for
    /// `--cost-estimate adapter`.
    pub pipeline_name: &'a str,
    /// The `type` of the plan's target adapter (`"duckdb"`, ...).
    pub adapter_type: &'a str,
    /// The models the plan rebuilds.
    pub models: &'a [String],
    pub mode: CostEstimateMode,
}

/// Gather the inputs and build the cost preview for a run plan.
///
/// Compiles the models offline for the DAG. Contacts the warehouse only when
/// `mode` is [`CostEstimateMode::Adapter`]; an adapter failure degrades to
/// the heuristic with a note, because the preview never fails a plan.
pub(crate) async fn compute_plan_cost_preview(ctx: PlanCostContext<'_>) -> PlanCostPreview {
    let compile_cfg = rocky_compiler::compile::CompilerConfig {
        models_dir: ctx.models_dir.to_path_buf(),
        ..Default::default()
    };
    let dag_nodes = match rocky_compiler::compile::compile(&compile_cfg) {
        Ok(result) => result.project.dag_nodes,
        Err(e) => {
            tracing::debug!(error = %e, "plan cost preview: compile failed");
            Vec::new()
        }
    };
    let mut notes = Vec::new();
    let adapter = match ctx.mode {
        CostEstimateMode::Heuristic => None,
        CostEstimateMode::Adapter => {
            let model_filter = match ctx.models {
                [only] => Some(only.as_str()),
                _ => None,
            };
            match super::estimate::compute_estimate(
                ctx.config_path,
                ctx.models_dir,
                Some(ctx.pipeline_name),
                model_filter,
            )
            .await
            {
                Ok(report) => {
                    for (model, reason) in report.skipped.iter().chain(&report.explain_failed) {
                        notes.push(format!(
                            "model '{model}': adapter estimate failed: {reason}"
                        ));
                    }
                    let (estimates, empty): (Vec<_>, Vec<_>) =
                        report.output.estimates.into_iter().partition(|e| {
                            e.estimated_rows.is_some()
                                || e.estimated_bytes_scanned.is_some()
                                || e.estimated_cost_usd.is_some()
                        });
                    // An adapter whose EXPLAIN reports no figures (DuckDB)
                    // has nothing to offer over the heuristic.
                    for e in empty {
                        notes.push(format!(
                            "model '{}': the adapter's EXPLAIN reports no row, byte or cost \
                             figures",
                            e.model_name
                        ));
                    }
                    Some(
                        estimates
                            .into_iter()
                            .map(|e| {
                                (
                                    e.model_name,
                                    AdapterEstimate {
                                        rows: e.estimated_rows,
                                        bytes_scanned: e.estimated_bytes_scanned,
                                        cost_usd: e.estimated_cost_usd,
                                    },
                                )
                            })
                            .collect::<BTreeMap<_, _>>(),
                    )
                }
                Err(e) => {
                    notes.push(format!(
                        "the adapter estimate failed ({e:#}); the heuristic is used"
                    ));
                    None
                }
            }
        }
    };
    let previous = previous_observed_costs(ctx.state_path, ctx.config_path, ctx.models);
    build_cost_preview(CostPreviewInputs {
        dag_nodes: &dag_nodes,
        models: ctx.models,
        warehouse_type: WarehouseType::from_adapter_type(ctx.adapter_type),
        adapter: adapter.as_ref(),
        previous: &previous,
        notes,
    })
}

/// Observed cost of each of `models` in the newest successful production
/// execution that built it, read from the state store at `state_path`.
///
/// The store is opened read-only. A missing store, a store this binary
/// cannot read, or a project with no billed warehouse yields an empty map:
/// the previous cost is context, never a reason to fail a plan.
pub(crate) fn previous_observed_costs(
    state_path: &Path,
    config_path: &Path,
    models: &[String],
) -> BTreeMap<String, f64> {
    let Ok(Some((_, warehouse_type, dbu_per_hour, cost_per_dbu))) =
        super::cost::adapter_pricing(config_path)
    else {
        return BTreeMap::new();
    };
    if !state_path.exists() {
        return BTreeMap::new();
    }
    let Ok(store) = StateStore::open_read_only(state_path) else {
        return BTreeMap::new();
    };
    let Ok(runs) = store.list_runs_matching(usize::MAX, |run| {
        run.counts_as_production(UnrecordedScope::Exclude)
    }) else {
        return BTreeMap::new();
    };
    let mut found = BTreeMap::new();
    for model in models {
        let newest = runs
            .iter()
            .flat_map(|run| run.models_executed.iter())
            .find(|exec| exec.status == "success" && execution_is_model(exec, model));
        if let Some(exec) = newest
            && let Some(cost) = compute_observed_cost_usd(
                warehouse_type,
                exec.bytes_scanned,
                exec.duration_ms,
                dbu_per_hour,
                cost_per_dbu,
            )
        {
            found.insert(model.clone(), cost);
        }
    }
    found
}

/// Whether `exec` is an execution of `model`. A record with a recorded
/// target names the model; an older record only names its table, which is
/// the model name unless the model sets a different table.
fn execution_is_model(exec: &ModelExecution, model: &str) -> bool {
    match &exec.output_target {
        Some(target) => target.model == model,
        None => exec.model_name == model,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, deps: &[&str]) -> DagNode {
        DagNode {
            name: name.to_string(),
            depends_on: deps.iter().map(|d| (*d).to_string()).collect(),
        }
    }

    fn dag() -> Vec<DagNode> {
        vec![
            node("orders", &[]),
            node("customers", &[]),
            node("joined", &["orders", "customers"]),
        ]
    }

    fn names(models: &[&str]) -> Vec<String> {
        models.iter().map(|m| (*m).to_string()).collect()
    }

    #[test]
    fn heuristic_preview_counts_scope_and_marks_estimates() {
        let dag = dag();
        let models = names(&["orders", "customers", "joined"]);
        let preview = build_cost_preview(CostPreviewInputs {
            dag_nodes: &dag,
            models: &models,
            warehouse_type: Some(WarehouseType::Databricks),
            adapter: None,
            previous: &BTreeMap::new(),
            notes: Vec::new(),
        });
        assert!(preview.is_estimate);
        assert_eq!(preview.source, CostEstimateSource::Heuristic);
        assert_eq!(preview.models_to_rebuild, 3);
        let joined = &preview.models[2];
        // `joined` reads both upstreams' output: 2 x 10,000 rows x 256 bytes.
        assert_eq!(joined.estimated_bytes_scanned, Some(2 * 10_000 * 256));
        assert_eq!(
            preview.estimated_bytes_scanned,
            Some(4 * 10_000 * 256),
            "two source scans plus the join's read"
        );
        // Each model counts only its own share, so the total is not the
        // cumulative cost counted twice.
        let total = preview.estimated_cost_usd.unwrap();
        let cumulative_joined = heuristic_cost_estimates(&dag, WarehouseType::Databricks)["joined"]
            .estimated_compute_cost_usd;
        assert!(
            (total - cumulative_joined).abs() < 1e-12,
            "{total} vs {cumulative_joined}"
        );
        assert_eq!(
            preview.cost_delta_usd, None,
            "a heuristic is never compared"
        );
    }

    #[test]
    fn duckdb_bills_nothing_and_unknown_warehouse_has_no_cost() {
        let dag = dag();
        let models = names(&["joined"]);
        let duck = build_cost_preview(CostPreviewInputs {
            dag_nodes: &dag,
            models: &models,
            warehouse_type: Some(WarehouseType::DuckDb),
            adapter: None,
            previous: &BTreeMap::new(),
            notes: Vec::new(),
        });
        assert_eq!(duck.models_to_rebuild, 1, "a --model plan rebuilds one");
        assert_eq!(duck.estimated_cost_usd, Some(0.0));
        let unknown = build_cost_preview(CostPreviewInputs {
            dag_nodes: &dag,
            models: &models,
            warehouse_type: None,
            adapter: None,
            previous: &BTreeMap::new(),
            notes: Vec::new(),
        });
        assert_eq!(unknown.estimated_cost_usd, None);
        assert!(unknown.estimated_bytes_scanned.is_some());
    }

    #[test]
    fn adapter_estimates_win_and_delta_needs_every_model() {
        let dag = dag();
        let models = names(&["orders", "joined"]);
        let adapter = BTreeMap::from([(
            "orders".to_string(),
            AdapterEstimate {
                rows: Some(5),
                bytes_scanned: Some(1_000),
                cost_usd: Some(2.0),
            },
        )]);
        let previous = BTreeMap::from([("orders".to_string(), 1.5), ("joined".to_string(), 1.0)]);
        let mixed = build_cost_preview(CostPreviewInputs {
            dag_nodes: &dag,
            models: &models,
            warehouse_type: Some(WarehouseType::BigQuery),
            adapter: Some(&adapter),
            previous: &previous,
            notes: Vec::new(),
        });
        assert_eq!(mixed.source, CostEstimateSource::Mixed);
        assert_eq!(mixed.models[0].source, CostEstimateSource::Adapter);
        assert_eq!(mixed.models[0].estimated_bytes_scanned, Some(1_000));
        assert_eq!(mixed.models[1].source, CostEstimateSource::Heuristic);
        assert_eq!(mixed.cost_delta_usd, None, "joined has no adapter estimate");
        assert!(mixed.notes.iter().any(|n| n.contains("'joined'")));

        let only_orders = names(&["orders"]);
        let full = build_cost_preview(CostPreviewInputs {
            dag_nodes: &dag,
            models: &only_orders,
            warehouse_type: Some(WarehouseType::BigQuery),
            adapter: Some(&adapter),
            previous: &previous,
            notes: Vec::new(),
        });
        assert_eq!(full.source, CostEstimateSource::Adapter);
        assert_eq!(full.previous_cost_usd, Some(1.5));
        assert_eq!(full.cost_delta_usd, Some(0.5));
    }

    #[test]
    fn empty_plan_is_heuristic_with_zero_scope() {
        let preview = build_cost_preview(CostPreviewInputs {
            dag_nodes: &[],
            models: &[],
            warehouse_type: Some(WarehouseType::Databricks),
            adapter: Some(&BTreeMap::new()),
            previous: &BTreeMap::new(),
            notes: Vec::new(),
        });
        assert_eq!(preview.models_to_rebuild, 0);
        assert_eq!(preview.source, CostEstimateSource::Heuristic);
        assert_eq!(preview.estimated_cost_usd, None);
    }
}
