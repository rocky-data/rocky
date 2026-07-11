//! `rocky backfill` — compose a review-gated, scoped recovery plan.
//!
//! A backfill re-runs *existing* recipes over a scoped window; it never
//! rewrites SQL to "fix" data. On a failure or gap trigger, this command:
//!
//! 1. resolves the **affected models** — either an explicit `--model` set
//!    (manual) or the previous run's failed models (`--from-last-run`, i.e.
//!    the contained/quarantined window),
//! 2. expands them to their **downstream lineage closure** (everything that
//!    consumed the stale/missing output and must be rebuilt),
//! 3. orders that closure topologically (dependency-first),
//! 4. scopes partitioned models to a `--from`/`--to` window when supplied,
//! 5. estimates the rebuild cost from historical observations (offline),
//! 6. persists the plan as a `PlanKind::Backfill` and emits it.
//!
//! The plan is **always** review-gated: `rocky apply <plan-id>` refuses to run
//! it until `rocky review <plan-id> --approve` has recorded a sign-off marker,
//! regardless of any configured policy. Backfills are where blast radius
//! hides, so the review gate is a hard rule here — not policy-tunable.
//!
//! Once approved, execution reuses the standard run path (classified retry +
//! failure containment); see [`crate::commands::run::execute_backfill_set`].

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use rocky_compiler::compile::CompileResult;
use rocky_core::config::{PolicyCapability, PolicyPrincipal};
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::models::StrategyConfig;
use rocky_core::state::StateStore;
use rocky_ir::dag::{DagNode, execution_layers};

use crate::commands::audit::{blast_radius_of, compile_project};
use crate::commands::review::record_plan_review_escalation;
use crate::output::{
    BackfillCostEstimate, BackfillModelCost, BackfillOutput, BackfillPartitionScope, RunPlan,
    print_json,
};
use crate::plan_store::{EmbeddedCapabilities, PlanKind, write_plan_governed};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Upper bound on recent runs scanned when pricing the backfill from history.
const COST_HISTORY_SCAN: usize = 100;

/// Execute `rocky backfill`.
///
/// Resolves the worktree root from the process cwd (mirroring
/// [`crate::commands::run_apply`]) and delegates to [`run_backfill_in`].
#[allow(clippy::too_many_arguments)]
pub async fn run_backfill(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    seed_models: &[String],
    from_last_run: bool,
    partition_from: Option<&str>,
    partition_to: Option<&str>,
    include_downstream: bool,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_backfill_in(
        &cwd,
        config_path,
        state_path,
        models_dir,
        seed_models,
        from_last_run,
        partition_from,
        partition_to,
        include_downstream,
        output_json,
    )
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_backfill_in(
    root: &Path,
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    seed_models: &[String],
    from_last_run: bool,
    partition_from: Option<&str>,
    partition_to: Option<&str>,
    include_downstream: bool,
    output_json: bool,
) -> Result<()> {
    if from_last_run && !seed_models.is_empty() {
        bail!(
            "pass either explicit --model names or --from-last-run, not both — \
             they are two ways to seed the same backfill"
        );
    }

    // Open the state store best-effort: it is required for --from-last-run and
    // for the historical cost estimate, but a manual backfill can still compose
    // (with an unavailable cost estimate) without one.
    let store = StateStore::open_read_only(state_path).ok();

    // 1. Compile first so lineage + strategy are available, and so the seed
    //    resolution can validate names against the real model graph.
    let compiled = compile_project(config_path, state_path, models_dir)
        .context("failed to compile the project for backfill composition")?;

    // 2. Resolve the affected seed models + the trigger label.
    let (seeds, trigger) = if from_last_run {
        let store = store.as_ref().with_context(|| {
            format!(
                "no state store at {} — --from-last-run needs run history",
                state_path.display()
            )
        })?;
        (seeds_from_last_run(store, &compiled)?, "last_run_failure")
    } else {
        if seed_models.is_empty() {
            bail!(
                "nothing to backfill — pass one or more --model <name> to rebuild, \
                 or --from-last-run to seed from the previous run's failed models"
            );
        }
        let mut seeds: Vec<String> = seed_models.to_vec();
        seeds.sort();
        seeds.dedup();
        // Validate explicit seeds against the model graph.
        let unknown: Vec<String> = seeds
            .iter()
            .filter(|s| compiled.semantic_graph.model_schema(s).is_none())
            .cloned()
            .collect();
        if !unknown.is_empty() {
            bail!(
                "unknown model(s) requested for backfill: {}. \
                 Run `rocky compile` to see the model names in this project.",
                unknown.join(", ")
            );
        }
        (seeds, "manual")
    };

    // 3. Expand to the downstream lineage closure.
    let closure = compute_closure(&seeds, &compiled, include_downstream);

    // 4. Order the closure topologically (dependency-first), using only the
    //    intra-closure dependencies — upstreams outside the closure are healthy
    //    and are not rebuilt.
    let dep_by_model: HashMap<&str, &[String]> = compiled
        .project
        .dag_nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();
    let layers = order_closure(&closure, &dep_by_model)?;
    let ordered: Vec<String> = layers.iter().flatten().cloned().collect();

    // 5. Partition scope: which closure models are partitioned, and the window.
    let partitioned: Vec<String> = ordered
        .iter()
        .filter(|name| {
            compiled
                .project
                .model(name)
                .is_some_and(|m| matches!(m.config.strategy, StrategyConfig::TimeInterval { .. }))
        })
        .cloned()
        .collect();
    let partition_scope = if partition_from.is_some() || partition_to.is_some() {
        Some(BackfillPartitionScope {
            from: partition_from.map(str::to_string),
            to: partition_to.map(str::to_string),
            models: partitioned,
        })
    } else {
        None
    };

    // 6. Cost estimate from historical observations (offline).
    let cost_estimate = estimate_cost(&ordered, store.as_ref(), config_path);
    // The read handle drops here so the escalation write below can open its
    // own handle on the same store.
    drop(store);

    // 7. Persist the plan (always review-gated).
    let run_plan = build_run_plan(models_dir, &ordered, &layers, partition_from, partition_to);
    // A backfill executes the same on-disk models it plans; bind their
    // compiled-IR fingerprint + routing identity so the apply-time TOCTOU gate
    // rejects a change.
    let config_identity = rocky_core::config::load_rocky_config(config_path)
        .ok()
        .as_ref()
        .map(crate::commands::apply::config_policy_identity);
    let identity = config_identity.clone().unwrap_or_default();
    // Fold the seeding-independent extras (surrogate-key sidecars #1, contract
    // presence/contents #3) into the bound fingerprint so the apply-time TOCTOU
    // gate rejects a post-plan swap of either, built from the SAME `models_dir`
    // the apply choke-point re-reads.
    let extras = crate::commands::apply::ExecutionExtras::build(
        &rocky_core::models::load_surrogate_keys_from_dir(models_dir).unwrap_or_default(),
        &compiled.project.models,
    );
    let capabilities = EmbeddedCapabilities {
        diff_available: true,
        changed: ordered
            .iter()
            .map(|m| (m.clone(), PolicyCapability::Backfill))
            .collect(),
        models_fingerprint: crate::commands::apply::execution_ir_fingerprint(
            &compiled.project.models,
            &identity,
            &extras,
        ),
        config_identity,
        fingerprint_version: crate::plan_store::CURRENT_FINGERPRINT_VERSION,
    };
    let plan_id = write_plan_governed(
        root,
        PlanKind::Backfill,
        &run_plan,
        PolicyPrincipal::Agent,
        capabilities,
    )
    .context("failed to persist the backfill plan")?;

    // A backfill's apply bails on the missing review marker before
    // `evaluate_apply_policy` could record anything, so record its escalation
    // at creation — this one plan-level row is what puts it in the review
    // queue (and in front of the governor's MCP approve path).
    record_plan_review_escalation(
        state_path,
        &plan_id,
        PolicyPrincipal::Agent,
        PolicyCapability::Backfill,
        &format!("backfill: {} model(s)", ordered.len()),
        "backfill plan awaits review — backfills are unconditionally review-gated (blast \
         radius hides in scoped rebuilds)",
    );

    let message = format!(
        "composed a backfill of {} model(s) ({} seed → {} closure); review required before apply",
        ordered.len(),
        seeds.len(),
        ordered.len(),
    );

    let output = BackfillOutput {
        version: VERSION.to_string(),
        command: "backfill".to_string(),
        trigger: trigger.to_string(),
        plan_id: plan_id.clone(),
        requires_review: true,
        seed_models: seeds,
        models: ordered,
        execution_layers: layers,
        partition_scope,
        cost_estimate,
        review_command: format!("rocky review {plan_id} --approve"),
        apply_command: format!("rocky apply {plan_id}"),
        message: message.clone(),
    };

    if output_json {
        print_json(&output)?;
    } else {
        print_table(&output);
    }
    Ok(())
}

/// Seed a backfill from the most recent run's failed models — the
/// contained/quarantined window a `PartialFailure` left behind.
///
/// A failure is only a usable seed when it was recorded against a real model
/// name. Failures the run loop could only attribute to a sentinel
/// (`<runtime>`, `<cycle>`, a pipeline name) — which the fail-fast local path
/// emits when it cannot name the model — are dropped, and a clear error asks
/// for an explicit `--model`. Enabling failure containment (`[resilience]
/// contain_failures`) records the model by name, which makes this trigger
/// resolve the contained window directly.
fn seeds_from_last_run(store: &StateStore, compiled: &CompileResult) -> Result<Vec<String>> {
    let runs = store
        .list_runs(1)
        .context("failed to read run history for --from-last-run")?;
    let Some(latest) = runs.into_iter().next() else {
        bail!("no runs recorded yet — nothing to backfill from --from-last-run");
    };
    let mut failed: Vec<String> = latest
        .models_executed
        .iter()
        .filter(|m| m.status == "failed")
        .map(|m| m.model_name.clone())
        .collect();
    failed.sort();
    failed.dedup();
    if failed.is_empty() {
        bail!(
            "the most recent run '{}' recorded no failed models — nothing to backfill. \
             Pass explicit --model names to force a rebuild.",
            latest.run_id
        );
    }

    // Keep only failures that map to a real model; drop run-loop sentinels.
    let seeds: Vec<String> = failed
        .iter()
        .filter(|m| compiled.semantic_graph.model_schema(m).is_some())
        .cloned()
        .collect();
    if seeds.is_empty() {
        bail!(
            "the most recent run '{}' failed, but its failure was not attributed to a \
             named model (recorded as: {}). Enable `[resilience] contain_failures` so \
             failures are named, or pass explicit --model names to backfill.",
            latest.run_id,
            failed.join(", ")
        );
    }
    Ok(seeds)
}

/// The set of models to rebuild: the seeds plus (optionally) their transitive
/// downstream lineage closure.
fn compute_closure(
    seeds: &[String],
    compiled: &CompileResult,
    include_downstream: bool,
) -> BTreeSet<String> {
    let mut closure: BTreeSet<String> = seeds.iter().cloned().collect();
    if include_downstream {
        for seed in seeds {
            if let Some((_, transitive)) = blast_radius_of(compiled, seed) {
                closure.extend(transitive);
            }
        }
    }
    closure
}

/// Topologically order a closure using only its intra-closure dependencies.
///
/// Upstreams outside the closure are healthy and not rebuilt, so they impose
/// no ordering constraint on the backfill. The result mirrors the order the
/// run path builds the closure in (the project's topological layers, filtered
/// to the set), so the emitted plan and the eventual execution agree.
fn order_closure(
    closure: &BTreeSet<String>,
    dep_by_model: &HashMap<&str, &[String]>,
) -> Result<Vec<Vec<String>>> {
    let nodes: Vec<DagNode> = closure
        .iter()
        .map(|m| {
            let depends_on = dep_by_model
                .get(m.as_str())
                .copied()
                .unwrap_or(&[])
                .iter()
                .filter(|d| closure.contains(*d))
                .cloned()
                .collect();
            DagNode {
                name: m.clone(),
                depends_on,
            }
        })
        .collect();
    execution_layers(&nodes)
        .map_err(|e| anyhow::anyhow!("failed to order the backfill closure: {e:?}"))
}

/// Build the persisted `RunPlan` payload for the backfill. The apply path
/// reads `models` (the closure), `execution_layers`, `models_dir`, and the
/// partition window off this.
fn build_run_plan(
    models_dir: &Path,
    ordered: &[String],
    layers: &[Vec<String>],
    partition_from: Option<&str>,
    partition_to: Option<&str>,
) -> RunPlan {
    RunPlan {
        filter: None,
        pipeline: None,
        model: None,
        branch: None,
        partition: None,
        partition_from: partition_from.map(str::to_string),
        partition_to: partition_to.map(str::to_string),
        latest: false,
        missing: false,
        lookback: None,
        parallel: 1,
        run_all: false,
        env: None,
        models_dir: Some(models_dir.to_string_lossy().into_owned()),
        resume: None,
        resume_latest: false,
        shadow: false,
        shadow_suffix: None,
        shadow_schema: None,
        dag: false,
        idempotency_key: None,
        governance_override: None,
        models: ordered.to_vec(),
        execution_layers: layers.to_vec(),
    }
}

/// Price the backfill from the most recent recorded execution of each closure
/// model, using the same warehouse cost formula as `rocky cost`. Offline and
/// approximate — always surfaced as an estimate.
fn estimate_cost(
    ordered: &[String],
    store: Option<&StateStore>,
    config_path: &Path,
) -> BackfillCostEstimate {
    let warehouse = resolve_warehouse(config_path);
    let latest_exec = latest_success_executions(store, ordered);

    let mut per_model = Vec::with_capacity(ordered.len());
    let mut total = 0.0;
    let mut any_priced = false;
    for model in ordered {
        let found = latest_exec.get(model);
        let cost = found.and_then(|(exec, _)| {
            warehouse.and_then(|(wh, dbu_per_hour, cost_per_dbu)| {
                compute_observed_cost_usd(
                    wh,
                    exec.bytes_scanned,
                    exec.duration_ms,
                    dbu_per_hour,
                    cost_per_dbu,
                )
            })
        });
        if let Some(c) = cost {
            total += c;
            any_priced = true;
        }
        per_model.push(BackfillModelCost {
            model_name: model.clone(),
            cost_usd: cost,
            duration_ms: found.map(|(exec, _)| exec.duration_ms),
            source_run_id: found.map(|(_, run_id)| run_id.clone()),
        });
    }

    BackfillCostEstimate {
        is_estimate: true,
        basis: if any_priced {
            "historical_observed".to_string()
        } else {
            "unavailable".to_string()
        },
        total_cost_usd: any_priced.then_some(total),
        per_model,
    }
}

/// The most recent successful execution of each requested model, scanning the
/// last [`COST_HISTORY_SCAN`] runs newest-first. Returns `(execution, run_id)`
/// per model that has one.
fn latest_success_executions(
    store: Option<&StateStore>,
    models: &[String],
) -> HashMap<String, (rocky_core::state::ModelExecution, String)> {
    let mut out: HashMap<String, (rocky_core::state::ModelExecution, String)> = HashMap::new();
    let Some(store) = store else {
        return out;
    };
    let Ok(runs) = store.list_runs(COST_HISTORY_SCAN) else {
        return out;
    };
    let wanted: BTreeSet<&str> = models.iter().map(String::as_str).collect();
    // `list_runs` is newest-first, so the first execution we see for a model is
    // the most recent one — keep it and never overwrite.
    for run in &runs {
        for exec in &run.models_executed {
            if exec.status == "success"
                && wanted.contains(exec.model_name.as_str())
                && !out.contains_key(&exec.model_name)
            {
                out.insert(exec.model_name.clone(), (exec.clone(), run.run_id.clone()));
            }
        }
    }
    out
}

/// Resolve `(warehouse, dbu_per_hour, cost_per_dbu)` from config, or `None`
/// when the config can't be read or names no billed warehouse.
fn resolve_warehouse(config_path: &Path) -> Option<(WarehouseType, f64, f64)> {
    let cfg = rocky_core::config::load_rocky_config(config_path).ok()?;
    let preferred = cfg
        .adapters
        .get("default")
        .map(|a| a.adapter_type.clone())
        .or_else(|| cfg.adapters.values().next().map(|a| a.adapter_type.clone()))?;
    let wh = WarehouseType::from_adapter_type(&preferred)?;
    let dbu_per_hour = warehouse_size_to_dbu_per_hour(&cfg.cost.warehouse_size);
    Some((wh, dbu_per_hour, cfg.cost.compute_cost_per_dbu))
}

/// Render the composed plan as a concise human-readable report.
fn print_table(out: &BackfillOutput) {
    println!("backfill plan {} (trigger: {})", out.plan_id, out.trigger);
    println!("  seeds:   {}", out.seed_models.join(", "));
    println!("  rebuild: {} model(s)", out.models.len());
    for (i, layer) in out.execution_layers.iter().enumerate() {
        println!("    layer {}: {}", i + 1, layer.join(", "));
    }
    if let Some(scope) = &out.partition_scope {
        let from = scope.from.as_deref().unwrap_or("-");
        let to = scope.to.as_deref().unwrap_or("-");
        println!(
            "  partitions: {}..{} over [{}]",
            from,
            to,
            scope.models.join(", ")
        );
    }
    match out.cost_estimate.total_cost_usd {
        Some(c) => println!(
            "  est. cost: ${c:.6} (estimate, {})",
            out.cost_estimate.basis
        ),
        None => println!("  est. cost: - ({})", out.cost_estimate.basis),
    }
    println!("  review required before apply:");
    println!("    {}", out.review_command);
    println!("    {}", out.apply_command);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deps(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(name, ds)| {
                (
                    name.to_string(),
                    ds.iter().map(ToString::to_string).collect(),
                )
            })
            .collect()
    }

    /// Ordering is dependency-first and drops out-of-closure upstreams.
    #[test]
    fn order_closure_is_dependency_first_within_the_set() {
        // Graph: a → b → c → d. Closure = {b, c, d} (a is a healthy upstream
        // outside the closure). b must come before c before d, and a imposes
        // no constraint because it is not in the set.
        let owned = deps(&[("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        let dep_by_model: HashMap<&str, &[String]> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        let closure: BTreeSet<String> = ["b", "c", "d"].iter().map(ToString::to_string).collect();

        let layers = order_closure(&closure, &dep_by_model).unwrap();
        let flat: Vec<String> = layers.iter().flatten().cloned().collect();
        assert_eq!(flat, vec!["b", "c", "d"]);
        // `a` is never scheduled — it is a healthy upstream, not part of the
        // rebuild set.
        assert!(!flat.iter().any(|m| m == "a"));
    }

    /// Two independent seeds land in the same first layer; their shared
    /// downstream lands strictly after both.
    #[test]
    fn order_closure_layers_independent_models_together() {
        let owned = deps(&[("x", &[]), ("y", &[]), ("z", &["x", "y"])]);
        let dep_by_model: HashMap<&str, &[String]> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        let closure: BTreeSet<String> = ["x", "y", "z"].iter().map(ToString::to_string).collect();

        let layers = order_closure(&closure, &dep_by_model).unwrap();
        assert_eq!(layers.len(), 2, "two topological layers");
        assert!(layers[0].contains(&"x".to_string()));
        assert!(layers[0].contains(&"y".to_string()));
        assert_eq!(layers[1], vec!["z".to_string()]);
    }

    /// An empty history yields an "unavailable" estimate with per-model `None`
    /// costs — never a fabricated figure.
    #[test]
    fn estimate_cost_unavailable_without_history() {
        let models = vec!["m1".to_string(), "m2".to_string()];
        // No store, and a config path that does not exist ⇒ no warehouse.
        let est = estimate_cost(&models, None, Path::new("/does/not/exist/rocky.toml"));
        assert!(est.is_estimate);
        assert_eq!(est.basis, "unavailable");
        assert!(est.total_cost_usd.is_none());
        assert_eq!(est.per_model.len(), 2);
        assert!(est.per_model.iter().all(|m| m.cost_usd.is_none()));
    }

    /// FIX: a backfill plan is unconditionally review-gated but its apply
    /// bails before `evaluate_apply_policy` runs — so plan creation itself
    /// must record the `require_review` ledger row that puts it in the
    /// decision-driven review queue.
    #[test]
    fn backfill_plan_records_review_escalation_and_enters_queue() {
        use rocky_core::config::PolicyEffect;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        for (name, sql) in [
            ("a", "SELECT id FROM source.raw.t"),
            ("b", "SELECT id FROM a"),
        ] {
            std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
            std::fs::write(
                models_dir.join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
        let state_path = root.join("state.redb");
        let config_path = root.join("rocky.toml"); // absent — fine, degrades

        run_backfill_in(
            root,
            &config_path,
            &state_path,
            &models_dir,
            &["a".to_string()],
            false,
            None,
            None,
            true,
            true,
        )
        .unwrap();

        // The plan file landed under <root>/.rocky/plans.
        let plans_dir = root.join(".rocky").join("plans");
        let plan_id = std::fs::read_dir(&plans_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_suffix(".json").map(str::to_string)
            })
            .next()
            .expect("a backfill plan file must exist");

        // Plan creation recorded exactly one plan-level escalation row.
        let decisions = {
            let store = StateStore::open_read_only(&state_path).unwrap();
            store.list_policy_decisions().unwrap()
        };
        assert_eq!(decisions.len(), 1, "one plan-level row, not one per model");
        let d = &decisions[0];
        assert_eq!(d.plan_id, plan_id);
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert_eq!(d.capability, PolicyCapability::Backfill);
        assert_eq!(d.principal, PolicyPrincipal::Agent);
        assert!(d.model.contains("2 model(s)"), "scope summary: {}", d.model);

        // And the decision-driven review queue now lists the backfill.
        let queue = crate::commands::review::compute_review_queue(
            root,
            &config_path,
            &state_path,
            &models_dir,
        )
        .unwrap();
        assert_eq!(queue.total, 1);
        assert_eq!(queue.pending[0].plan_id, plan_id);
        assert_eq!(queue.excluded_non_plan_rows, 0);
        assert_eq!(
            queue.pending[0].approve_command,
            format!("rocky review {plan_id} --approve")
        );
    }

    /// The persisted `RunPlan` carries the closure, layers, partition window,
    /// and models_dir the apply path replays.
    #[test]
    fn build_run_plan_carries_scope() {
        let ordered = vec!["a".to_string(), "b".to_string()];
        let layers = vec![vec!["a".to_string()], vec!["b".to_string()]];
        let plan = build_run_plan(
            Path::new("models"),
            &ordered,
            &layers,
            Some("2026-01"),
            Some("2026-03"),
        );
        assert_eq!(plan.models, ordered);
        assert_eq!(plan.execution_layers, layers);
        assert_eq!(plan.partition_from.as_deref(), Some("2026-01"));
        assert_eq!(plan.partition_to.as_deref(), Some("2026-03"));
        assert_eq!(plan.models_dir.as_deref(), Some("models"));
        assert_eq!(plan.parallel, 1);
        assert!(!plan.run_all);
    }
}
