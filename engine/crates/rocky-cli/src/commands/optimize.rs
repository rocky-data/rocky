//! `rocky optimize` — cost model analysis and materialization recommendations.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::cost::downstream_counts;
use rocky_core::models::StrategyConfig;
use rocky_core::optimize::{
    CostConfig, MaterializationCost, ModelStats, UNKNOWN_STRATEGY, recommend_strategy,
};
use rocky_core::state::StateStore;
use rocky_ir::dag::DagNode;

use crate::output::{OptimizeOutput, OptimizeRecommendation, print_json};

/// Build the optimize output from run history + the on-disk DAG. Pure
/// compute — no printing — so other surfaces (the MCP `optimize` tool) can
/// reuse it. Returns [`OptimizeOutput::empty`] when there is no run history.
pub fn optimize_output(
    state_path: &Path,
    config_path: &Path,
    models_dir: Option<&Path>,
    model_filter: Option<&str>,
) -> Result<OptimizeOutput> {
    let store = StateStore::open_read_only(state_path)?;

    // `[cost]` pricing: read it from the loaded project config when one
    // exists, and fall back to `CostConfig::default()` only when the
    // project has no `rocky.toml` to read (#2056). Credential-tolerant and
    // offline, the same loader `rocky cost`'s `adapter_pricing` uses — this
    // command opens no warehouse connection either. A `rocky.toml` that is
    // there but does not load is a hard error: a wrong price is worse than
    // no price.
    let config = match rocky_core::config::load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?
    {
        Some(cfg) => CostConfig::from(cfg.cost),
        None => CostConfig::default(),
    };

    // Get all runs to extract model names and compute stats
    let runs = store.list_runs(100)?;

    if runs.is_empty() {
        return Ok(OptimizeOutput::empty("no run history available"));
    }

    // Build the DAG + each model's configured strategy from the models on
    // disk (if models_dir is available). This lets us compute
    // downstream_references accurately instead of defaulting to 0, and
    // report each model's real current strategy instead of assuming
    // "table" (#2056).
    let (dag_nodes, strategies) = load_models_from_disk(models_dir);
    let downstream = downstream_counts(&dag_nodes);

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
            current_strategy: strategies
                .get(model_name.as_str())
                .cloned()
                .unwrap_or_else(|| UNKNOWN_STRATEGY.to_string()),
            avg_duration_seconds,
            estimated_size_gb,
            downstream_references: downstream.get(model_name.as_str()).copied().unwrap_or(0),
            history_runs: history.len(),
            runs_per_month,
        };

        recommendations.push(recommend_strategy(&stats, &config));
    }

    let typed_recs: Vec<OptimizeRecommendation> = recommendations
        .iter()
        .map(|r| OptimizeRecommendation {
            model_name: r.model_name.clone(),
            current_strategy: r.current_strategy.clone(),
            recommended_strategy: r.recommended_strategy.clone(),
            estimated_monthly_savings: r.estimated_monthly_savings,
            reasoning: r.reasoning.clone(),
            compute_cost_per_run: r.compute_cost_per_run,
            storage_cost_per_month: r.storage_cost_per_month,
            downstream_references: r.downstream_references as u64,
        })
        .collect();
    Ok(OptimizeOutput::new(typed_recs))
}

/// Execute `rocky optimize`.
pub fn run_optimize(
    state_path: &Path,
    config_path: &Path,
    models_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let output = optimize_output(state_path, config_path, models_dir, model_filter)?;

    if output_json {
        print_json(&output)?;
        return Ok(());
    }

    if output.recommendations.is_empty() && output.message.is_some() {
        println!("No run history available. Run `rocky run` first to collect execution data.");
        return Ok(());
    }

    println!(
        "{:<30} {:<12} {:<14} {:<12} {:<10}",
        "MODEL", "CURRENT", "RECOMMENDED", "SAVINGS/MO", "REASONING"
    );
    println!("{}", "-".repeat(90));

    for rec in &output.recommendations {
        println!(
            "{:<30} {:<12} {:<14} ${:<11.4} {}",
            truncate(&rec.model_name, 29),
            // Before #2056, `current_strategy`/`recommended_strategy` were
            // always "table" or "view" — both fit `{:<12}`/`{:<14}` with
            // room to spare. Now they can be "materialized_view" or
            // "content_addressed" (17 chars), so truncate like the other
            // variable-width columns rather than let the table go ragged.
            truncate(&rec.current_strategy, 11),
            truncate(&rec.recommended_strategy, 13),
            rec.estimated_monthly_savings,
            truncate(&rec.reasoning, 40),
        );
    }

    let total_savings: f64 = output
        .recommendations
        .iter()
        .map(|r| r.estimated_monthly_savings)
        .sum();
    println!();
    println!("Total estimated monthly savings: ${total_savings:.2}");
    println!("Models analyzed: {}", output.recommendations.len());

    Ok(())
}

/// Load model definitions from disk, building DAG nodes (for downstream
/// reference counts) and each model's configured strategy, mapped to the
/// label [`recommend_strategy`](rocky_core::optimize::recommend_strategy)
/// compares `current_strategy` against.
///
/// Returns empty collections if `models_dir` is `None` or if loading fails.
/// A model absent from the returned map (not on disk, e.g. seen only in run
/// history) is left for the caller to report as [`UNKNOWN_STRATEGY`] —
/// failure to read models is non-fatal — the optimize command degrades
/// gracefully to `downstream_references: 0` / an unknown strategy for all
/// models.
fn load_models_from_disk(models_dir: Option<&Path>) -> (Vec<DagNode>, HashMap<String, String>) {
    let Some(dir) = models_dir else {
        return (vec![], HashMap::new());
    };

    // Top-level + immediate subdirectories, including `.rocky` DSL files.
    // Partial: this drives advisory downstream-reference counts, so one broken
    // draft model must not silently collapse every healthy count to zero.
    // No project `[freshness]`: this path loads no `RockyConfig`, and the
    // models are used only to count downstream references and read their
    // configured strategy.
    let (all_models, load_errors) = crate::models_loader::load_project_models_partial(dir, None);
    // Tolerant on purpose — recommendations over the models that DID load are
    // still useful — but never silent: a model missing from this set gets no
    // recommendation at all, and that absence must be visible (#1262).
    for e in &load_errors {
        tracing::warn!(
            error = %format!("{e:#}"),
            "some models could not be loaded; they are absent from optimize recommendations"
        );
    }

    let dag_nodes = all_models
        .iter()
        .map(|m| DagNode {
            name: m.config.name.clone(),
            depends_on: m.config.depends_on.clone(),
        })
        .collect();

    let strategies = all_models
        .iter()
        .map(|m| {
            (
                m.config.name.clone(),
                current_strategy_label(&m.config.strategy),
            )
        })
        .collect();

    (dag_nodes, strategies)
}

/// Maps a model's configured [`StrategyConfig`] to the label
/// `recommend_strategy` compares `current_strategy` against: `"view"` for a
/// strategy with no physical storage, `"table"` for `full_refresh` (the
/// literal rebuild-the-whole-table baseline `recommend_strategy`'s two-value
/// vocabulary means by "table"), and the strategy's own name for every other
/// stored strategy (`merge`, `incremental`, `time_interval`, ...).
///
/// Those other strategies deliberately do NOT collapse into `"table"`:
/// `recommend_strategy` treats a `recommended_strategy` equal to
/// `current_strategy` as "already optimal" and reports zero savings. A merge
/// or time-interval model recommended `"table"` is a genuine, actionable
/// suggestion — collapsing its label to `"table"` would make it look
/// identical to the recommendation and silently zero out real savings
/// (#2056).
fn current_strategy_label(strategy: &StrategyConfig) -> String {
    match strategy {
        StrategyConfig::View => "view".to_string(),
        StrategyConfig::FullRefresh => "table".to_string(),
        StrategyConfig::Incremental { .. } => "incremental".to_string(),
        StrategyConfig::Merge { .. } => "merge".to_string(),
        StrategyConfig::TimeInterval { .. } => "time_interval".to_string(),
        StrategyConfig::Ephemeral => "ephemeral".to_string(),
        StrategyConfig::DeleteInsert { .. } => "delete_insert".to_string(),
        StrategyConfig::Microbatch { .. } => "microbatch".to_string(),
        StrategyConfig::ContentAddressed { .. } => "content_addressed".to_string(),
        StrategyConfig::MaterializedView => "materialized_view".to_string(),
        StrategyConfig::DynamicTable { .. } => "dynamic_table".to_string(),
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource};

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, contents).expect("write");
    }

    /// A sidecar `.sql` + `.toml` model pair with the given strategy TOML
    /// body (e.g. `"type = \"view\"\n"`).
    fn write_model(dir: &Path, name: &str, strategy_toml: &str) {
        write(&dir.join(format!("{name}.sql")), "SELECT 1 AS id\n");
        write(
            &dir.join(format!("{name}.toml")),
            &format!(
                "name = \"{name}\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"{name}\"\n[strategy]\n{strategy_toml}"
            ),
        );
    }

    fn make_exec(model_name: &str, duration_ms: u64, bytes_written: Option<u64>) -> ModelExecution {
        ModelExecution {
            model_name: model_name.to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration_ms,
            rows_affected: Some(10),
            status: "success".to_string(),
            sql_hash: "abc".to_string(),
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written,
            tenant: None,
            recipe_hash: None,
            input_hash: None,
            input_proof_class: None,
            env_hash: None,
            hash_scheme: None,
            output_column_hashes: None,
            attempts: Vec::new(),
        }
    }

    fn run_with(run_id: &str, execs: Vec<ModelExecution>) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now() + chrono::Duration::seconds(1),
            status: RunStatus::Success,
            models_executed: execs,
            trigger: RunTrigger::Manual,
            config_hash: "cfg".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            check_gate_failed: false,
            verify_after_failed: false,
        }
    }

    /// Records `count` runs, each with one execution of `model_name`, and
    /// returns the state DB path (the writer handle is dropped so the
    /// read-only `optimize_output` path can open it — redb is single-writer).
    fn record_history(
        dir: &Path,
        model_name: &str,
        count: usize,
        duration_ms: u64,
        bytes_written: Option<u64>,
    ) -> std::path::PathBuf {
        let state_path = dir.join("state.redb");
        let store = StateStore::open(&state_path).expect("open state store");
        for i in 0..count {
            store
                .record_run(&run_with(
                    &format!("run-{i:03}"),
                    vec![make_exec(model_name, duration_ms, bytes_written)],
                ))
                .expect("record run");
        }
        drop(store);
        state_path
    }

    /// A model configured `type = "view"` must report `current_strategy =
    /// "view"` — not the pre-fix hard-coded `"table"` — and, since a view is
    /// also what `recommend_strategy` recommends for a fast single-consumer
    /// model, must get NO "switch to view" recommendation: the current and
    /// recommended strategy match, so savings is zero (#2056).
    #[test]
    fn view_model_reports_view_and_gets_no_switch_recommendation() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models_dir = tmp.path().join("models");
        write_model(&models_dir, "v_active", "type = \"view\"\n");
        // Fast (0.5s) + no dependents => recommend_strategy's "view" branch.
        let state_path = record_history(tmp.path(), "v_active", 5, 500, Some(1_000_000));
        let config_path = tmp.path().join("rocky.toml");

        let output = optimize_output(&state_path, &config_path, Some(&models_dir), None)
            .expect("optimize_output");
        assert_eq!(output.recommendations.len(), 1);
        let rec = &output.recommendations[0];
        assert_eq!(rec.current_strategy, "view", "reads the sidecar strategy");
        assert_eq!(rec.recommended_strategy, "view");
        assert_eq!(
            rec.estimated_monthly_savings, 0.0,
            "already a view: no recommended change"
        );
        // Pins that the "view" branch actually ran — not the
        // insufficient-history or unknown-strategy early return, which
        // would also produce recommended == current == "view" with zero
        // savings for the wrong reason.
        assert!(
            rec.reasoning.contains("fast execution"),
            "expected the fast-execution view reasoning, got: {}",
            rec.reasoning
        );
    }

    /// A model that appears only in run history (absent from the compiled
    /// models directory) must report `current_strategy = "unknown"`, and
    /// `recommend_strategy` must not recommend a change for it (#2056).
    #[test]
    fn history_only_model_reports_unknown_strategy() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models_dir = tmp.path().join("models"); // empty: no sidecar for this model
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = record_history(tmp.path(), "ghost_model", 5, 30_000, Some(2_000_000_000));
        let config_path = tmp.path().join("rocky.toml");

        let output = optimize_output(&state_path, &config_path, Some(&models_dir), None)
            .expect("optimize_output");
        assert_eq!(output.recommendations.len(), 1);
        let rec = &output.recommendations[0];
        assert_eq!(rec.current_strategy, "unknown");
        assert_eq!(
            rec.recommended_strategy, "unknown",
            "no recommendation for an unknown current strategy"
        );
        assert_eq!(rec.estimated_monthly_savings, 0.0);
    }

    /// `[cost]` prices from the loaded `rocky.toml` must feed the savings
    /// math, not the built-in `CostConfig::default()` rates (#2056). Uses a
    /// real (`view`) model + a branch where `current_strategy` differs from
    /// `recommended_strategy`, so `estimated_monthly_savings` is nonzero and
    /// visibly driven by the custom prices rather than by the "already
    /// optimal" zeroing path.
    #[test]
    fn cost_prices_come_from_the_loaded_project_config() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models_dir = tmp.path().join("models");
        write_model(&models_dir, "priced_model", "type = \"view\"\n");
        // compute_cost_per_dbu = 3600 => compute_cost_per_second = 1.0 (the
        // conversion in `CostConfig::from(CostSection)` divides by 3600), so
        // a 4s average run costs exactly $4.00/run, not the default $0.008.
        // storage_cost_per_gb_month = 100 => 1 GiB costs $100/mo, not $0.023.
        write(
            &tmp.path().join("rocky.toml"),
            "[cost]\ncompute_cost_per_dbu = 3600.0\nstorage_cost_per_gb_month = 100.0\n",
        );
        let config_path = tmp.path().join("rocky.toml");
        // duration >= 2s keeps this out of the "fast, view" branch so the
        // recommendation is driven by the compute-vs-storage comparison,
        // where both custom prices feed the number.
        let state_path = record_history(tmp.path(), "priced_model", 5, 4_000, Some(1_073_741_824));

        let output = optimize_output(&state_path, &config_path, Some(&models_dir), None)
            .expect("optimize_output");
        assert_eq!(output.recommendations.len(), 1);
        let rec = &output.recommendations[0];
        assert_eq!(rec.current_strategy, "view");
        assert!(
            (rec.compute_cost_per_run - 4.0).abs() < 1e-9,
            "expected $4.00/run from the custom compute price, got {}",
            rec.compute_cost_per_run
        );
        assert!(
            (rec.storage_cost_per_month - 100.0).abs() < 1e-9,
            "expected $100.00/mo from the custom storage price, got {}",
            rec.storage_cost_per_month
        );
        // 5 runs recorded within the same instant => runs_per_month = 150
        // (span_days floors to the 1-day minimum). monthly_compute = 4.0 *
        // 1.0 * 150 = 600 > storage (100) => "table" branch, savings = 600 -
        // 100 = 500. With the DEFAULT rates this would be ~$1.18/mo — the
        // two-orders-of-magnitude gap is the custom `[cost]` block, not
        // noise.
        assert_eq!(rec.recommended_strategy, "table");
        assert!(
            (rec.estimated_monthly_savings - 500.0).abs() < 1e-6,
            "expected $500.00/mo savings from the custom prices, got {}",
            rec.estimated_monthly_savings
        );
    }

    /// A `rocky.toml` that fails to parse must refuse rather than silently
    /// falling back to default pricing (mirrors `rocky cost`'s
    /// `adapter_pricing`: "a wrong price is worse than no price").
    #[test]
    fn a_broken_rocky_toml_is_an_error_not_a_silent_default() {
        let tmp = tempfile::tempdir().expect("tempdir");
        write(&tmp.path().join("rocky.toml"), "not valid toml [[[");
        let config_path = tmp.path().join("rocky.toml");
        let state_path = record_history(tmp.path(), "m", 5, 1_000, None);

        let err = optimize_output(&state_path, &config_path, None, None)
            .expect_err("a broken rocky.toml must refuse, not degrade to defaults");
        assert!(format!("{err:#}").contains("rocky.toml"));
    }
}
