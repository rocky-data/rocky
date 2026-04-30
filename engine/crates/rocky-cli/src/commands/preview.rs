//! `rocky preview` — PR preview workflow (Arc 1 ∩ Arc 2 user-facing surface).
//!
//! Three subcommands compose into a single PR comment:
//!
//! * `preview create` — pruned re-run on a per-PR branch with
//!   copy-from-base for unchanged upstream
//! * `preview diff`   — structural + sampled row-level diff vs. base
//! * `preview cost`   — per-model bytes/duration/USD delta vs. base

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info};

use crate::output::{
    PreviewCopiedModel, PreviewCostSummary, PreviewCreateOutput, PreviewDiffSummary,
    PreviewPrunedModel, print_json,
};

// ---------------------------------------------------------------------------
// `rocky preview create`
// ---------------------------------------------------------------------------

/// Orchestrate pruned planning on a per-PR branch with copy-from-base for
/// unchanged upstream.
///
/// Plans the prune-and-copy decision against the working DAG, registers
/// the branch in the state store, and **executes** the copy-from-base
/// step via the adapter's `clone_table_for_branch` trait method. The
/// trait dispatches per adapter: Databricks uses `SHALLOW CLONE`,
/// BigQuery uses `CREATE TABLE ... COPY` (both metadata-only); DuckDB
/// and Snowflake fall through to the portable CTAS default. Per-model
/// `copy_strategy = "ctas"` on success regardless of which underlying
/// primitive ran, `"failed"` on a per-model error (the rest of the copy
/// set still runs — partial-success surfaces in the output).
///
/// Auto-invoking the actual run for the prune set is intentionally
/// still deferred (the GitHub Action orchestrates). `run_status` is
/// `"planned"` for the prune set; `"copied"` is implicit for the copy
/// set via `copy_strategy = "ctas"`.
///
/// Steps:
///
/// 1. Run `git diff --name-status <base>...HEAD` against the working tree
///    to identify changed model files (mirrors `rocky_core::ci_diff`).
/// 2. Resolve change-set into model names (filename stems for `.sql` /
///    `.rocky`; sidecar `.toml` rolls back to its model).
/// 3. Build the working-tree DAG by walking `models_dir` for sidecars
///    declaring `depends_on`.
/// 4. Compute the prune set: changed models + every model that
///    transitively depends on a changed model.
/// 5. Compute the copy set: every working-DAG model not in the prune
///    set. (Removed-in-PR models are reported in `skipped_set`.)
/// 6. Generate a branch name from the current git branch when the
///    caller did not supply one.
/// 7. Register the branch via the state store (mirrors `rocky branch
///    create`).
/// 8. Build the adapter from `rocky.toml`, then for each copy-set model:
///    resolve its source schema from the model sidecar (`target.schema`),
///    issue `CREATE SCHEMA IF NOT EXISTS branch_schema` then
///    `CREATE OR REPLACE TABLE branch_schema.model AS SELECT * FROM
///    source_schema.model`. Phase 5 lifts this to native warehouse
///    clones (`SHALLOW CLONE` / zero-copy `CLONE`).
///
/// Returns a [`PreviewCreateOutput`] regardless of partial failures —
/// the PR-comment surface needs to stay informative.
#[allow(clippy::too_many_arguments)]
pub async fn run_preview_create(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    branch_name: Option<&str>,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    let head_ref = git_head_sha().unwrap_or_else(|_| "HEAD".to_string());

    // Step 1+2: identify changed model files between base and HEAD.
    let changed_paths = git_changed_paths(base_ref).with_context(|| {
        format!(
            "failed to compute changed files between {base_ref} and HEAD — is git installed and is this a git repository?"
        )
    })?;
    let changed_models = changed_models_under(&changed_paths, models_dir);
    debug!(
        "preview create: {} changed model(s) under {}",
        changed_models.len(),
        models_dir.display()
    );

    // Step 3: build the working-DAG from sidecar `depends_on`.
    let dag = ModelDag::scan(models_dir)
        .with_context(|| format!("failed to scan models in {}", models_dir.display()))?;

    // Step 4: compute prune set as the closure of changed models under
    // forward-edge traversal (downstream).
    let prune_names = dag.transitive_downstream(&changed_models);
    let prune_set: Vec<PreviewPrunedModel> = dag
        .models_in_topological_order()
        .into_iter()
        .filter(|name| prune_names.contains(name))
        .map(|name| {
            let reason = if changed_models.contains(&name) {
                "changed"
            } else {
                "downstream_of_changed"
            };
            PreviewPrunedModel {
                model_name: name.clone(),
                reason: reason.to_string(),
                changed_columns: vec![],
            }
        })
        .collect();

    // Step 5: copy set = every model in the working DAG NOT pruned.
    let copy_names: Vec<String> = dag
        .models_in_topological_order()
        .into_iter()
        .filter(|m| !prune_names.contains(m))
        .collect();

    // Resolve branch name (default: from current git branch).
    let resolved_branch_name = match branch_name {
        Some(n) => n.to_string(),
        None => default_branch_name_from_git()?,
    };

    // Step 6+7: register branch in state store. Idempotent — if the branch
    // already exists, surface a crisp error directing the user to
    // `rocky branch list`.
    crate::commands::run_branch_create(
        state_path,
        &resolved_branch_name,
        None,
        /*json=*/ false,
    )
    .with_context(|| {
        format!("failed to register preview branch '{resolved_branch_name}' in the state store")
    })?;

    let branch_schema = format!("branch__{resolved_branch_name}");

    // Step 8 (Phase 1.5): execute copy-from-base via the configured
    // warehouse adapter. Each entry in `copy_set` carries its
    // resolved source schema + the SQL strategy actually used. On
    // partial failure (model didn't exist in base, schema mismatch),
    // the per-model `copy_strategy = "failed"` and the rest of the
    // copy set still runs.
    let copy_set = execute_copy_from_base(
        config_path,
        models_dir,
        &branch_schema,
        &copy_names,
    )
    .await
    .unwrap_or_else(|e| {
        // A registry-build failure means we never even tried — surface
        // the plan with `copy_strategy = "planning_only"` so the
        // PR-comment surface still ships the prune-set decision.
        tracing::warn!(
            "preview create: copy-from-base step skipped — {e}. Falling back to planning-only output."
        );
        copy_names
            .iter()
            .map(|name| PreviewCopiedModel {
                model_name: name.clone(),
                source_schema: "<unresolved>".to_string(),
                target_schema: branch_schema.clone(),
                copy_strategy: "planning_only".to_string(),
            })
            .collect()
    });

    let duration_ms = start.elapsed().as_millis() as u64;

    // Phase 1 emits `run_status = "planned"` — actually invoking
    // `rocky run --branch <name>` for each prune-set model is left to
    // the caller (or to the Phase 4 GitHub Action). Going planned-only
    // keeps Phase 1 honest about scope without breaking the PR-comment
    // contract.
    let out = PreviewCreateOutput::new(
        resolved_branch_name,
        branch_schema,
        base_ref.to_string(),
        head_ref,
        prune_set,
        copy_set,
        Vec::new(), // skipped_set: removed-in-PR detection deferred
        String::new(),
        "planned".to_string(),
        duration_ms,
    );

    if json {
        print_json(&out)?;
    } else {
        info!("{}", render_preview_create_text(&out));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky preview diff`
// ---------------------------------------------------------------------------

/// Structural + (deferred) sampled row-level diff between branch and base
/// for every model that ran on both sides.
///
/// **Phase 2 scope.** Ships the *structural* layer — for each model that
/// has a `RunRecord` against both the branch run and the base run, surface
/// the row-count delta + bytes-scanned/written deltas with a deterministic
/// Markdown rendering. Each model carries a [`PreviewSamplingWindow`] with
/// `coverage = "not_yet_sampled"` and `coverage_warning = true` — honest
/// flag that row-level samples don't run yet. Phase 2.5 lifts to
/// checksum-bisection exhaustive diff (datafold-style) over the
/// `rocky_core::compare` shadow kernel; until then, structural deltas
/// catch row-count and byte-volume regressions, which is the most common
/// failure mode the PR-comment surface needs to flag.
///
/// **Why structural-only is useful.** `RunRecord` carries `rows_affected`
/// and `bytes_scanned` per model from the live run path, so a
/// branch-vs-base run-record diff already answers *"did this PR change
/// how many rows the model produced?"* and *"did the cost change?"* —
/// the two things a reviewer most needs to see. Sampled row content
/// remains the gold standard but the structural layer ships today.
pub async fn run_preview_diff(
    _config_path: &Path,
    state_path: &Path,
    branch_name: &str,
    base_ref: &str,
    _sample_size: usize,
    json: bool,
) -> Result<()> {
    use crate::output::PreviewDiffOutput;

    let store = rocky_core::state::StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    // Phase 2 substance: pull the latest two runs from the state store
    // and pair them up by branch-name. The *latest* run keyed to the
    // branch is "branch"; the most recent prior run *not* tagged to
    // this branch is "base." Tighter branch-vs-main partitioning lands
    // when `git_branch` is plumbed through the state-store branch
    // record (today the audit trail records `git_branch` on the
    // RunRecord directly).
    let mut branch_run: Option<rocky_core::state::RunRecord> = None;
    let mut base_run: Option<rocky_core::state::RunRecord> = None;
    for record in store.list_runs(50)? {
        if record.git_branch.as_deref() == Some(branch_name) {
            if branch_run.is_none() {
                branch_run = Some(record);
            }
        } else if base_run.is_none() {
            base_run = Some(record);
        }
        if branch_run.is_some() && base_run.is_some() {
            break;
        }
    }

    let (summary, models) = match (branch_run.as_ref(), base_run.as_ref()) {
        (Some(b), Some(p)) => build_preview_diff(b, p),
        _ => (empty_diff_summary(), Vec::new()),
    };
    let markdown = render_preview_diff_markdown(branch_name, base_ref, &summary, &models);

    let out = PreviewDiffOutput::new(
        branch_name.to_string(),
        base_ref.to_string(),
        summary,
        models,
        markdown,
    );

    if json {
        print_json(&out)?;
    } else {
        info!(
            "preview diff '{branch_name}' vs {base_ref}: {} models with changes \
             (sampling deferred to Phase 2.5)",
            out.summary.models_with_changes
        );
    }
    Ok(())
}

/// Build the per-model + summary structural diff from a branch and base
/// `RunRecord`. Pure: takes the two stored runs and produces deltas
/// without warehouse touchpoints.
///
/// **Pairing semantics.** Models matched by name across the two runs.
/// Models that ran on the branch but not on base are reported with a
/// `rows_added` figure equal to the branch-side `rows_affected` (best
/// proxy without sampled comparison). Models on base but not on branch
/// are skipped here — `preview cost` reports those as
/// `models_skipped_via_copy`. Both runs are forward-compatible: new
/// fields don't change the diff shape.
fn build_preview_diff(
    branch: &rocky_core::state::RunRecord,
    base: &rocky_core::state::RunRecord,
) -> (
    crate::output::PreviewDiffSummary,
    Vec<crate::output::PreviewModelDiff>,
) {
    use crate::output::{
        PreviewModelDiff, PreviewSampledRowDiff, PreviewSamplingWindow, PreviewStructuralDiff,
    };

    let mut models: Vec<PreviewModelDiff> = Vec::new();
    let mut models_with_changes: usize = 0;
    let mut total_rows_added: u64 = 0;
    let mut total_rows_removed: u64 = 0;
    let total_rows_changed: u64 = 0; // sampled-only; always 0 in Phase 2

    // Index base executions by model name for O(N) pairing.
    let base_by_name: BTreeMap<&str, &rocky_core::state::ModelExecution> = base
        .models_executed
        .iter()
        .map(|m| (m.model_name.as_str(), m))
        .collect();

    for branch_exec in &branch.models_executed {
        let base_exec = base_by_name.get(branch_exec.model_name.as_str()).copied();

        // Row delta: signed difference of `rows_affected`. None on either
        // side surfaces as zero — the sampled layer (Phase 2.5) catches
        // unreported row changes.
        let branch_rows = branch_exec.rows_affected.unwrap_or(0);
        let base_rows = base_exec.and_then(|e| e.rows_affected).unwrap_or(0);
        let (rows_added, rows_removed) = if branch_rows >= base_rows {
            (branch_rows.saturating_sub(base_rows), 0_u64)
        } else {
            (0_u64, base_rows.saturating_sub(branch_rows))
        };
        if rows_added > 0 || rows_removed > 0 {
            models_with_changes = models_with_changes.saturating_add(1);
        }
        total_rows_added = total_rows_added.saturating_add(rows_added);
        total_rows_removed = total_rows_removed.saturating_add(rows_removed);

        models.push(PreviewModelDiff {
            model_name: branch_exec.model_name.clone(),
            // Structural column-level delta is an open follow-up — the
            // RunRecord doesn't persist column lists today, and the
            // compiler-IR-driven path requires both runs to be
            // re-compiled (heavy). Phase 2.5 wires this; until then,
            // empty arrays make the absence explicit on the wire.
            structural: PreviewStructuralDiff {
                added_columns: Vec::new(),
                removed_columns: Vec::new(),
                type_changes: Vec::new(),
            },
            sampled: PreviewSampledRowDiff {
                rows_added,
                rows_removed,
                rows_changed: 0,
                samples: Vec::new(),
            },
            sampling_window: PreviewSamplingWindow {
                ordered_by: String::new(),
                limit: 0,
                // `coverage_warning = true` is honest: row content
                // changes that don't change row counts will not
                // surface in this diff. The PR-comment surface
                // renders this verbatim so reviewers don't infer
                // false coverage.
                coverage: "not_yet_sampled".to_string(),
                coverage_warning: true,
            },
        });
    }

    let summary = crate::output::PreviewDiffSummary {
        models_with_changes,
        models_unchanged: models.len().saturating_sub(models_with_changes),
        total_rows_added,
        total_rows_removed,
        total_rows_changed,
        any_coverage_warning: !models.is_empty(),
    };
    (summary, models)
}

/// Render a `PreviewDiffOutput` summary into the Markdown the PR-comment
/// surface posts verbatim. Format is stable: changing it requires
/// updating fixtures.
fn render_preview_diff_markdown(
    branch_name: &str,
    base_ref: &str,
    summary: &crate::output::PreviewDiffSummary,
    models: &[crate::output::PreviewModelDiff],
) -> String {
    if models.is_empty() {
        return format!(
            "**Preview diff** — branch `{branch_name}` vs `{base_ref}`\n\n\
             _No paired runs in the state store. Run `rocky run --branch {branch_name}` \
             on the prune set, then re-invoke `rocky preview diff`._\n"
        );
    }
    let mut out = String::new();
    out.push_str(&format!(
        "**Preview diff** — branch `{branch_name}` vs `{base_ref}`\n\n\
         {} model(s) with changes • {} unchanged • +{} / −{} rows\n\n",
        summary.models_with_changes,
        summary.models_unchanged,
        summary.total_rows_added,
        summary.total_rows_removed,
    ));
    out.push_str("| model | +rows | −rows | sampled? |\n|---|---:|---:|---|\n");
    for m in models {
        let sampled = if m.sampling_window.coverage_warning {
            ":warning: not yet sampled"
        } else {
            ":white_check_mark: sampled"
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            m.model_name, m.sampled.rows_added, m.sampled.rows_removed, sampled,
        ));
    }
    if summary.any_coverage_warning {
        out.push_str(
            "\n> :warning: Row-content changes that don't move row counts \
             are not surfaced in this diff. Phase 2.5 lifts to \
             checksum-bisection exhaustive sampling.\n",
        );
    }
    out
}

// ---------------------------------------------------------------------------
// `rocky preview cost`
// ---------------------------------------------------------------------------

/// Per-model bytes/duration/USD delta between branch and base. Diff layer
/// over `rocky cost latest`'s machinery — does not introduce new cost
/// math.
///
/// **Phase 3 scope.** Resolves the latest branch run and the most recent
/// non-branch run from the state store, computes per-model deltas via
/// `compute_observed_cost_usd` (the same formula `rocky cost` uses), and
/// emits a `PreviewCostOutput`. Models on base but not on branch roll
/// into `models_skipped_via_copy` + `savings_from_copy_usd`.
pub async fn run_preview_cost(
    config_path: &Path,
    state_path: &Path,
    branch_name: &str,
    json: bool,
) -> Result<()> {
    use crate::output::PreviewCostOutput;

    let store = rocky_core::state::StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    // Branch run = newest run with `git_branch == branch_name`.
    // Base run = newest run with a different (or absent) `git_branch`.
    let mut branch_run: Option<rocky_core::state::RunRecord> = None;
    let mut base_run: Option<rocky_core::state::RunRecord> = None;
    for record in store.list_runs(50)? {
        if record.git_branch.as_deref() == Some(branch_name) {
            if branch_run.is_none() {
                branch_run = Some(record);
            }
        } else if base_run.is_none() {
            base_run = Some(record);
        }
        if branch_run.is_some() && base_run.is_some() {
            break;
        }
    }

    // Resolve adapter cost params best-effort (mirrors `run_cost`).
    let cost_params: Option<(rocky_core::cost::WarehouseType, f64, f64)> =
        match rocky_core::config::load_rocky_config(config_path) {
            Ok(cfg) => {
                let dbu_per_hour =
                    rocky_core::cost::warehouse_size_to_dbu_per_hour(&cfg.cost.warehouse_size);
                let cost_per_dbu = cfg.cost.compute_cost_per_dbu;
                cfg.adapters
                    .get("default")
                    .or_else(|| cfg.adapters.values().next())
                    .and_then(|a| {
                        rocky_core::cost::WarehouseType::from_adapter_type(&a.adapter_type)
                    })
                    .map(|wh| (wh, dbu_per_hour, cost_per_dbu))
            }
            Err(_) => None,
        };

    let (summary, per_model) = match (branch_run.as_ref(), base_run.as_ref()) {
        (Some(b), Some(p)) => build_preview_cost_delta(b, p, cost_params.as_ref()),
        _ => (empty_cost_summary(), Vec::new()),
    };
    let markdown = render_preview_cost_markdown(branch_name, &summary, &per_model);

    let out = PreviewCostOutput::new(
        branch_name.to_string(),
        base_run.as_ref().map(|r| r.run_id.clone()),
        branch_run
            .as_ref()
            .map_or(String::new(), |r| r.run_id.clone()),
        summary,
        per_model,
        markdown,
    );

    if json {
        print_json(&out)?;
    } else {
        info!(
            "preview cost '{branch_name}': delta_usd={:?}",
            out.summary.delta_usd
        );
    }
    Ok(())
}

/// Pure cost-delta builder. Computes per-model bytes/duration/USD deltas
/// between two `RunRecord`s.
///
/// **Pairing semantics.**
/// - Models in **both** runs → full delta with `skipped_via_copy = false`.
/// - Models only on **branch** → delta vs. `0` (the model is new in the
///   PR; full branch cost is the delta).
/// - Models only on **base** → emitted with `skipped_via_copy = true`,
///   `branch_*` zero/None, `base_*` populated. The base cost rolls into
///   `savings_from_copy_usd` (the cost the PR avoided by copying).
fn build_preview_cost_delta(
    branch: &rocky_core::state::RunRecord,
    base: &rocky_core::state::RunRecord,
    cost_params: Option<&(rocky_core::cost::WarehouseType, f64, f64)>,
) -> (
    crate::output::PreviewCostSummary,
    Vec<crate::output::PreviewModelCostDelta>,
) {
    use crate::output::PreviewModelCostDelta;

    let cost_for = |bytes: Option<u64>, duration_ms: u64| -> Option<f64> {
        let (wh, dbu_per_hour, cost_per_dbu) = cost_params?;
        rocky_core::cost::compute_observed_cost_usd(
            *wh,
            bytes,
            duration_ms,
            *dbu_per_hour,
            *cost_per_dbu,
        )
    };

    let base_by_name: BTreeMap<&str, &rocky_core::state::ModelExecution> = base
        .models_executed
        .iter()
        .map(|m| (m.model_name.as_str(), m))
        .collect();
    let branch_by_name: BTreeMap<&str, &rocky_core::state::ModelExecution> = branch
        .models_executed
        .iter()
        .map(|m| (m.model_name.as_str(), m))
        .collect();

    // Stable union of model names, sorted alphabetically for
    // deterministic Markdown output.
    let mut all_names: BTreeSet<&str> = BTreeSet::new();
    for m in &branch.models_executed {
        all_names.insert(m.model_name.as_str());
    }
    for m in &base.models_executed {
        all_names.insert(m.model_name.as_str());
    }

    let mut per_model = Vec::with_capacity(all_names.len());
    let mut total_branch_cost: f64 = 0.0;
    let mut any_branch_cost = false;
    let mut total_base_cost: f64 = 0.0;
    let mut any_base_cost = false;
    let mut savings: f64 = 0.0;
    let mut any_savings = false;
    let mut models_skipped_via_copy: usize = 0;

    for name in all_names {
        let branch_exec = branch_by_name.get(name).copied();
        let base_exec = base_by_name.get(name).copied();

        let skipped_via_copy = branch_exec.is_none() && base_exec.is_some();
        if skipped_via_copy {
            models_skipped_via_copy = models_skipped_via_copy.saturating_add(1);
        }

        let branch_duration_ms = branch_exec.map(|e| e.duration_ms).unwrap_or(0);
        let base_duration_ms = base_exec.map(|e| e.duration_ms).unwrap_or(0);
        let branch_bytes_scanned = branch_exec.and_then(|e| e.bytes_scanned);
        let base_bytes_scanned = base_exec.and_then(|e| e.bytes_scanned);

        let branch_cost_usd = if branch_exec.is_some() {
            let c = cost_for(branch_bytes_scanned, branch_duration_ms);
            if let Some(c) = c {
                total_branch_cost += c;
                any_branch_cost = true;
            }
            c
        } else {
            None
        };
        let base_cost_usd = if base_exec.is_some() {
            let c = cost_for(base_bytes_scanned, base_duration_ms);
            if let Some(c) = c {
                if !skipped_via_copy {
                    // Only models that ran on both sides count toward
                    // the comparable base-cost total — copied models'
                    // base cost rolls into savings_from_copy_usd.
                    total_base_cost += c;
                    any_base_cost = true;
                } else {
                    savings += c;
                    any_savings = true;
                }
            }
            c
        } else {
            None
        };
        let delta_usd = match (branch_cost_usd, base_cost_usd) {
            (Some(b), Some(p)) if !skipped_via_copy => Some(b - p),
            // For models only on branch (new in PR), delta == branch cost.
            (Some(b), None) => Some(b),
            _ => None,
        };

        per_model.push(PreviewModelCostDelta {
            model_name: name.to_string(),
            skipped_via_copy,
            branch_cost_usd,
            base_cost_usd,
            delta_usd,
            branch_duration_ms,
            base_duration_ms,
            branch_bytes_scanned,
            base_bytes_scanned,
        });
    }

    let total_branch_cost_usd = if any_branch_cost {
        Some(total_branch_cost)
    } else {
        None
    };
    let total_base_cost_usd = if any_base_cost {
        Some(total_base_cost)
    } else {
        None
    };
    let delta_usd = match (total_branch_cost_usd, total_base_cost_usd) {
        (Some(b), Some(p)) => Some(b - p),
        _ => None,
    };
    let savings_from_copy_usd = if any_savings { Some(savings) } else { None };

    let summary = crate::output::PreviewCostSummary {
        total_branch_cost_usd,
        total_base_cost_usd,
        delta_usd,
        models_skipped_via_copy,
        savings_from_copy_usd,
    };
    (summary, per_model)
}

/// Render a `PreviewCostOutput` summary into the PR-comment Markdown.
fn render_preview_cost_markdown(
    branch_name: &str,
    summary: &crate::output::PreviewCostSummary,
    per_model: &[crate::output::PreviewModelCostDelta],
) -> String {
    if per_model.is_empty() {
        return format!(
            "**Preview cost** — branch `{branch_name}`\n\n\
             _No branch run yet. Run `rocky run --branch {branch_name}` on the prune set, \
             then re-invoke `rocky preview cost`._\n"
        );
    }
    let fmt_usd = |v: Option<f64>| -> String {
        v.map(|v| format!("${v:.6}")).unwrap_or_else(|| "—".into())
    };
    let mut out = String::new();
    out.push_str(&format!(
        "**Preview cost** — branch `{branch_name}`\n\n\
         Δ vs base: {}  •  branch total: {}  •  base total: {}  •  copy-savings: {}\n\n",
        fmt_usd(summary.delta_usd),
        fmt_usd(summary.total_branch_cost_usd),
        fmt_usd(summary.total_base_cost_usd),
        fmt_usd(summary.savings_from_copy_usd),
    ));
    out.push_str(
        "| model | branch $ | base $ | Δ $ | branch dur | base dur | copied |\n\
         |---|---:|---:|---:|---:|---:|---|\n",
    );
    for m in per_model {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {}ms | {}ms | {} |\n",
            m.model_name,
            fmt_usd(m.branch_cost_usd),
            fmt_usd(m.base_cost_usd),
            fmt_usd(m.delta_usd),
            m.branch_duration_ms,
            m.base_duration_ms,
            if m.skipped_via_copy { "✓" } else { "" },
        ));
    }
    out.push_str(&format!(
        "\n_{} model(s) skipped via copy._\n",
        summary.models_skipped_via_copy,
    ));
    out
}

// ---------------------------------------------------------------------------
// Git plumbing — change detection between two refs
// ---------------------------------------------------------------------------

/// Resolve `HEAD` to a short SHA for output provenance. Returns the
/// literal `"HEAD"` if git is unavailable so the PreviewCreateOutput
/// stays well-formed even off git.
fn git_head_sha() -> Result<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .context("`git rev-parse HEAD` failed")?;
    if !out.status.success() {
        anyhow::bail!("git rev-parse HEAD exited non-zero");
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Resolve the current branch name into a stable preview-branch slug.
/// Falls back to a timestamp-based name if no branch is checked out
/// (e.g. detached HEAD).
fn default_branch_name_from_git() -> Result<String> {
    let out = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .context("`git rev-parse --abbrev-ref HEAD` failed")?;
    if !out.status.success() {
        return Ok(format!("pr-preview-{}", Utc::now().format("%Y%m%d-%H%M%S")));
    }
    let raw = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if raw.is_empty() || raw == "HEAD" {
        Ok(format!("pr-preview-{}", Utc::now().format("%Y%m%d-%H%M%S")))
    } else {
        // Slug: replace `/` and other path-unfriendly chars with `_`.
        let slug: String = raw
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        Ok(format!("pr-preview-{slug}"))
    }
}

/// Run `git diff --name-status` between `base_ref` and `HEAD`. Mirrors
/// `rocky_core::ci_diff` semantics: three-dot syntax first (merge-base),
/// two-dot fallback for shallow clones. Returns paths relative to the
/// repository root.
fn git_changed_paths(base_ref: &str) -> Result<Vec<String>> {
    let three_dot = Command::new("git")
        .args(["diff", "--name-only", &format!("{base_ref}...HEAD")])
        .output()
        .context("`git diff` failed — is git installed?")?;
    if three_dot.status.success() {
        return Ok(parse_paths(&three_dot.stdout));
    }

    debug!(
        "three-dot git diff failed (exit {}), falling back to two-dot",
        three_dot.status
    );
    let two_dot = Command::new("git")
        .args(["diff", "--name-only", base_ref, "HEAD"])
        .output()
        .context("`git diff` (two-dot) failed")?;
    if !two_dot.status.success() {
        let stderr = String::from_utf8_lossy(&two_dot.stderr);
        anyhow::bail!("git diff failed against base ref '{base_ref}': {stderr}");
    }
    Ok(parse_paths(&two_dot.stdout))
}

/// Split `git diff --name-only` output into trimmed lines.
fn parse_paths(raw: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(raw)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Filter changed paths to model files under `models_dir` and return the
/// affected model names (filename stems). A `.toml` sidecar maps back to
/// the model whose name it carries; absent that, falls back to the file
/// stem.
fn changed_models_under(paths: &[String], models_dir: &Path) -> BTreeSet<String> {
    let canonical_models = match models_dir.canonicalize() {
        Ok(p) => p,
        Err(_) => models_dir.to_path_buf(),
    };
    let mut out = BTreeSet::new();
    for raw in paths {
        let p = PathBuf::from(raw);
        let abs = match p.canonicalize() {
            Ok(a) => a,
            Err(_) => p.clone(),
        };
        // Path filter: must be under the models dir (best-effort —
        // canonicalize may fail for deleted files, in which case we
        // string-match the raw path instead).
        let in_models_dir = abs.starts_with(&canonical_models)
            || raw.contains(&format!(
                "{}{}",
                models_dir.display(),
                std::path::MAIN_SEPARATOR
            ));
        if !in_models_dir {
            continue;
        }
        let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
        if !matches!(ext, "sql" | "rocky" | "toml") {
            continue;
        }
        if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
            // Strip a `_defaults` sidecar — it's directory-level config,
            // not a model. (Sidecars otherwise share their stem with the
            // model file.)
            if stem == "_defaults" {
                continue;
            }
            out.insert(stem.to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Working-tree DAG — built from sidecar `.toml` files' `depends_on`
// ---------------------------------------------------------------------------

/// Lightweight DAG built from `models/` sidecars. Keys are model
/// filename stems; edges are forward (a → b means "a feeds b").
///
/// This is intentionally not the full `rocky_core::dag::DependencyGraph`
/// — preview only needs reachability, not type-checked schemas. Building
/// from sidecars keeps the dependency on the compiler off the preview
/// hot path. If the sidecar is malformed, the model is still tracked
/// with empty deps so renames don't drop a node from the prune set.
#[derive(Debug, Default)]
struct ModelDag {
    /// model_name → list of upstream model names.
    upstreams: BTreeMap<String, Vec<String>>,
}

impl ModelDag {
    /// Walk `models_dir` non-recursively, parse every `.toml` sidecar
    /// for `depends_on`, and register each model with its upstreams.
    /// Bare `.sql` or `.rocky` files without sidecars get an empty
    /// upstream list.
    fn scan(models_dir: &Path) -> Result<Self> {
        let mut upstreams: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if !models_dir.exists() {
            return Ok(ModelDag { upstreams });
        }
        for entry in fs::read_dir(models_dir)
            .with_context(|| format!("reading models dir {}", models_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            if stem == "_defaults" {
                continue;
            }
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            match ext {
                "sql" | "rocky" => {
                    upstreams.entry(stem).or_default();
                }
                "toml" => {
                    let deps = read_depends_on(&path).unwrap_or_default();
                    upstreams.insert(stem, deps);
                }
                _ => {}
            }
        }
        Ok(ModelDag { upstreams })
    }

    /// Forward-traverse downstream from every seed model: returns the
    /// closure of `seeds ∪ { x : ∃ seed s.t. seed →* x }` over the
    /// reverse-edge index.
    fn transitive_downstream(&self, seeds: &BTreeSet<String>) -> HashSet<String> {
        let mut downstreams: HashMap<String, Vec<String>> = HashMap::new();
        for (model, deps) in &self.upstreams {
            for d in deps {
                downstreams
                    .entry(d.clone())
                    .or_default()
                    .push(model.clone());
            }
        }

        let mut result: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        for s in seeds {
            // Only seed if the model exists in the DAG; renames are
            // surfaced as "model added" later.
            if self.upstreams.contains_key(s) || downstreams.contains_key(s) {
                queue.push_back(s.clone());
                result.insert(s.clone());
            }
        }
        while let Some(m) = queue.pop_front() {
            if let Some(children) = downstreams.get(&m) {
                for c in children {
                    if result.insert(c.clone()) {
                        queue.push_back(c.clone());
                    }
                }
            }
        }
        result
    }

    /// Topologically-ordered model list (Kahn's algorithm). Output is
    /// stable across runs — `BTreeMap` iteration gives alphabetical
    /// tie-break. Cycles fall back to alphabetical to keep the surface
    /// deterministic even on pathological DAGs.
    fn models_in_topological_order(&self) -> Vec<String> {
        // `indeg[m]` = number of upstream models that `m` depends on.
        // Models with no upstreams (leaves) start with `indeg == 0` and
        // emit first.
        let mut indeg: BTreeMap<String, usize> = self
            .upstreams
            .keys()
            .map(|m| (m.clone(), 0_usize))
            .collect();
        for (model, deps) in &self.upstreams {
            // Only count deps that actually exist in the DAG — a sidecar
            // pointing at a model removed from the working tree must not
            // produce an indeg that never reaches zero.
            let valid = deps.iter().filter(|d| indeg.contains_key(*d)).count();
            if let Some(c) = indeg.get_mut(model) {
                *c = valid;
            }
        }
        let mut queue: VecDeque<String> = indeg
            .iter()
            .filter(|(_, c)| **c == 0)
            .map(|(m, _)| m.clone())
            .collect();
        let mut out = Vec::new();
        while let Some(m) = queue.pop_front() {
            out.push(m.clone());
            // Every model whose deps contain `m` loses an unsatisfied
            // upstream; emit it once all its upstreams are settled.
            for (other, deps) in &self.upstreams {
                if deps.contains(&m)
                    && let Some(c) = indeg.get_mut(other)
                {
                    *c -= 1;
                    if *c == 0 {
                        queue.push_back(other.clone());
                    }
                }
            }
        }
        if out.len() != self.upstreams.len() {
            // Cycle fallback: deterministic alphabetical.
            let mut all: Vec<String> = self.upstreams.keys().cloned().collect();
            all.sort();
            return all;
        }
        out
    }
}

/// Parse `depends_on = [...]` from a sidecar `.toml`. Tolerant of
/// malformed files — returns empty deps rather than failing the whole
/// preview.
fn read_depends_on(path: &Path) -> Result<Vec<String>> {
    let text =
        fs::read_to_string(path).with_context(|| format!("reading sidecar {}", path.display()))?;
    let value: toml::Value = match toml::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    let Some(arr) = value.get("depends_on").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };
    Ok(arr
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect())
}

// ---------------------------------------------------------------------------
// Helpers shared across phases (test surface)
// ---------------------------------------------------------------------------

/// Render a `PreviewCreateOutput` as a single-line human summary used by the
/// non-JSON path. Kept here (not in `output.rs`) because formatting is a CLI
/// concern, not a wire-protocol concern.
pub fn render_preview_create_text(out: &PreviewCreateOutput) -> String {
    format!(
        "preview branch '{}' (schema: {}) — pruned: {}, copied: {}, skipped: {}; run_id={} ({})",
        out.branch_name,
        out.branch_schema,
        out.prune_set.len(),
        out.copy_set.len(),
        out.skipped_set.len(),
        if out.run_id.is_empty() {
            "<no-op>"
        } else {
            &out.run_id
        },
        out.run_status,
    )
}

/// Build an empty-but-valid summary for a no-op preview (no models changed).
/// Keeps every consumer downstream of `PreviewDiffOutput` from special-casing
/// "the JSON has no `summary` field."
pub fn empty_diff_summary() -> PreviewDiffSummary {
    PreviewDiffSummary {
        models_with_changes: 0,
        models_unchanged: 0,
        total_rows_added: 0,
        total_rows_removed: 0,
        total_rows_changed: 0,
        any_coverage_warning: false,
    }
}

/// Build an empty-but-valid cost summary for a no-op preview.
pub fn empty_cost_summary() -> PreviewCostSummary {
    PreviewCostSummary {
        total_branch_cost_usd: None,
        total_base_cost_usd: None,
        delta_usd: None,
        models_skipped_via_copy: 0,
        savings_from_copy_usd: None,
    }
}

// ---------------------------------------------------------------------------
// Phase 1.5 — copy-from-base execution via the WarehouseAdapter trait
// ---------------------------------------------------------------------------

/// Execute the copy-from-base step against the configured adapter.
///
/// Builds the adapter registry from `rocky.toml`, resolves each
/// copy-set model's source schema from its sidecar (`target.schema`),
/// then issues:
///
/// 1. `CREATE SCHEMA IF NOT EXISTS branch_schema` (once per distinct schema).
/// 2. For each model, `CREATE OR REPLACE TABLE branch_schema.<model> AS
///    SELECT * FROM <source_schema>.<model>`.
///
/// Per-model failures are surfaced as `copy_strategy = "failed"` on the
/// returned `PreviewCopiedModel` rather than aborting — the rest of the
/// copy set still runs so the PR-comment surface is informative.
///
/// **DuckDB note.** DuckDB CTAS doesn't accept the catalog prefix on
/// every dialect path; the default trait impl writes the SQL without a
/// catalog qualifier (just `schema.table`), which works for DuckDB and
/// the cloud warehouses. Adapters that ship native warehouse clones
/// (Databricks `SHALLOW CLONE`, Snowflake `CLONE`, BigQuery
/// `CREATE TABLE ... COPY`) override
/// [`WarehouseAdapter::clone_table_for_branch`] in their own crates;
/// this function only calls the trait method.
async fn execute_copy_from_base(
    config_path: &Path,
    models_dir: &Path,
    branch_schema: &str,
    copy_names: &[String],
) -> Result<Vec<PreviewCopiedModel>> {
    if copy_names.is_empty() {
        return Ok(Vec::new());
    }

    // Load the config + build the adapter registry. A failure here is
    // a hard skip: the caller's `unwrap_or_else` falls back to
    // planning-only output.
    let cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;

    let registry = crate::registry::AdapterRegistry::from_config(&cfg)
        .context("building adapter registry from config")?;

    // Pick the warehouse adapter. Mirrors `rocky cost`'s resolution:
    // prefer `default`; fall back to first-declared.
    let adapter_names = registry.warehouse_adapter_names();
    let adapter_name = if adapter_names.iter().any(|n| n == "default") {
        "default".to_string()
    } else {
        adapter_names
            .into_iter()
            .next()
            .context("no warehouse adapters configured for preview create")?
    };
    let adapter = registry.warehouse_adapter(&adapter_name)?;
    let adapter_type = registry
        .adapter_config(&adapter_name)
        .map(|c| c.adapter_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Load the model sidecars to resolve each model's source schema.
    let models = rocky_core::models::load_models_from_dir(models_dir)
        .with_context(|| format!("loading models from {}", models_dir.display()))?;
    let model_by_name: BTreeMap<String, &rocky_core::models::Model> =
        models.iter().map(|m| (m.config.name.clone(), m)).collect();

    // Create the branch schema once (idempotent across both DuckDB and
    // cloud warehouses).
    let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{branch_schema}\"");
    if let Err(e) = adapter.execute_statement(&create_schema_sql).await {
        // A schema-create failure on a writeable adapter is unrecoverable
        // for the copy step — bail to planning-only.
        return Err(anyhow::anyhow!(
            "creating branch schema '{branch_schema}' on adapter '{adapter_name}' ({adapter_type}): {e}"
        ));
    }

    let mut results = Vec::with_capacity(copy_names.len());
    for model_name in copy_names {
        let Some(model) = model_by_name.get(model_name) else {
            // Model doesn't exist in the working tree — planning-only
            // entry so the prune set still ships.
            results.push(PreviewCopiedModel {
                model_name: model_name.clone(),
                source_schema: "<unresolved>".to_string(),
                target_schema: branch_schema.to_string(),
                copy_strategy: "planning_only".to_string(),
            });
            continue;
        };
        let source_schema = model.config.target.schema.clone();
        let table = model.config.target.table.clone();
        let source_catalog = model.config.target.catalog.clone();
        let source_ref = rocky_core::ir::TableRef {
            catalog: source_catalog,
            schema: source_schema.clone(),
            table: table.clone(),
        };

        match adapter
            .clone_table_for_branch(&source_ref, branch_schema)
            .await
        {
            Ok(()) => results.push(PreviewCopiedModel {
                model_name: model_name.clone(),
                source_schema,
                target_schema: branch_schema.to_string(),
                copy_strategy: "ctas".to_string(),
            }),
            Err(e) => {
                tracing::warn!(
                    "preview create: copy-from-base failed for '{model_name}' \
                     (likely missing in base schema '{source_schema}'): {e}"
                );
                results.push(PreviewCopiedModel {
                    model_name: model_name.clone(),
                    source_schema,
                    target_schema: branch_schema.to_string(),
                    copy_strategy: "failed".to_string(),
                });
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{
        PreviewModelCostDelta, PreviewModelDiff, PreviewRowSample, PreviewRowSampleChange,
        PreviewSampledRowDiff, PreviewSamplingWindow, PreviewStructuralDiff,
    };
    use std::io::Write;
    use tempfile::TempDir;

    /// A no-op preview (no changes, no copies, no skips, no run) renders
    /// without panicking and surfaces the `<no-op>` sentinel.
    #[test]
    fn render_preview_create_text_handles_noop() {
        let out = PreviewCreateOutput::new(
            "fix-price".into(),
            "branch__fix-price".into(),
            "main".into(),
            "deadbeef".into(),
            vec![],
            vec![],
            vec![],
            String::new(),
            "no_op".into(),
            0,
        );
        let text = render_preview_create_text(&out);
        assert!(text.contains("preview branch 'fix-price'"));
        assert!(text.contains("run_id=<no-op>"));
        assert!(text.contains("pruned: 0, copied: 0, skipped: 0"));
    }

    /// A populated preview output renders the counts and the run id.
    #[test]
    fn render_preview_create_text_handles_populated() {
        let out = PreviewCreateOutput::new(
            "feature_x".into(),
            "branch__feature_x".into(),
            "main".into(),
            "head_sha".into(),
            vec![PreviewPrunedModel {
                model_name: "fct_revenue".into(),
                reason: "changed".into(),
                changed_columns: vec!["total".into()],
            }],
            vec![PreviewCopiedModel {
                model_name: "raw_orders".into(),
                source_schema: "demo".into(),
                target_schema: "branch__feature_x__demo".into(),
                copy_strategy: "ctas".into(),
            }],
            vec!["unrelated_model".into()],
            "run_abc123".into(),
            "succeeded".into(),
            450,
        );
        let text = render_preview_create_text(&out);
        assert!(text.contains("pruned: 1, copied: 1, skipped: 1"));
        assert!(text.contains("run_id=run_abc123"));
        assert!(text.contains("(succeeded)"));
    }

    /// `empty_diff_summary` and `empty_cost_summary` produce all-zero
    /// payloads — important for no-op preview consumers.
    #[test]
    fn empty_summaries_are_zeroed() {
        let diff = empty_diff_summary();
        assert_eq!(diff.models_with_changes, 0);
        assert_eq!(diff.total_rows_added, 0);
        assert!(!diff.any_coverage_warning);

        let cost = empty_cost_summary();
        assert!(cost.total_branch_cost_usd.is_none());
        assert!(cost.delta_usd.is_none());
        assert_eq!(cost.models_skipped_via_copy, 0);
    }

    /// Smoke test that the structural diff struct roundtrips through
    /// schemars — catches a JsonSchema derive break before `just codegen`
    /// runs.
    #[test]
    fn structural_diff_serializes() {
        let diff = PreviewStructuralDiff {
            added_columns: vec!["new_col".into()],
            removed_columns: vec![],
            type_changes: vec![],
        };
        let s = serde_json::to_string(&diff).unwrap();
        assert!(s.contains("new_col"));
    }

    /// Coverage-warning path: the sampling-window field surfaces verbatim
    /// in the per-model diff, even when the sampled deltas are zero.
    #[test]
    fn sampling_window_surfaces_coverage_warning() {
        let m = PreviewModelDiff {
            model_name: "wide_table".into(),
            structural: PreviewStructuralDiff {
                added_columns: vec![],
                removed_columns: vec![],
                type_changes: vec![],
            },
            sampled: PreviewSampledRowDiff {
                rows_added: 0,
                rows_removed: 0,
                rows_changed: 0,
                samples: vec![],
            },
            sampling_window: PreviewSamplingWindow {
                ordered_by: "id".into(),
                limit: 1000,
                coverage: "first_n_by_order".into(),
                coverage_warning: true,
            },
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"coverage_warning\":true"));
        assert!(s.contains("\"first_n_by_order\""));
    }

    /// Sample row roundtrips cleanly — sanity check on the nested struct.
    #[test]
    fn row_sample_serializes() {
        let sample = PreviewRowSample {
            primary_key: "42".into(),
            changes: vec![PreviewRowSampleChange {
                column: "amount".into(),
                base_value: "100".into(),
                branch_value: "101".into(),
            }],
        };
        let s = serde_json::to_string(&sample).unwrap();
        assert!(s.contains("\"primary_key\":\"42\""));
        assert!(s.contains("\"branch_value\":\"101\""));
    }

    /// Cost delta roundtrips with `skipped_via_copy = true` and `None`
    /// branch fields — the canonical shape for a copied model.
    #[test]
    fn cost_delta_skipped_via_copy() {
        let delta = PreviewModelCostDelta {
            model_name: "raw_orders".into(),
            skipped_via_copy: true,
            branch_cost_usd: None,
            base_cost_usd: Some(0.012),
            delta_usd: None,
            branch_duration_ms: 0,
            base_duration_ms: 1200,
            branch_bytes_scanned: None,
            base_bytes_scanned: Some(98_304),
        };
        let s = serde_json::to_string(&delta).unwrap();
        assert!(s.contains("\"skipped_via_copy\":true"));
        assert!(s.contains("\"base_cost_usd\":0.012"));
    }

    // ---------------------- DAG / git plumbing ----------------------

    fn write_sidecar(dir: &Path, stem: &str, deps: &[&str]) {
        let mut f = std::fs::File::create(dir.join(format!("{stem}.toml"))).unwrap();
        if deps.is_empty() {
            writeln!(f, "name = \"{stem}\"").unwrap();
        } else {
            let arr: Vec<String> = deps.iter().map(|d| format!("\"{d}\"")).collect();
            writeln!(f, "name = \"{stem}\"\ndepends_on = [{}]", arr.join(", ")).unwrap();
        }
        // Touch a model file so scan picks the stem up unconditionally.
        std::fs::File::create(dir.join(format!("{stem}.sql"))).unwrap();
    }

    #[test]
    fn dag_topological_order_is_stable() {
        let tmp = TempDir::new().unwrap();
        write_sidecar(tmp.path(), "raw_orders", &[]);
        write_sidecar(tmp.path(), "stg_orders", &["raw_orders"]);
        write_sidecar(tmp.path(), "fct_revenue", &["stg_orders"]);
        let dag = ModelDag::scan(tmp.path()).unwrap();
        let order = dag.models_in_topological_order();
        // raw_orders before stg_orders before fct_revenue.
        let pos = |s: &str| order.iter().position(|m| m == s).unwrap();
        assert!(pos("raw_orders") < pos("stg_orders"));
        assert!(pos("stg_orders") < pos("fct_revenue"));
    }

    #[test]
    fn dag_transitive_downstream_walks_forward() {
        let tmp = TempDir::new().unwrap();
        write_sidecar(tmp.path(), "raw_orders", &[]);
        write_sidecar(tmp.path(), "stg_orders", &["raw_orders"]);
        write_sidecar(tmp.path(), "fct_revenue", &["stg_orders"]);
        write_sidecar(tmp.path(), "dim_customers", &[]); // unrelated
        let dag = ModelDag::scan(tmp.path()).unwrap();
        let mut seeds = BTreeSet::new();
        seeds.insert("raw_orders".to_string());
        let pruned = dag.transitive_downstream(&seeds);
        assert!(pruned.contains("raw_orders"));
        assert!(pruned.contains("stg_orders"));
        assert!(pruned.contains("fct_revenue"));
        assert!(
            !pruned.contains("dim_customers"),
            "unrelated model must not be pruned"
        );
    }

    #[test]
    fn dag_transitive_downstream_is_idempotent_on_seeds() {
        // Seeding with a model that has no downstream dependencies
        // returns just the seed itself.
        let tmp = TempDir::new().unwrap();
        write_sidecar(tmp.path(), "raw_orders", &[]);
        let dag = ModelDag::scan(tmp.path()).unwrap();
        let mut seeds = BTreeSet::new();
        seeds.insert("raw_orders".to_string());
        let pruned = dag.transitive_downstream(&seeds);
        assert_eq!(pruned.len(), 1);
        assert!(pruned.contains("raw_orders"));
    }

    /// Renaming a file shows up as both an added stem and a removed stem
    /// (git diff sees two paths). The DAG only carries the one that
    /// exists in the working tree, so a renamed-from model just isn't
    /// in the prune set's reachable nodes — that's fine. This test
    /// proves the seed-existence guard skips renamed-away models so we
    /// don't pull in spurious downstream.
    #[test]
    fn dag_transitive_downstream_skips_unknown_seed() {
        let tmp = TempDir::new().unwrap();
        write_sidecar(tmp.path(), "raw_orders", &[]);
        let dag = ModelDag::scan(tmp.path()).unwrap();
        let mut seeds = BTreeSet::new();
        seeds.insert("renamed_away".to_string());
        let pruned = dag.transitive_downstream(&seeds);
        assert!(pruned.is_empty());
    }

    #[test]
    fn dag_handles_malformed_sidecar() {
        let tmp = TempDir::new().unwrap();
        let bad_path = tmp.path().join("bad.toml");
        std::fs::write(&bad_path, "this is not valid TOML [\n").unwrap();
        std::fs::File::create(tmp.path().join("bad.sql")).unwrap();
        let dag = ModelDag::scan(tmp.path()).unwrap();
        // Model is still tracked with empty deps — no panic on bad TOML.
        assert!(dag.upstreams.contains_key("bad"));
    }

    #[test]
    fn changed_models_under_filters_to_models_dir() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // Use raw paths that aren't real on disk — `changed_models_under`
        // falls back to string match when canonicalize fails.
        let paths = vec![
            format!("{}/foo.sql", models.display()),
            format!("{}/bar.toml", models.display()),
            format!("{}/_defaults.toml", models.display()),
            "README.md".to_string(),
            format!("{}/baz.txt", models.display()),
        ];
        let out = changed_models_under(&paths, &models);
        assert!(out.contains("foo"));
        assert!(out.contains("bar"));
        assert!(!out.contains("_defaults"));
        assert!(!out.contains("README"));
        assert!(!out.contains("baz"));
    }

    #[test]
    fn parse_paths_strips_whitespace() {
        let raw = b"foo.sql\n  bar.toml\n\nbaz\n";
        let out = parse_paths(raw);
        assert_eq!(out, vec!["foo.sql", "bar.toml", "baz"]);
    }

    // ---------------------- Phase 2 + Phase 3 substance ----------------------

    use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource};

    fn exec(name: &str, duration_ms: u64, rows: Option<u64>, bytes: Option<u64>) -> ModelExecution {
        let now = chrono::Utc::now();
        ModelExecution {
            model_name: name.to_string(),
            started_at: now,
            finished_at: now + chrono::Duration::milliseconds(duration_ms as i64),
            duration_ms,
            rows_affected: rows,
            status: "success".to_string(),
            sql_hash: format!("hash_{name}"),
            bytes_scanned: bytes,
            bytes_written: None,
        }
    }

    fn run_record(
        run_id: &str,
        models: Vec<ModelExecution>,
        git_branch: Option<&str>,
    ) -> RunRecord {
        let now = chrono::Utc::now();
        RunRecord {
            run_id: run_id.to_string(),
            started_at: now,
            finished_at: now,
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "h".into(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: git_branch.map(str::to_string),
            idempotency_key: None,
            target_catalog: None,
            hostname: "test".into(),
            rocky_version: "0.0.0-test".into(),
        }
    }

    /// Branch row count > base → rows_added; reverse → rows_removed.
    /// Both directions must surface as `models_with_changes` and roll
    /// into the summary totals.
    #[test]
    fn diff_row_delta_signs_correctly() {
        let branch = run_record(
            "br",
            vec![
                exec("a", 100, Some(110), None),
                exec("b", 100, Some(50), None),
            ],
            Some("feature_x"),
        );
        let base = run_record(
            "ba",
            vec![
                exec("a", 100, Some(100), None),
                exec("b", 100, Some(60), None),
            ],
            None,
        );
        let (summary, models) = build_preview_diff(&branch, &base);
        assert_eq!(summary.models_with_changes, 2);
        assert_eq!(summary.total_rows_added, 10); // a +10
        assert_eq!(summary.total_rows_removed, 10); // b −10
        assert!(summary.any_coverage_warning);
        let by_name: HashMap<&str, &crate::output::PreviewModelDiff> =
            models.iter().map(|m| (m.model_name.as_str(), m)).collect();
        assert_eq!(by_name["a"].sampled.rows_added, 10);
        assert_eq!(by_name["a"].sampled.rows_removed, 0);
        assert_eq!(by_name["b"].sampled.rows_added, 0);
        assert_eq!(by_name["b"].sampled.rows_removed, 10);
    }

    /// Identical row counts → no changes; coverage_warning still fires
    /// because Phase 2 didn't sample contents.
    #[test]
    fn diff_no_change_still_warns_about_coverage() {
        let branch = run_record("br", vec![exec("a", 100, Some(100), None)], Some("feature"));
        let base = run_record("ba", vec![exec("a", 100, Some(100), None)], None);
        let (summary, models) = build_preview_diff(&branch, &base);
        assert_eq!(summary.models_with_changes, 0);
        assert_eq!(summary.models_unchanged, 1);
        assert!(summary.any_coverage_warning);
        assert!(models[0].sampling_window.coverage_warning);
        assert_eq!(models[0].sampling_window.coverage, "not_yet_sampled");
    }

    /// Cost delta on paired models: branch faster than base → negative
    /// delta (savings); slower → positive delta.
    #[test]
    fn cost_delta_databricks_paired_models() {
        let branch = run_record(
            "br",
            vec![exec("a", 60_000, Some(100), Some(1024))],
            Some("feature"),
        );
        let base = run_record("ba", vec![exec("a", 120_000, Some(100), Some(1024))], None);
        let params = (rocky_core::cost::WarehouseType::Databricks, 12.0, 0.55);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, Some(&params));
        assert_eq!(per_model.len(), 1);
        let m = &per_model[0];
        assert_eq!(m.model_name, "a");
        assert!(!m.skipped_via_copy);
        // Branch is half the duration, so cost is half. delta == branch - base < 0.
        let bcost = m.branch_cost_usd.unwrap();
        let pcost = m.base_cost_usd.unwrap();
        assert!(bcost > 0.0);
        assert!(pcost > bcost);
        let delta = m.delta_usd.unwrap();
        assert!(delta < 0.0);
        assert!((delta - (bcost - pcost)).abs() < 1e-9);
        assert_eq!(summary.models_skipped_via_copy, 0);
    }

    /// Models on base but not on branch → `skipped_via_copy = true` and
    /// `savings_from_copy_usd` accumulates; per-model `delta_usd` is None
    /// for the copied row (no comparison meaningful — the model didn't
    /// run on the branch).
    #[test]
    fn cost_delta_skipped_via_copy_rolls_into_savings() {
        let branch = run_record(
            "br",
            vec![exec("changed", 60_000, Some(50), Some(2048))],
            Some("feature"),
        );
        let base = run_record(
            "ba",
            vec![
                exec("changed", 80_000, Some(50), Some(2048)),
                exec("upstream", 30_000, Some(1000), Some(4096)),
            ],
            None,
        );
        let params = (rocky_core::cost::WarehouseType::Databricks, 12.0, 0.55);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, Some(&params));
        let upstream = per_model
            .iter()
            .find(|m| m.model_name == "upstream")
            .unwrap();
        assert!(upstream.skipped_via_copy);
        assert!(upstream.branch_cost_usd.is_none());
        assert!(upstream.base_cost_usd.is_some());
        assert!(upstream.delta_usd.is_none());
        assert_eq!(summary.models_skipped_via_copy, 1);
        // Savings == upstream's base cost.
        assert!(summary.savings_from_copy_usd.is_some());
        assert!(
            (summary.savings_from_copy_usd.unwrap() - upstream.base_cost_usd.unwrap()).abs() < 1e-9
        );
        // Total comparable base cost only counts the paired model.
        let changed = per_model
            .iter()
            .find(|m| m.model_name == "changed")
            .unwrap();
        assert!(
            (summary.total_base_cost_usd.unwrap() - changed.base_cost_usd.unwrap()).abs() < 1e-9
        );
    }

    /// Models on branch but not on base → new in PR; delta == branch
    /// cost (since base cost is 0 / None).
    #[test]
    fn cost_delta_branch_only_model_delta_eq_branch_cost() {
        let branch = run_record(
            "br",
            vec![exec("new_model", 30_000, Some(10), Some(512))],
            Some("feature"),
        );
        let base = run_record("ba", vec![], None);
        let params = (rocky_core::cost::WarehouseType::Databricks, 12.0, 0.55);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, Some(&params));
        assert_eq!(per_model.len(), 1);
        let m = &per_model[0];
        assert!(!m.skipped_via_copy);
        assert!(m.base_cost_usd.is_none());
        assert!(m.branch_cost_usd.is_some());
        assert!((m.delta_usd.unwrap() - m.branch_cost_usd.unwrap()).abs() < 1e-9);
        assert_eq!(summary.models_skipped_via_copy, 0);
    }

    /// Without cost params (config can't be loaded), every cost field
    /// is None — the surface still emits durations + bytes so the diff
    /// is non-empty.
    #[test]
    fn cost_delta_no_params_emits_durations_only() {
        let branch = run_record(
            "br",
            vec![exec("a", 60_000, Some(100), Some(1024))],
            Some("feature"),
        );
        let base = run_record("ba", vec![exec("a", 60_000, Some(100), Some(1024))], None);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, None);
        assert_eq!(per_model.len(), 1);
        let m = &per_model[0];
        assert!(m.branch_cost_usd.is_none());
        assert!(m.base_cost_usd.is_none());
        assert!(m.delta_usd.is_none());
        assert_eq!(m.branch_duration_ms, 60_000);
        assert!(summary.delta_usd.is_none());
    }

    /// DuckDB cost is always 0 — delta should be 0, not None.
    #[test]
    fn cost_delta_duckdb_always_zero() {
        let branch = run_record("br", vec![exec("a", 100, Some(50), None)], Some("feature"));
        let base = run_record("ba", vec![exec("a", 100, Some(50), None)], None);
        let params = (rocky_core::cost::WarehouseType::DuckDb, 0.0, 0.0);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, Some(&params));
        assert_eq!(per_model[0].branch_cost_usd, Some(0.0));
        assert_eq!(per_model[0].base_cost_usd, Some(0.0));
        assert_eq!(per_model[0].delta_usd, Some(0.0));
        assert_eq!(summary.delta_usd, Some(0.0));
    }

    /// Markdown rendering is deterministic and includes the headline
    /// metrics. Used by the Phase 4 GitHub Action.
    #[test]
    fn diff_markdown_renders_table() {
        let branch = run_record(
            "br",
            vec![
                exec("a", 100, Some(110), None),
                exec("b", 100, Some(50), None),
            ],
            Some("feature_x"),
        );
        let base = run_record(
            "ba",
            vec![
                exec("a", 100, Some(100), None),
                exec("b", 100, Some(60), None),
            ],
            None,
        );
        let (summary, models) = build_preview_diff(&branch, &base);
        let md = render_preview_diff_markdown("feature_x", "main", &summary, &models);
        assert!(md.contains("**Preview diff**"));
        assert!(md.contains("`feature_x`"));
        assert!(md.contains("vs `main`"));
        assert!(md.contains("| `a` |"));
        assert!(md.contains("| `b` |"));
        assert!(md.contains("not yet sampled"));
        assert!(md.contains("Phase 2.5"));
    }

    /// Empty diff path produces a "no paired runs" hint, not an empty
    /// markdown.
    #[test]
    fn diff_markdown_empty_path_explains_setup() {
        let summary = empty_diff_summary();
        let md = render_preview_diff_markdown("feature_x", "main", &summary, &[]);
        assert!(md.contains("No paired runs"));
        assert!(md.contains("rocky run --branch feature_x"));
    }

    // ---------------------- Phase 1.5 — copy-step execution ----------------------

    /// End-to-end smoke test for `execute_copy_from_base` on DuckDB. Drives
    /// the kernel through `AdapterRegistry::from_config` exactly the way the
    /// CLI does — proves the trait-driven CTAS works, the copy set lands in
    /// the branch schema, and partial-success handles a missing-in-base model.
    #[tokio::test]
    async fn copy_from_base_executes_ctas_on_duckdb() {
        use rocky_duckdb::DuckDbConnector;

        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("preview.duckdb");
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();

        // Set up a base schema with one materialised table + write a
        // matching model sidecar so the loader resolves it.
        {
            let conn = DuckDbConnector::open(&db_path).unwrap();
            conn.execute_statement(
                "CREATE SCHEMA IF NOT EXISTS demo;\
                 CREATE TABLE demo.raw_orders AS SELECT 1 AS id, 'a' AS name;",
            )
            .unwrap();
        }

        // Sidecar + .sql for a model whose source data exists in `demo`.
        std::fs::write(
            models_dir.join("raw_orders.sql"),
            "SELECT * FROM source.orders",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("raw_orders.toml"),
            "name = \"raw_orders\"\n[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"poc\"\nschema = \"demo\"\ntable = \"raw_orders\"\n",
        )
        .unwrap();
        // Sidecar for a model whose source DOESN'T exist — Phase 1.5
        // surfaces this as `copy_strategy = "failed"`, not a hard abort.
        std::fs::write(
            models_dir.join("missing_in_base.sql"),
            "SELECT * FROM source.missing",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("missing_in_base.toml"),
            "name = \"missing_in_base\"\n[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"poc\"\nschema = \"demo\"\ntable = \"missing_in_base\"\n",
        )
        .unwrap();

        let config_path = tmp.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                "[adapter.default]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.demo]\ntype = \"transformation\"\nmodels = \"models/**\"\n[pipeline.demo.target]\nadapter = \"default\"\n",
                db_path.display()
            ),
        )
        .unwrap();

        let copy_names = vec!["raw_orders".to_string(), "missing_in_base".to_string()];
        let result = execute_copy_from_base(&config_path, &models_dir, "branch__pr1", &copy_names)
            .await
            .expect("registry build + branch schema must succeed on DuckDB");
        assert_eq!(result.len(), 2);

        let by_name: HashMap<&str, &PreviewCopiedModel> =
            result.iter().map(|m| (m.model_name.as_str(), m)).collect();
        assert_eq!(by_name["raw_orders"].copy_strategy, "ctas");
        assert_eq!(by_name["raw_orders"].source_schema, "demo");
        assert_eq!(by_name["raw_orders"].target_schema, "branch__pr1");
        assert_eq!(by_name["missing_in_base"].copy_strategy, "failed");

        // Verify the actual table landed in the branch schema by
        // reading it back through DuckDbConnector.
        let conn = DuckDbConnector::open(&db_path).unwrap();
        let q = conn
            .execute_sql("SELECT id FROM branch__pr1.raw_orders")
            .unwrap();
        assert_eq!(q.rows.len(), 1);
        // execute_sql normalises every cell to a JSON String — the
        // numeric value `1` lands as `"1"`. Sufficient for "row exists".
        assert!(!q.columns.is_empty());
    }

    /// Empty copy-set short-circuits without touching the adapter — important
    /// for PRs where every working-DAG model is changed.
    #[tokio::test]
    async fn copy_from_base_empty_copy_set_short_circuits() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("rocky.toml"); // doesn't need to exist
        let models_dir = tmp.path().join("models");
        let result = execute_copy_from_base(&config_path, &models_dir, "branch__x", &[])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    /// Cost markdown includes the headline delta + per-model rows.
    #[test]
    fn cost_markdown_renders_table() {
        let branch = run_record(
            "br",
            vec![exec("a", 60_000, Some(100), None)],
            Some("feature"),
        );
        let base = run_record(
            "ba",
            vec![
                exec("a", 120_000, Some(100), None),
                exec("upstream", 30_000, Some(1000), None),
            ],
            None,
        );
        let params = (rocky_core::cost::WarehouseType::Databricks, 12.0, 0.55);
        let (summary, per_model) = build_preview_cost_delta(&branch, &base, Some(&params));
        let md = render_preview_cost_markdown("feature", &summary, &per_model);
        assert!(md.contains("**Preview cost**"));
        assert!(md.contains("`feature`"));
        assert!(md.contains("Δ vs base"));
        assert!(md.contains("| `a` |"));
        assert!(md.contains("| `upstream` |"));
        assert!(md.contains("1 model(s) skipped via copy"));
    }
}
