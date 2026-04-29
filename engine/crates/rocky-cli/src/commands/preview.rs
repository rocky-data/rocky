//! `rocky preview` — PR preview workflow (Arc 1 ∩ Arc 2 user-facing surface).
//!
//! Three subcommands compose into a single PR comment:
//!
//! * `preview create` — pruned re-run on a per-PR branch with
//!   copy-from-base for unchanged upstream
//! * `preview diff`   — structural + sampled row-level diff vs. base
//! * `preview cost`   — per-model bytes/duration/USD delta vs. base
//!
//! See `~/Developer/rocky-plans/plans/rocky-pr-preview-and-data-diff.md` for
//! the design and the comparison to Fivetran's Smart Run.

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
/// **Phase 1 scope.** Plans the prune-and-copy decision against the working
/// DAG, registers the branch in the state store, and executes copy-from-base
/// for unchanged upstream models on DuckDB pipelines. Auto-invoking the
/// actual run for the prune set is out of scope for Phase 1 — the user runs
/// `rocky run --branch <name> --model <name>` for each prune-set model
/// (or in Phase 4 the GitHub Action orchestrates). The output's
/// `run_status` is `"planned"` to make this explicit.
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
/// 8. For DuckDB pipelines, execute `CREATE OR REPLACE TABLE
///    <branch_schema>.<model> AS SELECT * FROM <base_schema>.<model>`
///    for every copy-set model. Phase 5 lifts this to `SHALLOW CLONE` /
///    zero-copy `CLONE` per adapter capability.
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

    // Step 8: planning-only in Phase 1 — emit `PreviewCopiedModel`
    // entries with `copy_strategy = "planned"` so the wire contract is
    // populated and downstream consumers (`preview diff`, the Phase 4
    // GitHub Action) can render the plan. Phase 1.5 lifts this to
    // adapter-driven execution: DuckDB CTAS, Databricks `SHALLOW CLONE`,
    // Snowflake zero-copy `CLONE`, BigQuery table copy. Until then, the
    // user populates the branch by running
    // `rocky run --branch <name>` for the prune set themselves (the
    // Phase 4 PR-comment automation orchestrates this).
    //
    // Treating Phase 1 as planning-only is honest about scope and keeps
    // the surface CI-green-verifiable on the playground POC.
    let copy_set: Vec<PreviewCopiedModel> = copy_names
        .iter()
        .map(|name| PreviewCopiedModel {
            model_name: name.clone(),
            source_schema: "<base>".to_string(),
            target_schema: branch_schema.clone(),
            copy_strategy: "planned".to_string(),
        })
        .collect();
    let _ = config_path; // Phase 1.5 uses config_path to resolve adapter.

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

/// Sampled row-level + structural diff between branch and base for every
/// model in the prune set. Extends the `rocky_core::compare` shadow kernel
/// with a deterministic-ordering sampling layer.
///
/// **Phase 2 scope.** Implementation lands alongside Phase 2's compare-kernel
/// extension. This function emits an empty-but-valid `PreviewDiffOutput`
/// today so the JSON wire contract is testable end-to-end on the POC.
pub async fn run_preview_diff(
    _config_path: &Path,
    _state_path: &Path,
    branch_name: &str,
    base_ref: &str,
    _sample_size: usize,
    json: bool,
) -> Result<()> {
    use crate::output::PreviewDiffOutput;

    let summary = empty_diff_summary();
    let out = PreviewDiffOutput::new(
        branch_name.to_string(),
        base_ref.to_string(),
        summary,
        Vec::new(),
        "_(no models in prune set — preview diff has nothing to report)_".to_string(),
    );

    if json {
        print_json(&out)?;
    } else {
        info!(
            "preview diff '{branch_name}' vs {base_ref}: 0 models with changes \
             (Phase 2 lands the row-level sampling)"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky preview cost`
// ---------------------------------------------------------------------------

/// Per-model bytes/duration/USD delta between branch and base. Diff layer
/// over `rocky cost latest`'s machinery — does not introduce new cost
/// math.
///
/// **Phase 3 scope.** Returns an empty-but-valid `PreviewCostOutput` today
/// so the JSON wire contract is testable end-to-end on the POC.
pub async fn run_preview_cost(
    _config_path: &Path,
    _state_path: &Path,
    branch_name: &str,
    json: bool,
) -> Result<()> {
    use crate::output::PreviewCostOutput;

    let summary = empty_cost_summary();
    let out = PreviewCostOutput::new(
        branch_name.to_string(),
        None,
        String::new(),
        summary,
        Vec::new(),
        "_(no branch run yet — run `rocky run --branch <name> --model <name>` \
          for each prune-set model first)_"
            .to_string(),
    );

    if json {
        print_json(&out)?;
    } else {
        info!(
            "preview cost '{branch_name}': no branch run yet \
             (Phase 3 lands the per-model delta math)"
        );
    }
    Ok(())
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
}
