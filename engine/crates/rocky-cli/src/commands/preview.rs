//! `rocky preview` — PR preview workflow (Arc 1 ∩ Arc 2 user-facing surface).
//!
//! Three subcommands compose into a single PR comment:
//!
//! * `preview create` — pruned re-run on a per-PR branch with copy-from-base
//!                      for unchanged upstream
//! * `preview diff`   — structural + sampled row-level diff vs. base
//! * `preview cost`   — per-model bytes/duration/USD delta vs. base
//!
//! See `~/Developer/rocky-plans/plans/rocky-pr-preview-and-data-diff.md` for
//! the design.

use std::path::Path;

use anyhow::Result;

use crate::output::{PreviewCostSummary, PreviewCreateOutput, PreviewDiffSummary};

// ---------------------------------------------------------------------------
// `rocky preview create`
// ---------------------------------------------------------------------------

/// Orchestrate pruned re-run on a per-PR branch with copy-from-base for
/// unchanged upstream.
///
/// Steps (Phase 1 model-level pruning):
///
/// 1. Run `git diff --name-only <base>...HEAD` against the models dir to
///    identify changed model files (reuses the `rocky_core::ci_diff`
///    plumbing).
/// 2. Resolve the change-set into model names.
/// 3. Compute the prune set: every changed model + every model that
///    transitively depends on a changed model in the working DAG.
/// 4. Compute the copy set: every working-DAG model not in the prune set.
/// 5. Register a branch via the state store (mirrors `rocky branch create`).
/// 6. For each copy-set model, execute `CREATE TABLE <branch_schema>.<model>
///    AS SELECT * FROM <base_schema>.<model>` against the configured
///    adapter (CTAS in Phase 1; lifted to `SHALLOW CLONE` / zero-copy
///    `CLONE` in Phase 5 per adapter capability).
/// 7. Invoke `rocky run --branch <name>` with a model selector limited to
///    the prune set.
///
/// Returns a [`PreviewCreateOutput`] regardless of run status — partial
/// failures still produce a usable summary so the PR comment stays
/// informative.
#[allow(clippy::too_many_arguments)]
pub async fn run_preview_create(
    _config_path: &Path,
    _state_path: &Path,
    _models_dir: &Path,
    _base_ref: &str,
    _branch_name: Option<&str>,
    _json: bool,
) -> Result<()> {
    // Phase 0a stub. Phase 1 fills this in.
    anyhow::bail!(
        "rocky preview create: implementation lands in Phase 1 of the preview \
         bundle (see plans/rocky-pr-preview-and-data-diff.md). The output \
         struct, schema, and dispatch are wired so the rest of the bundle \
         can be reviewed alongside this scaffolding."
    )
}

// ---------------------------------------------------------------------------
// `rocky preview diff`
// ---------------------------------------------------------------------------

/// Sampled row-level + structural diff between branch and base for every
/// model in the prune set. Extends the `rocky_core::compare` shadow kernel
/// with a deterministic-ordering sampling layer.
pub async fn run_preview_diff(
    _config_path: &Path,
    _state_path: &Path,
    _branch_name: &str,
    _base_ref: &str,
    _sample_size: usize,
    _json: bool,
) -> Result<()> {
    anyhow::bail!(
        "rocky preview diff: implementation lands in Phase 2 of the preview \
         bundle. Output schema is wired."
    )
}

// ---------------------------------------------------------------------------
// `rocky preview cost`
// ---------------------------------------------------------------------------

/// Per-model bytes/duration/USD delta between branch and base. Diff layer
/// over `rocky cost latest`'s machinery — does not introduce new cost
/// math.
pub async fn run_preview_cost(
    _config_path: &Path,
    _state_path: &Path,
    _branch_name: &str,
    _json: bool,
) -> Result<()> {
    anyhow::bail!(
        "rocky preview cost: implementation lands in Phase 3 of the preview \
         bundle. Output schema is wired."
    )
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
        PreviewCopiedModel, PreviewModelCostDelta, PreviewModelDiff, PreviewPrunedModel,
        PreviewRowSample, PreviewRowSampleChange, PreviewSampledRowDiff, PreviewSamplingWindow,
        PreviewStructuralDiff,
    };

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
}
