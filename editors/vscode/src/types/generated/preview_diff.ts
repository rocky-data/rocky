/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/preview_diff.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Row-level diff payload tagged by which algorithm produced it. The `kind` discriminator (`"sampled"` or `"bisection"`) lets consumers reading the JSON pick the right shape without having to check optional fields.
 */
export type PreviewModelDiffAlgorithm =
  | {
      kind: "sampled";
      sampled: PreviewSampledRowDiff;
      sampling_window: PreviewSamplingWindow;
      [k: string]: unknown;
    }
  | {
      bisection_stats: BisectionStatsOutput;
      diff: PreviewBisectionRowDiff;
      kind: "bisection";
      [k: string]: unknown;
    };

/**
 * JSON output for `rocky preview diff`.
 *
 * Combines the structural diff (column added/removed/type-changed) from the existing `rocky_core::ci_diff` machinery with a row-level diff produced by either the sampled or the checksum-bisection algorithm. Each per-model entry's `algorithm` field carries a `kind` discriminator (`"sampled"` or `"bisection"`) plus the matching payload.
 *
 * **Sampled correctness ceiling.** The sampled algorithm reads `LIMIT N` rows ordered by primary key (or first column). Changes outside that window appear as no-change unless the row count itself differs; `sampling_window.coverage_warning = true` flags that risk. **Bisection** walks the chunk lattice exhaustively over a single-column primary key; `bisection_stats.depth_capped = true` flags the rare case where the recursion bottomed out at the depth cap before reaching leaf size. `summary.any_coverage_warning` rolls both signals up.
 */
export interface PreviewDiffOutput {
  base_ref: string;
  branch_name: string;
  command: string;
  /**
   * Pre-rendered Markdown suitable for posting as a GitHub PR comment.
   */
  markdown: string;
  models: PreviewModelDiff[];
  summary: PreviewDiffSummary;
  version: string;
  [k: string]: unknown;
}
/**
 * Per-model diff. Combines a structural (column-level) delta with a row-level diff that was produced by either the sampled or the checksum-bisection algorithm. The active algorithm is encoded by the `kind` discriminator on [`PreviewModelDiffAlgorithm`].
 */
export interface PreviewModelDiff {
  /**
   * Row-level diff payload, tagged by which algorithm produced it. Different models in the same run can carry different variants: a model with a single-column integer primary key runs through the bisection kernel, while a model that lacks a usable PK falls back to the sampled algorithm.
   */
  algorithm: PreviewModelDiffAlgorithm;
  model_name: string;
  structural: PreviewStructuralDiff;
  [k: string]: unknown;
}
/**
 * Sampled row-level diff. All counts are over the sampling window.
 */
export interface PreviewSampledRowDiff {
  rows_added: number;
  rows_changed: number;
  rows_removed: number;
  /**
   * Up to `--max-samples` (default 5) representative changed rows for human review. Pure noise when sampling found no change.
   */
  samples: PreviewRowSample[];
  [k: string]: unknown;
}
/**
 * One representative changed row in [`PreviewSampledRowDiff::samples`].
 */
export interface PreviewRowSample {
  /**
   * Pairs of `(column_name, base_value, branch_value)` for every column that differs on this row.
   */
  changes: PreviewRowSampleChange[];
  primary_key: string;
  [k: string]: unknown;
}
/**
 * One `(column, base, branch)` triple in [`PreviewRowSample::changes`].
 */
export interface PreviewRowSampleChange {
  base_value: string;
  branch_value: string;
  column: string;
  [k: string]: unknown;
}
/**
 * Sampling-window metadata surfaced verbatim in the PR comment.
 *
 * `coverage_warning = true` flags that the row count outside the sampling window is non-trivial; reviewers should not infer "no change" from a clean sample if this flag is set.
 */
export interface PreviewSamplingWindow {
  /**
   * Sampling-strategy tag. `"first_n_by_order"` is the standard PK-ordered window; `"not_yet_sampled"` is a placeholder used when the sampling layer didn't run (e.g. the structural-only fallback path).
   */
  coverage: string;
  coverage_warning: boolean;
  limit: number;
  ordered_by: string;
  [k: string]: unknown;
}
/**
 * CLI-side mirror of [`rocky_core::compare::bisection::BisectionStats`]. Kept separate so the JSON schema lives alongside the rest of the `rocky preview diff` output types — same pattern as [`BudgetBreachOutput`] mirroring `rocky_core::config::BudgetBreach`.
 */
export interface BisectionStatsOutput {
  /**
   * Total non-empty chunk-checksum entries returned across both sides and all recursion levels.
   */
  chunks_examined: number;
  /**
   * `true` if the runner hit the `max_depth` cap before any chunk fell below the leaf threshold. The dense range was still materialized exhaustively, so the diff is correct — just slower.
   */
  depth_capped: boolean;
  /**
   * Maximum recursion depth reached.
   */
  depth_max: number;
  /**
   * Number of leaf chunks materialized for row-by-row diff.
   */
  leaves_materialized: number;
  /**
   * Rows on the base side whose primary-key column was NULL. Excluded from chunk membership; counted at the root so a null-PK divergence surfaces instead of silently dropping rows.
   */
  null_pk_rows_base: number;
  /**
   * Rows on the branch side whose primary-key column was NULL.
   */
  null_pk_rows_branch: number;
  /**
   * How the runner split the primary-key space into chunks. One of `"int_range"`, `"composite"`, `"hash_bucket"`, `"first_column"`.
   */
  split_strategy: string;
  [k: string]: unknown;
}
/**
 * Row-level diff produced by the checksum-bisection algorithm. All counts are exhaustive over the diffed table — no sampling window.
 */
export interface PreviewBisectionRowDiff {
  rows_added: number;
  rows_changed: number;
  rows_removed: number;
  /**
   * Up to `--max-samples` (default 5) representative changed rows surfaced from the leaves. Bisection samples only carry the primary key — column-level diffs are not retained on the kernel's leaf record. Empty when no rows differ.
   */
  samples: PreviewRowSample[];
  [k: string]: unknown;
}
/**
 * Column-level structural diff. Mirrors the shape produced by `rocky ci-diff` at the column granularity.
 */
export interface PreviewStructuralDiff {
  added_columns: string[];
  removed_columns: string[];
  /**
   * One entry per column whose type changed. Each carries `name`, `from`, `to`.
   */
  type_changes: PreviewColumnTypeChange[];
  [k: string]: unknown;
}
/**
 * One column-type change entry.
 */
export interface PreviewColumnTypeChange {
  from: string;
  name: string;
  to: string;
  [k: string]: unknown;
}
/**
 * Aggregate diff counts for [`PreviewDiffOutput`].
 */
export interface PreviewDiffSummary {
  /**
   * `true` if **any** per-model diff is either a sampled diff with `sampling_window.coverage_warning = true` or a bisection diff with `bisection_stats.depth_capped = true`. Both conditions indicate the row-level findings might be incomplete and a reviewer shouldn't infer "no change" from a clean result.
   */
  any_coverage_warning: boolean;
  models_unchanged: number;
  models_with_changes: number;
  total_rows_added: number;
  total_rows_changed: number;
  total_rows_removed: number;
  [k: string]: unknown;
}
