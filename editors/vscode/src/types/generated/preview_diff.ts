/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/preview_diff.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky preview diff`.
 *
 * Combines the structural diff (column added/removed/type-changed) from the existing `rocky_core::ci_diff` machinery with a sampled row-level diff that extends the `rocky_core::compare`/`shadow` kernel.
 *
 * **Sampling correctness ceiling.** Phase 2 sampling reads `LIMIT N` rows ordered by primary key (or first column). Changes outside that window appear as no-change unless the row count itself differs. `sampling_window.coverage_warning = true` flags this risk on the per-model level so reviewers don't infer false coverage. A checksum-bisection exhaustive diff is the Phase 2.5 lift.
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
 * Per-model diff. Combines structural (column-level) and sampled (row-level) findings.
 */
export interface PreviewModelDiff {
  model_name: string;
  sampled: PreviewSampledRowDiff;
  sampling_window: PreviewSamplingWindow;
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
   * One of `"first_n_by_order"` (Phase 2) or `"checksum_bisection"` (Phase 2.5 lift, not yet shipped).
   */
  coverage: string;
  coverage_warning: boolean;
  limit: number;
  ordered_by: string;
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
   * `true` if **any** per-model diff carries `sampling_window.coverage_warning = true`.
   */
  any_coverage_warning: boolean;
  models_unchanged: number;
  models_with_changes: number;
  total_rows_added: number;
  total_rows_changed: number;
  total_rows_removed: number;
  [k: string]: unknown;
}
