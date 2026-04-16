/* eslint-disable */

/**
 * The kind of change observed for a single column.
 */
export type ColumnChangeType = "added" | "removed" | "type_changed";
/**
 * Status of a single model in the diff.
 */
export type ModelDiffStatus = "unchanged" | "modified" | "added" | "removed";

/**
 * JSON output for `rocky ci-diff`.
 *
 * Reports which models changed between two git refs, with optional column-level structural diffs when compilation succeeds on both sides.
 */
export interface CiDiffOutput {
  /**
   * Git ref used as the comparison base (e.g. `main`).
   */
  base_ref: string;
  command: string;
  /**
   * Git ref for the incoming changes (typically `HEAD`).
   */
  head_ref: string;
  /**
   * Pre-rendered Markdown suitable for posting as a GitHub PR comment.
   */
  markdown: string;
  /**
   * Per-model diff results.
   */
  models: DiffResult[];
  /**
   * Aggregate counts of changed models.
   */
  summary: DiffSummary;
  version: string;
  [k: string]: unknown;
}
/**
 * Diff result for a single model between two pipeline states.
 */
export interface DiffResult {
  /**
   * Column-level changes (empty for unchanged models).
   */
  column_changes?: ColumnDiff[];
  /**
   * Fully-qualified model name (e.g. `catalog.schema.table`).
   */
  model_name: string;
  /**
   * Row count on the incoming (PR) side. `None` for removed models.
   */
  row_count_after?: number | null;
  /**
   * Row count on the base (target branch) side. `None` for added models.
   */
  row_count_before?: number | null;
  /**
   * Optional sample of changed rows for human review.
   */
  sample_changed_rows?: string[][] | null;
  /**
   * High-level status.
   */
  status: ModelDiffStatus;
  [k: string]: unknown;
}
/**
 * A single column-level difference within a model.
 */
export interface ColumnDiff {
  /**
   * Kind of change.
   */
  change_type: ColumnChangeType;
  /**
   * Column name.
   */
  column_name: string;
  /**
   * New data type (empty for removed columns).
   */
  new_type?: string | null;
  /**
   * Previous data type (empty for added columns).
   */
  old_type?: string | null;
  [k: string]: unknown;
}
/**
 * High-level summary across all models in a diff run.
 */
export interface DiffSummary {
  added: number;
  modified: number;
  removed: number;
  total_models: number;
  unchanged: number;
  [k: string]: unknown;
}
