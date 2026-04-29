/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/lineage_diff.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The kind of change observed for a single column.
 */
export type ColumnChangeType = "added" | "removed" | "type_changed";
/**
 * Status of a single model in the diff.
 */
export type ModelDiffStatus = "unchanged" | "modified" | "added" | "removed";

/**
 * JSON output for `rocky lineage-diff <base_ref>`.
 *
 * Combines the structural per-column diff produced by `rocky ci-diff` (added/removed/type-changed columns between two git refs) with the downstream blast-radius from `rocky lineage --downstream` (consumers of each changed column, traced through HEAD's semantic graph).
 *
 * The markdown payload is rendered server-side and ready to drop into a PR comment — answers "what does this PR change downstream?" in one command.
 *
 * Trace direction is fixed to **downstream from HEAD only** in v1. Removed columns therefore report an empty consumer set (the column no longer exists on HEAD's compile, so its downstream reach can't be walked); the structural diff still surfaces the removal.
 */
export interface LineageDiffOutput {
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
   * Per-changed-model entries, each with per-column downstream traces.
   */
  results: LineageDiffResult[];
  summary: DiffSummary;
  version: string;
  [k: string]: unknown;
}
/**
 * One model's worth of structural + lineage diff.
 */
export interface LineageDiffResult {
  column_changes: LineageColumnChange[];
  model_name: string;
  status: ModelDiffStatus;
  [k: string]: unknown;
}
/**
 * Per-column structural change augmented with downstream consumers.
 */
export interface LineageColumnChange {
  change_type: ColumnChangeType;
  column_name: string;
  /**
   * Columns reached by walking the lineage graph downstream from `(model_name, column_name)` on HEAD's compile. Empty when the column no longer exists on HEAD (e.g. for removed columns) or when the trace finds no consumers.
   */
  downstream_consumers?: LineageQualifiedColumn[];
  new_type?: string | null;
  old_type?: string | null;
  [k: string]: unknown;
}
export interface LineageQualifiedColumn {
  column: string;
  model: string;
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
