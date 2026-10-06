/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/lineage_diff.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Which code snapshot `rocky ci-diff` / `rocky lineage-diff` compares against the base.
 *
 * Selection (which files changed) and compilation (what those files contain) always read the same snapshot, so the report never mixes a committed file list with uncommitted contents.
 */
export type CiDiffMode = "head" | "working_tree";
/**
 * The kind of change observed for a single column.
 */
export type ColumnChangeType = "added" | "removed" | "type_changed";
/**
 * What happened to a consumer of a removed column.
 */
export type ConsumerImpactStatus = "newly_broken" | "unknown" | "deleted" | "repaired";
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
 * `downstream_consumers` is traced **downstream from HEAD**, so a removed column reports an empty set there. Removed columns instead carry `consumer_impact`: each direct consumer on the base side (or HEAD side), classified by comparing the base and HEAD lineage graphs.
 */
export interface LineageDiffOutput {
  /**
   * Commit the base side was read from (merge base of `base_ref` and HEAD). Omitted when it could not be computed.
   */
  base_commit?: string | null;
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
   * Which snapshot was compared against the base. See [`CiDiffMode`].
   */
  mode: CiDiffMode;
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
   * For a removed (or renamed-away) column: what happened to each model that read it directly, found by comparing the base and HEAD lineage graphs. Includes reads through value lineage and through row selection (join keys, filters, group keys, window keys). Omitted for other change types and when no consumer was found.
   */
  consumer_impact?: LineageConsumerImpact[];
  /**
   * Columns reached by walking the lineage graph downstream from `(model_name, column_name)` on HEAD's compile. Empty when the column no longer exists on HEAD (e.g. for removed columns) or when the trace finds no consumers.
   */
  downstream_consumers?: LineageQualifiedColumn[];
  new_type?: string | null;
  old_type?: string | null;
  [k: string]: unknown;
}
/**
 * One direct consumer of a removed column, classified.
 */
export interface LineageConsumerImpact {
  /**
   * Consumer output columns involved: the HEAD-side columns for `newly_broken`, the base-side columns otherwise. Empty when the read affects every column (a filter or join key).
   */
  columns?: string[];
  /**
   * The consumer model.
   */
  model: string;
  /**
   * One-line, human-readable explanation of the classification.
   */
  reason: string;
  status: ConsumerImpactStatus;
  /**
   * How the consumer reads the column: `value`, or a row-selection kind (`join_key`, `filter`, `group_by`, `having`, `qualify`, `window_partition`, `window_order`).
   */
  via?: string[];
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
