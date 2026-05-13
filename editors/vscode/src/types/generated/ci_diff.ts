/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ci_diff.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * A single typed semantic change between two `ProjectIr` snapshots.
 *
 * Each variant carries the minimum identifying context (model + column + before/after values) needed for a CLI / PR-preview surface to render a useful message without re-loading either IR.
 */
export type BreakingChange =
  | {
      kind: "model_removed";
      model: string;
      [k: string]: unknown;
    }
  | {
      kind: "model_added";
      model: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      data_type: string;
      kind: "column_dropped";
      model: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      data_type: string;
      kind: "column_added";
      model: string;
      nullable: boolean;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_type_changed";
      model: string;
      /**
       * `true` when the new type cannot represent every value of the old type (Int64 → Int32, Decimal precision shrink, Timestamp → Date).
       */
      narrowing: boolean;
      new_type: string;
      old_type: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_nullability_changed";
      model: string;
      new_nullable: boolean;
      old_nullable: boolean;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_reordered";
      model: string;
      new_index: number;
      old_index: number;
      [k: string]: unknown;
    }
  | {
      kind: "materialization_strategy_changed";
      model: string;
      new_strategy: string;
      old_strategy: string;
      [k: string]: unknown;
    }
  | {
      /**
       * Which key changed: `unique_key`, `timestamp_column`, `partition_by`, `time_column`, `granularity`, `target_lag`, `update_columns`, `storage_prefix`, or `partition_columns`.
       */
      key_kind: string;
      kind: "materialization_key_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "replication_columns_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "partition_by_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "target_renamed";
      model: string;
      new: string;
      old: string;
      [k: string]: unknown;
    }
  | {
      kind: "source_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_mask_changed";
      model: string;
      new_strategy?: string | null;
      old_strategy?: string | null;
      [k: string]: unknown;
    }
  | {
      kind: "lakehouse_format_changed";
      model: string;
      new: string;
      old: string;
      [k: string]: unknown;
    }
  | {
      kind: "sql_body_changed";
      model: string;
      [k: string]: unknown;
    };
/**
 * Severity classification for a single semantic change.
 */
export type BreakingSeverity = "breaking" | "warning" | "info";
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
  /**
   * Classified semantic findings from the typed-IR breaking-change classifier. Only populated when `ci-diff` is invoked with `--semantic` and both base + HEAD compiles succeed; omitted from JSON output when empty.
   */
  breaking_findings?: BreakingFinding[];
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
 * A classified finding produced by [`diff_project_ir`].
 */
export interface BreakingFinding {
  change: BreakingChange;
  severity: BreakingSeverity;
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
