/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/review.schema.json
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
 * JSON output for `rocky review <plan-id>`.
 *
 * `rocky review` is the human sign-off gate for an AI-authored plan. It compares the working-tree models against `base_ref`, classifies the semantic delta, and either reports the findings (dry run) or — with `--approve` — writes a review marker that unblocks `rocky apply`.
 *
 * The marker is written even when breaking changes exist, on the premise that the human approving has seen this report and is signing off on them explicitly. `breaking_changes` therefore always lists the full classified delta so the approval is informed.
 */
export interface ReviewOutput {
  /**
   * True when this invocation wrote the approval marker (i.e. `--approve` was set). When false, the review was a dry run and `rocky apply` stays blocked.
   */
  approved: boolean;
  /**
   * Git ref the working tree was compared against (default `HEAD`).
   */
  base_ref: string;
  /**
   * Semantic breaking-change findings between `base_ref` and the working tree. Empty when the classifier ran and found no breaking changes; absent when the gate was skipped (compile failure on either side, or the models directory was unavailable).
   */
  breaking_changes?: BreakingFinding[] | null;
  command: string;
  /**
   * True when the approval marker is now present on disk as a result of this invocation. Mirrors `approved` today; kept distinct so callers reading the JSON do not have to infer marker state from the flag.
   */
  marker_written: boolean;
  /**
   * Human-readable summary of the review outcome.
   */
  message?: string | null;
  /**
   * The AI-authored plan being reviewed (64-char blake3 hex).
   */
  plan_id: string;
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
