/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/plan.schema.json
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
 * Severity level of a diagnostic.
 *
 * Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay compatible with existing dagster fixtures and the hand-written `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
 */
export type Severity = "Error" | "Warning" | "Info";

/**
 * JSON output for `rocky plan`.
 *
 * `statements` enumerates the warehouse SQL Rocky would emit. The three `*_actions` collections are a parallel view of the control-plane governance work `rocky run` would do *after* a successful DAG — the classification / masking / retention reconcile pass. These never show up as SQL; they fire through [`rocky_core::traits::GovernanceAdapter`] methods (e.g. `apply_column_tags`, `apply_masking_policy`, `apply_retention_policy`). Projects without any `[classification]`, `[mask]`, or `retention` config get empty lists — the fields `skip_serializing_if = Vec::is_empty`, so JSON consumers written against the earlier shape are byte-stable.
 *
 * ## Plan-persistence additions
 *
 * `plan_id`, `plan_kind`, `created_at`, `models`, and `execution_layers` are additive — all have `skip_serializing_if` so existing fixtures and consumers that do not include a compile step remain byte-stable. When `rocky plan` runs against a project with a `models/` directory, these fields are populated and the plan is persisted to `.rocky/plans/`.
 */
export interface PlanOutput {
  /**
   * Semantic change-impact verdict from the typed-IR breaking-change classifier, surfaced as decision-support at plan time. Present only when `--semantic` ran with a usable baseline. See [`SemanticPlanVerdict`] — note its `caveat`: the classifier diffs OUTPUT SCHEMA only and is blind to schema-stable value changes.
   */
  breaking_verdict?: SemanticPlanVerdict | null;
  /**
   * Per-model E027 budget-exceeded diagnostics produced at plan time using real catalog statistics. Severity reflects the model's `on_breach` policy (`"warn"` or `"error"`). Empty when no ceiling was exceeded or no real stats were available.
   */
  budget_diagnostics?: Diagnostic[];
  /**
   * Column-tag applications the governance reconciler would issue via `apply_column_tags`. One row per `(model, column, tag)` triple declared in a model sidecar's `[classification]` block.
   */
  classification_actions?: ClassificationAction[];
  command: string;
  /**
   * UTC timestamp when the plan was persisted. Present when `plan_id` is present.
   */
  created_at?: string | null;
  /**
   * Environment name passed via `--env <name>`. Propagates to `mask_actions` so the preview resolves `[mask.<env>]` overrides on top of the workspace `[mask]` defaults. `None` when the flag is absent — preview resolves against defaults only.
   */
  env?: string | null;
  /**
   * Execution layers (topological order) as a list-of-lists of model names. Models within a layer can execute concurrently. Informational — re-derived at apply time. Empty for replication-only plans.
   */
  execution_layers?: string[][];
  filter: string;
  /**
   * `true` when at least one entry in `budget_diagnostics` has error-level severity (`on_breach = "error"`). Callers can use this flag to fail a pipeline-as-code check without inspecting individual diagnostic severities.
   */
  has_budget_errors?: boolean;
  /**
   * Masking-policy applications the governance reconciler would issue via `apply_masking_policy`. One row per `(model, column, tag)` where the tag resolves to a strategy for the active env. Unresolved tags are intentionally omitted — `rocky compliance` is the diagnostic surface for that gap.
   */
  mask_actions?: MaskAction[];
  /**
   * Qualified model names that will be executed by `rocky apply`. Empty for replication-only plans. Informational — re-derived at apply time.
   */
  models?: string[];
  /**
   * Full 64-char blake3 plan identifier. Present when the plan was persisted to `.rocky/plans/<plan_id>.json`. Apply with: `rocky apply <plan_id>`.
   */
  plan_id?: string | null;
  /**
   * Plan kind wire name (`"run"`). Present when `plan_id` is present.
   */
  plan_kind?: string | null;
  /**
   * Retention-policy applications the governance reconciler would issue via `apply_retention_policy`. One row per model whose sidecar declares `retention = "<N>[dy]"`. `warehouse_preview` shows the warehouse-native SQL that the current adapter would compile the policy to (Databricks / Snowflake); `None` on warehouses without a first-class retention knob.
   */
  retention_actions?: RetentionAction[];
  statements: PlannedStatement[];
  version: string;
  [k: string]: unknown;
}
/**
 * Decision-support verdict from the typed-IR breaking-change classifier, attached to `PlanOutput` when `rocky plan --semantic` runs against a usable baseline.
 *
 * # Scope and blindness
 *
 * The classifier diffs the typed **output schema** of each model between the baseline git ref and the working tree (column drop/add/type narrowing, nullability, materialization keys, masks, target rename). It is **blind to schema-stable value changes**: a `WHERE` / `JOIN`-key / `CASE` rewrite that changes every output row but leaves the column list and types untouched produces **no finding**. An empty `findings` list therefore means "no output-schema change was detected" — it is **not** a completeness or safety signal that the data is unchanged. The [`Self::caveat`] field carries this statement verbatim so a consumer that only reads the JSON cannot miss it.
 *
 * # Reporting-only
 *
 * This verdict never gates the plan. Even a `breaking`-severity finding leaves the planned statements and the process exit code unchanged. The hard gate (which blocks on `breaking`) lives on `rocky plan promote`.
 */
export interface SemanticPlanVerdict {
  /**
   * The git ref the working tree was diffed against (the `--base` value, default `"main"`).
   */
  base_ref: string;
  /**
   * Verbatim statement of what the classifier does and does not see. Always populated — present even when `findings` is empty, because "no findings" is the case most easily misread as "safe". See the type-level docs for why this is load-bearing.
   */
  caveat: string;
  /**
   * Classified output-schema findings (one per detected change), including `info`-severity entries. Each finding carries its own `model`, tagged `change.kind`, and `severity` (`breaking` / `warning` / `info`). Empty when no output-schema change was detected — which, per `caveat`, is not a safety signal.
   */
  findings: BreakingFinding[];
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
 * A compiler diagnostic (error, warning, or informational message).
 *
 * `code` and `message` use `Arc<str>` (§P3.5) — cloning a `Diagnostic` in the LSP publish loop becomes a refcount bump. Construction still accepts any `Into<String>` / `&str` via the helper constructors below; the arc wrap happens once at construction time.
 */
export interface Diagnostic {
  /**
   * Diagnostic code (e.g., "E001", "W001").
   */
  code: string;
  /**
   * Human-readable message.
   */
  message: string;
  /**
   * Which model this diagnostic relates to.
   */
  model: string;
  /**
   * Severity level.
   */
  severity: Severity;
  /**
   * Source location (if available).
   */
  span?: SourceSpan | null;
  /**
   * Suggested fix (if any).
   */
  suggestion?: string | null;
  [k: string]: unknown;
}
/**
 * Location in a source file.
 */
export interface SourceSpan {
  col: number;
  file: string;
  line: number;
  [k: string]: unknown;
}
/**
 * Classification-tag application row in `PlanOutput.classification_actions`.
 */
export interface ClassificationAction {
  /**
   * Column name the tag will be applied to.
   */
  column: string;
  /**
   * Model name the action targets.
   */
  model: string;
  /**
   * Free-form classification tag (e.g. `"pii"`, `"confidential"`).
   */
  tag: string;
  [k: string]: unknown;
}
/**
 * Masking-policy application row in `PlanOutput.mask_actions`.
 */
export interface MaskAction {
  /**
   * Column name the mask will be applied to.
   */
  column: string;
  /**
   * Model name the action targets.
   */
  model: string;
  /**
   * Wire name of the resolved strategy (`"hash"`, `"redact"`, `"partial"`, `"none"`). Matches `MaskStrategy::as_str`.
   */
  resolved_strategy: string;
  /**
   * Classification tag the mask is resolved against.
   */
  tag: string;
  [k: string]: unknown;
}
/**
 * Retention-policy application row in `PlanOutput.retention_actions`.
 */
export interface RetentionAction {
  /**
   * Retention duration parsed from the sidecar (`"90d"` → 90, `"1y"` → 365). Flat day count — no leap-year semantics.
   */
  duration_days: number;
  /**
   * Model name the action targets.
   */
  model: string;
  /**
   * Warehouse-native preview of the SQL / TBLPROPERTIES Rocky would issue for this model on the active adapter. `None` on warehouses that don't support a first-class retention knob (BigQuery, DuckDB).
   */
  warehouse_preview?: string | null;
  [k: string]: unknown;
}
export interface PlannedStatement {
  purpose: string;
  sql: string;
  target: string;
  [k: string]: unknown;
}
