/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/plan.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky plan`.
 *
 * `statements` enumerates the warehouse SQL Rocky would emit. The three `*_actions` collections are a parallel view of the control-plane governance work `rocky run` would do *after* a successful DAG — the classification / masking / retention reconcile pass. These never show up as SQL; they fire through [`rocky_core::traits::GovernanceAdapter`] methods (e.g. `apply_column_tags`, `apply_masking_policy`, `apply_retention_policy`). Projects without any `[classification]`, `[mask]`, or `retention` config get empty lists — the fields `skip_serializing_if = Vec::is_empty`, so JSON consumers written against the pre-Wave A shape are byte-stable.
 */
export interface PlanOutput {
  /**
   * Column-tag applications the governance reconciler would issue via `apply_column_tags`. One row per `(model, column, tag)` triple declared in a model sidecar's `[classification]` block.
   */
  classification_actions?: ClassificationAction[];
  command: string;
  /**
   * Environment name passed via `--env <name>`. Propagates to `mask_actions` so the preview resolves `[mask.<env>]` overrides on top of the workspace `[mask]` defaults. `None` when the flag is absent — preview resolves against defaults only.
   */
  env?: string | null;
  filter: string;
  /**
   * Masking-policy applications the governance reconciler would issue via `apply_masking_policy`. One row per `(model, column, tag)` where the tag resolves to a strategy for the active env. Unresolved tags are intentionally omitted — `rocky compliance` is the diagnostic surface for that gap.
   */
  mask_actions?: MaskAction[];
  /**
   * Retention-policy applications the governance reconciler would issue via `apply_retention_policy`. One row per model whose sidecar declares `retention = "<N>[dy]"`. `warehouse_preview` shows the warehouse-native SQL that the current adapter would compile the policy to (Databricks / Snowflake); `None` on warehouses without a first-class retention knob.
   */
  retention_actions?: RetentionAction[];
  statements: PlannedStatement[];
  version: string;
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
