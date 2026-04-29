/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/preview_create.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky preview create`.
 *
 * Records the prune-and-copy decision plus the run summary for the branch execution. The `prune_set` lists models that re-executed; the `copy_set` lists models pre-populated from the base schema instead of being re-run. `skipped_set` is the empty-cost residue — neither changed nor downstream of a change.
 */
export interface PreviewCreateOutput {
  /**
   * Git ref the change set was computed against. Mirrors `--base`.
   */
  base_ref: string;
  /**
   * Branch name registered in the state store. Mirrors the `name` from `rocky branch create`.
   */
  branch_name: string;
  /**
   * Schema prefix the branch run wrote into (e.g. `branch__fix-price`).
   */
  branch_schema: string;
  command: string;
  /**
   * Models pre-populated from the base schema instead of re-running. Empty in Phase 1 if no models could be safely copied.
   */
  copy_set: PreviewCopiedModel[];
  duration_ms: number;
  /**
   * Git SHA the diff was computed from (typically `HEAD`).
   */
  head_ref: string;
  /**
   * Models that re-executed against the branch.
   */
  prune_set: PreviewPrunedModel[];
  /**
   * `RunRecord` id for the branch execution. Empty when no models re-executed (a no-op preview where every changed model pruned to zero downstream).
   */
  run_id: string;
  /**
   * `succeeded`, `partial`, or `failed` — mirrors `RunRecord.status`.
   */
  run_status: string;
  /**
   * Models neither in the prune set nor copied — the column-level pruner determined they are unaffected and not depended on by any pruned model. Names only; rationale lives in the diff layer.
   */
  skipped_set: string[];
  version: string;
  [k: string]: unknown;
}
/**
 * One entry in [`PreviewCreateOutput::copy_set`].
 *
 * `copy_strategy` is `"ctas"` in Phase 1 (DuckDB CTAS or generic `CREATE TABLE AS SELECT *`); Phase 5 lifts this to `"shallow_clone"` (Databricks Delta) and `"zero_copy_clone"` (Snowflake) per the adapter's clone capability.
 */
export interface PreviewCopiedModel {
  copy_strategy: string;
  model_name: string;
  source_schema: string;
  target_schema: string;
  [k: string]: unknown;
}
/**
 * One entry in [`PreviewCreateOutput::prune_set`].
 */
export interface PreviewPrunedModel {
  /**
   * Columns the diff reports as changed on this model. Only populated when `reason = "changed"`.
   */
  changed_columns: string[];
  model_name: string;
  /**
   * `"changed"` for models the diff identified directly, or `"downstream_of_changed"` for models pulled in by column-level lineage from a changed model.
   */
  reason: string;
  [k: string]: unknown;
}
