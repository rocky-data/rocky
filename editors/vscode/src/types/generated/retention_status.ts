/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/retention_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky retention-status`.
 *
 * Reports which models declare a `retention = "<N>[dy]"` sidecar value and — when `--drift` is set in a future wave — whether the warehouse's current retention matches. Today `warehouse_days` is always `None` because the probe is deferred to v2; the schema is stable so v2 can populate the field without a JSON shape break.
 */
export interface RetentionStatusOutput {
  command: string;
  models: ModelRetentionStatus[];
  version: string;
  [k: string]: unknown;
}
/**
 * Per-model retention declaration + (eventually) warehouse-observed value.
 *
 * - `configured_days` is `None` when the model's sidecar has no `retention` key. - `warehouse_days` is populated by the (v2) `--drift` probe via `SHOW TBLPROPERTIES` on Databricks or `SHOW PARAMETERS ... FOR TABLE` on Snowflake. Always `None` in v1. - `in_sync` is `true` iff `configured_days == warehouse_days`, or both are `None`. A model with no configured retention is never flagged as out-of-sync.
 */
export interface ModelRetentionStatus {
  configured_days?: number | null;
  in_sync: boolean;
  model: string;
  warehouse_days?: number | null;
  [k: string]: unknown;
}
