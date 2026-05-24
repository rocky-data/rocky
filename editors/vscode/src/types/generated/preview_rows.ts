/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/preview_rows.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky preview rows`.
 *
 * A sample of result rows for a single transformation model (or one of its CTEs), executed against the pipeline's configured adapter. Classified columns are masked inline before execution, so the rows match what the materialized target would expose. `truncated` is `true` when the model produced at least `limit_applied` rows.
 */
export interface PreviewRowsOutput {
  /**
   * Adapter the preview ran against (e.g. `"duckdb"`, `"databricks"`).
   */
  adapter_kind: string;
  /**
   * Output column names, in order.
   */
  columns: string[];
  command: string;
  /**
   * CTE within the model that was isolated, if `--cte` was given.
   */
  cte?: string | null;
  duration_ms: number;
  /**
   * The exact SQL executed (carries no credentials). Useful for debugging the masking projection and CTE isolation.
   */
  executed_sql: string;
  /**
   * The `LIMIT` applied to the preview query.
   */
  limit_applied: number;
  /**
   * Model whose output was sampled.
   */
  model: string;
  /**
   * Number of rows returned (≤ `limit_applied`).
   */
  row_count: number;
  /**
   * Sampled rows; each row is a list of JSON-encoded cell values aligned to `columns`.
   */
  rows: unknown[][];
  /**
   * `true` when `row_count` reached `limit_applied` — more rows may exist.
   */
  truncated: boolean;
  version: string;
  [k: string]: unknown;
}
