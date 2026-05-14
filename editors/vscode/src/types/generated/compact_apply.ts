/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/compact_apply.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky compact apply <plan_id>`.
 */
export interface CompactApplyOutput {
  command: string;
  executed_at: string;
  /**
   * The plan that was applied.
   */
  plan_id: string;
  /**
   * Per-statement results, in the order they were executed.
   */
  statements: StatementResult[];
  /**
   * `true` when all statements succeeded; `false` on the first failure (remaining statements are still reported with `success: false` and `duration_ms: 0`).
   */
  success: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * Result of executing one SQL statement during `rocky compact apply` or `rocky archive apply`.
 */
export interface StatementResult {
  /**
   * Execution duration in milliseconds. Zero when execution was skipped due to a prior statement failure in the same apply run.
   */
  duration_ms: number;
  /**
   * Adapter error message. `None` when `success == true`.
   */
  error?: string | null;
  /**
   * Human-readable purpose label (mirrors `NamedStatement.purpose`).
   */
  purpose: string;
  /**
   * The SQL that was executed.
   */
  sql: string;
  /**
   * Whether the adapter accepted the statement without error.
   */
  success: boolean;
  [k: string]: unknown;
}
