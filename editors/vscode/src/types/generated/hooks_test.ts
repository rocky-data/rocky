/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/hooks_test.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky hooks test <event>`.
 */
export interface HooksTestOutput {
  event: string;
  message?: string | null;
  /**
   * Free-form Debug rendering of the hook result, when applicable.
   */
  result?: string | null;
  /**
   * One of "no_hooks", "continue", "abort".
   */
  status: string;
  [k: string]: unknown;
}
