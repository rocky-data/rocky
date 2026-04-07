/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai_test.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai test`.
 */
export interface AiTestOutput {
  command: string;
  results: AiTestModelResult[];
  version: string;
  [k: string]: unknown;
}
export interface AiTestModelResult {
  model: string;
  saved: boolean;
  tests: AiTestAssertion[];
  [k: string]: unknown;
}
export interface AiTestAssertion {
  description: string;
  name: string;
  sql?: string | null;
  [k: string]: unknown;
}
