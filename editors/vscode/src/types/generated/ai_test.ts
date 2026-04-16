/* eslint-disable */

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
