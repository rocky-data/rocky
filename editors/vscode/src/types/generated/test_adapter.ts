/* eslint-disable */

/**
 * JSON output for `rocky test-adapter`.
 *
 * Mirrors `rocky_adapter_sdk::conformance::ConformanceResult` with the status/category enums stringified, so we don't have to add `JsonSchema` to the SDK crate.
 */
export interface TestAdapterOutput {
  adapter: string;
  results: TestAdapterTestResult[];
  sdk_version: string;
  tests_failed: number;
  tests_passed: number;
  tests_run: number;
  tests_skipped: number;
  [k: string]: unknown;
}
export interface TestAdapterTestResult {
  /**
   * Stringified `TestCategory`: "connection", "ddl", "dml", "query", "types", "dialect", "governance", "discovery", "batch_checks".
   */
  category: string;
  duration_ms: number;
  message?: string | null;
  name: string;
  /**
   * Stringified `TestStatus`: "passed", "failed", "skipped".
   */
  status: string;
  [k: string]: unknown;
}
