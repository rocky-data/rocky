/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/test.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky test`.
 */
export interface TestOutput {
  command: string;
  /**
   * Results from declarative `[[tests]]` in model sidecars. Present only when `--declarative` is used.
   */
  declarative?: DeclarativeTestSummary | null;
  failed: number;
  failures: TestFailure[];
  /**
   * Per-model outcomes for the (DuckDB-backed) model-execution test — passes too, not just failures. Lets the VS Code Inspector Tests tab and the dagster integration render "good_mart: pass" without inferring it from `total - failures`. Empty when only declarative tests ran. Filtered to `--model` when that flag is set.
   */
  model_results?: ModelTestResult[];
  passed: number;
  total: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Summary of declarative test execution (from `[[tests]]` in model sidecars).
 */
export interface DeclarativeTestSummary {
  errored: number;
  failed: number;
  passed: number;
  results: DeclarativeTestResult[];
  total: number;
  warned: number;
  [k: string]: unknown;
}
/**
 * Result of a single declarative test assertion.
 */
export interface DeclarativeTestResult {
  /**
   * Column under test, if applicable.
   */
  column?: string | null;
  /**
   * Human-readable detail (e.g., "3 NULL rows found" or execution error).
   */
  detail?: string | null;
  /**
   * Model name the test belongs to.
   */
  model: string;
  /**
   * Severity declared in the sidecar ("error" or "warning").
   */
  severity: string;
  /**
   * The SQL that was executed.
   */
  sql: string;
  /**
   * "pass", "fail", or "error".
   */
  status: string;
  /**
   * Fully-qualified target table (catalog.schema.table).
   */
  table: string;
  /**
   * Test type (e.g., "not_null", "unique", "row_count_range").
   */
  test_type: string;
  [k: string]: unknown;
}
/**
 * One failed test, mirroring the (name, error) tuple in `rocky_engine::test_runner::TestResult::failures` but with named fields because schemars/JSON Schema can't represent positional tuples cleanly.
 */
export interface TestFailure {
  error: string;
  name: string;
  [k: string]: unknown;
}
/**
 * One per-model outcome from the local model-execution test.
 *
 * `status` is `"pass"` or `"fail"`. `error` is set only when `status = "fail"`. Mirrors `rocky_engine::test_runner::ModelTestResult` with the status flattened to a string so consumers (Pydantic, TypeScript) get a stable, JSON-Schema-friendly shape.
 */
export interface ModelTestResult {
  error?: string | null;
  model: string;
  /**
   * `"pass"` or `"fail"`.
   */
  status: string;
  [k: string]: unknown;
}
