/* eslint-disable */

/**
 * Severity level of a diagnostic.
 *
 * Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay compatible with existing dagster fixtures and the hand-written `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
 */
export type Severity = "Error" | "Warning" | "Info";

/**
 * JSON output for `rocky ci`.
 */
export interface CiOutput {
  command: string;
  compile_ok: boolean;
  diagnostics: Diagnostic[];
  exit_code: number;
  failures: TestFailure[];
  models_compiled: number;
  tests_failed: number;
  tests_ok: boolean;
  tests_passed: number;
  version: string;
  [k: string]: unknown;
}
/**
 * A compiler diagnostic (error, warning, or informational message).
 */
export interface Diagnostic {
  /**
   * Diagnostic code (e.g., "E001", "W001").
   */
  code: string;
  /**
   * Human-readable message.
   */
  message: string;
  /**
   * Which model this diagnostic relates to.
   */
  model: string;
  /**
   * Severity level.
   */
  severity: Severity;
  /**
   * Source location (if available).
   */
  span?: SourceSpan | null;
  /**
   * Suggested fix (if any).
   */
  suggestion?: string | null;
  [k: string]: unknown;
}
/**
 * Location in a source file.
 */
export interface SourceSpan {
  col: number;
  file: string;
  line: number;
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
