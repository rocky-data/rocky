/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/connector_test.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * A state check a `[[run]]` in `connector-test.toml` declares: `unchanged` (the run must return the state it was given) or `advanced` (the run must return another state).
 */
export type ConnectorTestStateCheck = "unchanged" | "advanced";

/**
 * JSON output for `rocky connector test <name>`.
 *
 * The command runs the connector once per `[[run]]` in its `connector-test.toml`, feeding each run the state the run before it returned. It loads nothing and saves no state.
 */
export interface ConnectorTestOutput {
  command: string;
  /**
   * The `[connector.<name>]` key.
   */
  connector: string;
  /**
   * `true` when every run passed every check.
   */
  passed: boolean;
  /**
   * One entry per run that started, in order. A failed run stops the test, so later runs are not listed.
   */
  runs: ConnectorTestRunOutput[];
  /**
   * The test spec that was read.
   */
  spec: string;
  version: string;
  /**
   * Problems that do not fail the test, such as a stale helper copy.
   */
  warnings: string[];
  [k: string]: unknown;
}
/**
 * One run of `rocky connector test`.
 */
export interface ConnectorTestRunOutput {
  /**
   * Why the run failed.
   */
  error?: string | null;
  /**
   * The rows the spec expects.
   */
  expected_rows: number;
  /**
   * Parquet files the run wrote.
   */
  files: number;
  passed: boolean;
  /**
   * The rows the connector wrote; absent when the run failed before its result was verified.
   */
  rows?: number | null;
  /**
   * 1 for the first run.
   */
  run: number;
  /**
   * The state check the spec declares for this run, if any.
   */
  state_check?: ConnectorTestStateCheck | null;
  [k: string]: unknown;
}
