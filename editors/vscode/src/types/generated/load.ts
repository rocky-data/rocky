/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/load.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky load`.
 */
export interface LoadOutput {
  command: string;
  duration_ms: number;
  files: LoadFileOutput[];
  files_failed: number;
  files_loaded: number;
  format: string;
  source_dir: string;
  total_bytes: number;
  total_rows: number;
  version: string;
  [k: string]: unknown;
}
/**
 * A single file result within `LoadOutput`.
 */
export interface LoadFileOutput {
  bytes_read: number;
  /**
   * Result of the data-contract gate, when the load pipeline declares a contract. `None` for ungated loads. On a failed gate the data was not promoted to the target; the violations explain why.
   */
  contract?: ContractResult | null;
  duration_ms: number;
  error?: string | null;
  file: string;
  rows_loaded: number;
  target: string;
  [k: string]: unknown;
}
/**
 * Result of contract validation.
 */
export interface ContractResult {
  passed: boolean;
  violations: ContractViolation[];
  /**
   * Non-fatal warnings — e.g. a contract clause that can't be meaningfully enforced in this context. Does not affect `passed`.
   */
  warnings?: string[];
  [k: string]: unknown;
}
/**
 * A single contract rule violation with the rule name, affected column, and message.
 */
export interface ContractViolation {
  column: string;
  message: string;
  rule: string;
  [k: string]: unknown;
}
