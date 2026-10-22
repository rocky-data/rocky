/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/load.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * How a `rocky load --connector` run ended: `loaded` (the batch is in the target table and the connector state is saved), `skipped` (another run holds the claim for this batch, so nothing ran) or `failed` (see `error`).
 */
export type ConnectorLoadStatus = "loaded" | "skipped" | "failed";

/**
 * JSON output for `rocky load`.
 */
export interface LoadOutput {
  command: string;
  /**
   * Set by `rocky load --connector`: what the child connector run did. Absent for a load from a directory.
   */
  connector?: ConnectorLoadInfo | null;
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
 * The child connector part of a `rocky load --connector` result.
 */
export interface ConnectorLoadInfo {
  /**
   * True when this run took over an expired in-flight claim. A prior run may then have promoted its batch before it crashed, and this run may load a duplicate batch.
   */
  adopted_expired_claim: boolean;
  /**
   * Attempts that ran, counting the successful one. 0 when skipped.
   */
  attempts: number;
  /**
   * Batches committed after this run. Equal to `commit_seq_before` unless `status` is `loaded`.
   */
  commit_seq_after: number;
  /**
   * Batches committed before this run.
   */
  commit_seq_before: number;
  /**
   * The hosts the manifest declares. Declared, not enforced.
   */
  egress_hosts: string[];
  /**
   * Why the run failed. Present only when `status` is `failed`.
   */
  error?: string | null;
  /**
   * The `[connector.<name>]` key.
   */
  name: string;
  status: ConnectorLoadStatus;
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
   * Non-fatal warnings — e.g. a contract clause that can't be meaningfully enforced in this context, or a required column whose type Rocky could not compare because the landed or the declared type string is outside its type map. Does not affect `passed`.
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
