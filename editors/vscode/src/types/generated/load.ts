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
  duration_ms: number;
  error?: string | null;
  file: string;
  rows_loaded: number;
  target: string;
  [k: string]: unknown;
}
