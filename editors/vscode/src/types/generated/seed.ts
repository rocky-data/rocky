/* eslint-disable */

/**
 * JSON output for `rocky seed`.
 */
export interface SeedOutput {
  command: string;
  duration_ms: number;
  seeds_dir: string;
  tables: SeedTableOutput[];
  tables_failed: number;
  tables_loaded: number;
  version: string;
  [k: string]: unknown;
}
/**
 * A single seed table result within `SeedOutput`.
 */
export interface SeedTableOutput {
  columns: number;
  duration_ms: number;
  error?: string | null;
  name: string;
  rows: number;
  target: string;
  [k: string]: unknown;
}
