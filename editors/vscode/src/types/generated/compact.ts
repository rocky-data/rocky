/* eslint-disable */

/**
 * JSON output for `rocky compact`.
 */
export interface CompactOutput {
  command: string;
  dry_run: boolean;
  model: string;
  statements: NamedStatement[];
  target_size_mb: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Named SQL statement (purpose + sql), reused by compact and archive.
 */
export interface NamedStatement {
  purpose: string;
  sql: string;
  [k: string]: unknown;
}
