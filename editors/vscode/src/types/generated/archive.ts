/* eslint-disable */

/**
 * JSON output for `rocky archive`.
 */
export interface ArchiveOutput {
  command: string;
  dry_run: boolean;
  model?: string | null;
  older_than: string;
  older_than_days: number;
  statements: NamedStatement[];
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
