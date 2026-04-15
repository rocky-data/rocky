/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/archive.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

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
