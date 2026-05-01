/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/archive.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky archive`.
 *
 * Mirrors [`CompactOutput`]: single-model invocations populate `model` and leave the catalog-scope fields absent; `--catalog` invocations populate `catalog`, `scope = "catalog"`, `tables`, and `totals`. The flat `statements` list carries every statement across every table.
 */
export interface ArchiveOutput {
  /**
   * Set when invoked as `rocky archive --catalog <name>`.
   */
  catalog?: string | null;
  command: string;
  dry_run: boolean;
  model?: string | null;
  older_than: string;
  older_than_days: number;
  /**
   * `"catalog"` for the catalog-scoped path; absent for single-model invocations.
   */
  scope?: string | null;
  statements: NamedStatement[];
  tables?: {
    [k: string]: ArchiveTableEntry;
  } | null;
  totals?: ArchiveTotals | null;
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
/**
 * Per-table archive plan inside a `--catalog` envelope.
 */
export interface ArchiveTableEntry {
  statements: NamedStatement[];
  [k: string]: unknown;
}
/**
 * Aggregate counts over a `rocky archive --catalog` invocation.
 */
export interface ArchiveTotals {
  statement_count: number;
  table_count: number;
  [k: string]: unknown;
}
