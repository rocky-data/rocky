/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/compact.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky compact`.
 *
 * Two shapes share this struct:
 *
 * - **Single-model (`rocky compact <fqn>`)**: `model` is set; `scope`, `catalog`, `tables`, and `totals` are absent. Byte-stable with envelopes that predate the catalog-scope flag. - **Catalog scope (`rocky compact --catalog <name>`)**: `model` is absent, `scope = "catalog"`, `catalog` is set, `tables` keys per-FQN statement bundles, and `totals` carries aggregate counts. The flat `statements` field still carries every SQL statement across all tables for consumers that just iterate it.
 */
export interface CompactOutput {
  /**
   * Set when invoked as `rocky compact --catalog <name>`. Stores the catalog identifier as resolved (lowercased to match the managed-table resolver's normalization).
   */
  catalog?: string | null;
  command: string;
  dry_run: boolean;
  /**
   * Set when invoked as `rocky compact <fqn>`.
   */
  model?: string | null;
  /**
   * `"catalog"` for the catalog-scoped path; absent for single-model invocations to keep their envelope byte-stable.
   */
  scope?: string | null;
  /**
   * Flat list of every SQL statement across every table. Single-model invocations have one bundle here; `--catalog` invocations have the concatenation of every per-table bundle (in the same order as `tables`'s key iteration order).
   */
  statements: NamedStatement[];
  /**
   * Per-table breakdown, keyed by fully-qualified table name. Present only on `--catalog` invocations.
   */
  tables?: {
    [k: string]: CompactTableEntry;
  } | null;
  target_size_mb: number;
  /**
   * Aggregate counts across the catalog. Present only on `--catalog` invocations.
   */
  totals?: CompactTotals | null;
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
 * Per-table compaction plan inside a `--catalog` envelope.
 */
export interface CompactTableEntry {
  statements: NamedStatement[];
  [k: string]: unknown;
}
/**
 * Aggregate counts over a `rocky compact --catalog` invocation.
 */
export interface CompactTotals {
  statement_count: number;
  table_count: number;
  [k: string]: unknown;
}
