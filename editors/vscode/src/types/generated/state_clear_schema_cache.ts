/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state_clear_schema_cache.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state clear-schema-cache`.
 *
 * Part of Arc 7 wave 2 wave-2 PR 4 — the explicit-flush path for the schema cache written by PR 2 (`rocky run` write tap) and read by PR 1b (compile/lsp/etc.). `dry_run = true` reports what *would* be deleted without touching redb; `dry_run = false` deletes the entries and `entries_deleted` is the actual removed count.
 */
export interface ClearSchemaCacheOutput {
  command: string;
  /**
   * `true` when `--dry-run` was set; the cache is left untouched.
   */
  dry_run: boolean;
  /**
   * Number of entries removed (or that would be removed in dry-run mode). Zero when the cache was already empty.
   */
  entries_deleted: number;
  version: string;
  [k: string]: unknown;
}
