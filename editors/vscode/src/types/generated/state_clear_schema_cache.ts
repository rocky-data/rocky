/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state_clear_schema_cache.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state clear-schema-cache`.
 *
 * `dry_run = true` reports what *would* be deleted without touching redb; `dry_run = false` deletes the entries. `entries_deleted` is the actual removed count (or would-be-removed count in dry-run).
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
