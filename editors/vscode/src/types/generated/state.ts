/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state show`.
 */
export interface StateOutput {
  command: string;
  /**
   * redb state-schema version stamped in the on-disk state file, or `null` when no state file exists yet or it predates schema versioning.
   */
  schema_version_on_disk?: number | null;
  /**
   * redb state-schema version this binary supports. Pair with `schema_version_on_disk` to make a compatibility decision without parsing a human error string: `on_disk > supported` means the file was written by a newer engine (forward-incompatible). See the state-schema-deploy-safety contract.
   */
  schema_version_supported: number;
  version: string;
  watermarks: WatermarkEntry[];
  [k: string]: unknown;
}
export interface WatermarkEntry {
  last_value: string;
  table: string;
  updated_at: string;
  [k: string]: unknown;
}
