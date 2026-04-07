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
