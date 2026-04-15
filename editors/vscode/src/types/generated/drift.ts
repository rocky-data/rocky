/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/drift.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky drift`.
 */
export interface DriftOutput {
  command: string;
  drift: DriftSummary;
  version: string;
  [k: string]: unknown;
}
export interface DriftSummary {
  actions_taken: DriftActionOutput[];
  tables_checked: number;
  tables_drifted: number;
  [k: string]: unknown;
}
export interface DriftActionOutput {
  action: string;
  reason: string;
  table: string;
  [k: string]: unknown;
}
