/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state_reconcile_watermark.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Result of repairing incremental cursors from physical target tables.
 */
export interface ReconcileWatermarkOutput {
  command: string;
  dry_run: boolean;
  pipeline: string;
  version: string;
  watermarks: ReconciledWatermark[];
  [k: string]: unknown;
}
export interface ReconciledWatermark {
  previous?: string | null;
  table: string;
  target_max?: string | null;
  watermark: string;
  [k: string]: unknown;
}
