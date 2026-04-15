/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/metrics.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky metrics`.
 *
 * All optional fields are present-or-absent depending on the flags (`--trend`, `--alerts`, `--column`). The empty case (no snapshots available) sets `message` and leaves the collections empty.
 */
export interface MetricsOutput {
  alerts: MetricsAlert[];
  column?: string | null;
  column_trend: ColumnTrendPoint[];
  command: string;
  count: number;
  message?: string | null;
  model: string;
  snapshots: MetricsSnapshotEntry[];
  version: string;
  [k: string]: unknown;
}
export interface MetricsAlert {
  column?: string | null;
  message: string;
  run_id: string;
  severity: string;
  type: string;
  [k: string]: unknown;
}
export interface ColumnTrendPoint {
  null_rate?: number | null;
  row_count: number;
  run_id: string;
  timestamp: string;
  [k: string]: unknown;
}
export interface MetricsSnapshotEntry {
  freshness_lag_seconds?: number | null;
  null_rates: {
    [k: string]: number;
  };
  row_count: number;
  run_id: string;
  timestamp: string;
  [k: string]: unknown;
}
