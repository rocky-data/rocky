/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/run.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * Result of a single data quality check.
 */
export type CheckResult = CheckResult1 & {
  name: string;
  passed: boolean;
  [k: string]: unknown;
};
export type CheckResult1 =
  | {
      source_count: number;
      target_count: number;
      [k: string]: unknown;
    }
  | {
      extra: string[];
      missing: string[];
      [k: string]: unknown;
    }
  | {
      lag_seconds: number;
      threshold_seconds: number;
      [k: string]: unknown;
    }
  | {
      column: string;
      null_rate: number;
      threshold: number;
      [k: string]: unknown;
    }
  | {
      query: string;
      result_value: number;
      threshold: number;
      [k: string]: unknown;
    };

/**
 * JSON output for `rocky run`.
 */
export interface RunOutput {
  anomalies: AnomalyOutput[];
  check_results: TableCheckOutput[];
  command: string;
  drift: DriftSummary;
  duration_ms: number;
  errors: TableErrorOutput[];
  /**
   * Tables that the discovery adapter reported as enabled but that do not exist in the source warehouse (e.g. Fivetran has them configured but hasn't synced them, or they carry the `do_not_alter__` broken-table marker). Surfaced so orchestrators can flag the gap in their UIs instead of silently dropping the assets.
   */
  excluded_tables: ExcludedTableOutput[];
  execution: ExecutionSummary;
  filter: string;
  materializations: MaterializationOutput[];
  metrics?: MetricsSnapshot | null;
  /**
   * Per-model partition execution summaries, present only when the run touched one or more `time_interval` models. Empty for runs that didn't execute any partitioned models.
   */
  partition_summaries: PartitionSummary[];
  permissions: PermissionSummary;
  /**
   * Pipeline type that was executed (e.g., "replication").
   */
  pipeline_type?: string | null;
  resumed_from?: string | null;
  /**
   * True when running in shadow mode (targets rewritten).
   */
  shadow: boolean;
  tables_copied: number;
  tables_failed: number;
  tables_skipped: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Row count anomaly detected by historical baseline comparison.
 */
export interface AnomalyOutput {
  baseline_avg: number;
  current_count: number;
  deviation_pct: number;
  reason: string;
  table: string;
  [k: string]: unknown;
}
export interface TableCheckOutput {
  asset_key: string[];
  checks: CheckResult[];
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
/**
 * Error from a table that failed during parallel processing.
 */
export interface TableErrorOutput {
  asset_key: string[];
  error: string;
  [k: string]: unknown;
}
/**
 * A table that the discovery adapter reported but that is missing from the source warehouse, so the run skipped it. Tracked separately from `errors` because it is not a runtime failure — the row never made it past the pre-flight existence check.
 */
export interface ExcludedTableOutput {
  /**
   * Dagster-style asset key path (`[source_type, ...components, table]`).
   */
  asset_key: string[];
  /**
   * Why the table was excluded. Currently always `"missing_from_source"` but kept as a free-form field so future causes (disabled, sync_paused, ...) can be added without a schema break.
   */
  reason: string;
  /**
   * Source schema the table was expected to live in.
   */
  source_schema: string;
  /**
   * Bare table name as reported by the discovery adapter.
   */
  table_name: string;
  [k: string]: unknown;
}
/**
 * Summary of execution parallelism and throughput.
 */
export interface ExecutionSummary {
  /**
   * Whether adaptive concurrency (AIMD throttle) was enabled for this run.
   */
  adaptive_concurrency: boolean;
  concurrency: number;
  /**
   * Final concurrency level at end of run (may differ from initial if adaptive concurrency adjusted it). Only present when adaptive concurrency is enabled.
   */
  final_concurrency?: number | null;
  /**
   * Number of rate-limit signals (HTTP 429 / UC_REQUEST_LIMIT_EXCEEDED) that triggered concurrency reduction. Only present when adaptive concurrency is enabled.
   */
  rate_limits_detected?: number | null;
  tables_failed: number;
  tables_processed: number;
  [k: string]: unknown;
}
export interface MaterializationOutput {
  asset_key: string[];
  duration_ms: number;
  metadata: MaterializationMetadata;
  /**
   * Partition window this materialization targeted, present only when the model's strategy is `time_interval`. `None` for unpartitioned strategies (full_refresh, incremental, merge).
   */
  partition?: PartitionInfo | null;
  rows_copied?: number | null;
  [k: string]: unknown;
}
export interface MaterializationMetadata {
  /**
   * Number of columns in the materialized table's typed schema. Populated for derived models (where the compiler resolved a typed schema); `None` for source-replication tables that inherit their schema from upstream.
   */
  column_count?: number | null;
  /**
   * Compile time in milliseconds for the model that produced this materialization. Populated only for derived models. Mirrors the relevant slice of `PhaseTimings.total_ms`.
   */
  compile_time_ms?: number | null;
  /**
   * Short hash (16 hex chars) of the generated SQL string. Lets orchestrators detect "what changed?" between runs without diffing full SQL bodies. Computed via `xxh3_64` of the canonical SQL the engine sent to the warehouse.
   */
  sql_hash?: string | null;
  strategy: string;
  /**
   * Fully-qualified target table identifier in `catalog.schema.table` format. Useful for click-through links to the warehouse UI from the Dagster asset detail page. Always set when the materialization targets a known table.
   */
  target_table_full_name?: string | null;
  watermark?: string | null;
  [k: string]: unknown;
}
/**
 * Partition window information for a single `time_interval` materialization.
 *
 * `key` is the canonical Rocky partition key (e.g. `"2026-04-07"` for daily; `"2026-04-07T13"` for hourly). `start` / `end` are the half-open `[start, end)` window the SQL substituted for `@start_date` / `@end_date`. `batched_with` lists any additional partition keys that were merged into this batch when `batch_size > 1` — empty for the default one-partition-per-statement case.
 */
export interface PartitionInfo {
  batched_with: string[];
  end: string;
  key: string;
  start: string;
  [k: string]: unknown;
}
/**
 * Serializable metrics summary for JSON output.
 */
export interface MetricsSnapshot {
  anomalies_detected: number;
  error_rate_pct: number;
  query_duration_max_ms: number;
  query_duration_p50_ms: number;
  query_duration_p95_ms: number;
  retries_attempted: number;
  retries_succeeded: number;
  statements_executed: number;
  table_duration_max_ms: number;
  table_duration_p50_ms: number;
  table_duration_p95_ms: number;
  tables_failed: number;
  tables_processed: number;
  [k: string]: unknown;
}
/**
 * Per-model summary of `time_interval` partition execution.
 *
 * One entry per partitioned model touched by the run. Lets dagster-rocky (and other orchestrators) display per-model partition stats without re-counting the per-partition `MaterializationOutput.partition` entries.
 */
export interface PartitionSummary {
  model: string;
  partitions_failed: number;
  partitions_planned: number;
  /**
   * Partitions that were already `Computed` in the state store and skipped by the runtime (currently always 0; reserved for the `--missing` change-detection optimization).
   */
  partitions_skipped: number;
  partitions_succeeded: number;
  [k: string]: unknown;
}
export interface PermissionSummary {
  catalogs_created: number;
  grants_added: number;
  grants_revoked: number;
  schemas_created: number;
  [k: string]: unknown;
}
