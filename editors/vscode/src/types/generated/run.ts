/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/run.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Result of a single data quality check.
 */
export type CheckResult = CheckResult1 & {
  name: string;
  passed: boolean;
  /**
   * Severity reported when the check fails. `error` causes the quality pipeline to exit non-zero (subject to `fail_on_error`); `warning` is advisory and does not fail the run.
   */
  severity?: TestSeverity & string;
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
      /**
       * Column under test, when the assertion has one.
       */
      column?: string | null;
      /**
       * Number of failing rows (0 when passed). For `row_count_range`, this stores the observed total row count.
       */
      failing_rows: number;
      /**
       * Assertion kind — the `TestType` discriminant serialized as snake_case (e.g., `"not_null"`, `"accepted_values"`).
       */
      kind: string;
      [k: string]: unknown;
    }
  | {
      query: string;
      result_value: number;
      threshold: number;
      [k: string]: unknown;
    };
/**
 * Severity of a test failure.
 */
export type TestSeverity = "error" | "warning";
/**
 * Status of a pipeline run.
 *
 * `Success` / `PartialFailure` / `Failure` cover the terminal outcomes of a run that actually executed. `SkippedIdempotent` / `SkippedInFlight` are the short-circuit outcomes of `rocky run --idempotency-key` — see [`crate::idempotency`].
 */
export type RunStatus = ("Success" | "PartialFailure" | "Failure") | "SkippedIdempotent" | "SkippedInFlight";

/**
 * JSON output for `rocky run`.
 */
export interface RunOutput {
  anomalies: AnomalyOutput[];
  /**
   * Budget breaches detected at end of run. Empty when no `[budget]` block is configured or all configured limits were respected. Each breach is also emitted as a `budget_breach` [`rocky_observe::events::PipelineEvent`] and fires the `on_budget_breach` hook so subscribers see them live.
   */
  budget_breaches?: BudgetBreachOutput[];
  check_results: TableCheckOutput[];
  command: string;
  /**
   * Aggregate cost attribution across every materialization in this run (per-model entries + run totals). `None` for DuckDB-only pipelines or when no materializations produced a cost number.
   */
  cost_summary?: RunCostSummary | null;
  drift: DriftSummary;
  duration_ms: number;
  errors: TableErrorOutput[];
  /**
   * Tables that the discovery adapter reported as enabled but that do not exist in the source warehouse (e.g. Fivetran has them configured but hasn't synced them, or they carry the `do_not_alter__` broken-table marker). Surfaced so orchestrators can flag the gap in their UIs instead of silently dropping the assets.
   */
  excluded_tables?: ExcludedTableOutput[];
  execution: ExecutionSummary;
  filter: string;
  /**
   * The `--idempotency-key` value this run was invoked with, echoed back for operator cross-reference in logs and `rocky history`. `None` for runs that didn't pass the flag.
   */
  idempotency_key?: string | null;
  /**
   * `true` when the run was cancelled by a SIGINT (Ctrl-C). Surfaced so orchestrators can distinguish "user interrupted" from "run failed". Tables that hadn't reached `Success` or `Failed` at interrupt time are recorded as `TableStatus::Interrupted` in the state store. Always serialised (even when `false`) so consumers don't have to treat its absence specially.
   */
  interrupted: boolean;
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
  /**
   * Row-quarantine outcomes — one entry per table the quality pipeline quarantined. Empty for runs that did not use `[pipeline.x.checks.quarantine]`.
   */
  quarantine: QuarantineOutput[];
  resumed_from?: string | null;
  /**
   * True when running in shadow mode (targets rewritten).
   */
  shadow: boolean;
  /**
   * Prior run whose idempotency key deflected this call, or the run currently holding the in-flight claim. Populated only when `status` is `skipped_idempotent` or `skipped_in_flight`.
   */
  skipped_by_run_id?: string | null;
  /**
   * Terminal status of the run. `success` / `partial_failure` / `failure` match the lifecycle semantics callers already understand; the two `skipped_*` variants short-circuit via the idempotency key (see `idempotency_key` below). Always populated — non-skipped runs derive this field from `tables_failed` / materialization counts, so JSON consumers no longer need to re-derive status from counts themselves.
   */
  status: RunStatus;
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
/**
 * One budget breach surfaced on [`RunOutput::budget_breaches`].
 *
 * Kept as a CLI-side struct (rather than re-using [`rocky_core::config::BudgetBreach`]) so the JSON schema lives alongside the other `rocky run` output types. The fields mirror `BudgetBreach` one-to-one.
 */
export interface BudgetBreachOutput {
  actual: number;
  limit: number;
  /**
   * Which limit was breached: `"max_usd"`, `"max_duration_ms"`, or `"max_bytes_scanned"`.
   */
  limit_type: string;
  [k: string]: unknown;
}
export interface TableCheckOutput {
  asset_key: string[];
  checks: CheckResult[];
  [k: string]: unknown;
}
/**
 * Aggregate cost attribution for a `rocky run` invocation.
 *
 * The run finalizer walks every [`MaterializationOutput`] in [`RunOutput::materializations`], computes a per-model dollar cost using [`rocky_core::cost::compute_observed_cost_usd`], and stuffs the totals here. Consumers (Dagster, custom dashboards) can then read either the per-model breakdown or the run total without re-deriving the cost model themselves.
 */
export interface RunCostSummary {
  /**
   * Adapter type the cost formula was parameterised against, for audit. Values mirror `AdapterConfig.type`: "databricks", "snowflake", "bigquery", "duckdb".
   */
  adapter_type: string;
  /**
   * Per-model cost attribution, ordered as in [`RunOutput::materializations`].
   */
  per_model: ModelCostEntry[];
  /**
   * Sum of every per-model `bytes_scanned` that produced a number. `None` when no materialization reported a byte count (the non-BigQuery adapters today, which still inherit the default stub on `WarehouseAdapter::execute_statement_with_stats`). Surfaced so consumers — and the `[budget]` `max_bytes_scanned` gate — can read scan volume without re-walking [`RunOutput::materializations`].
   */
  total_bytes_scanned?: number | null;
  /**
   * Sum of every per-model `cost_usd` that produced a number. `None` when no materialization produced a cost.
   */
  total_cost_usd?: number | null;
  /**
   * Wall-clock time summed across every materialization. Separate from [`RunOutput::duration_ms`] (which includes setup / governance overhead) so budget enforcement can target model-compute time specifically.
   */
  total_duration_ms: number;
  [k: string]: unknown;
}
/**
 * Per-model cost attribution entry inside [`RunCostSummary`].
 */
export interface ModelCostEntry {
  /**
   * Asset key path for the model this entry covers.
   */
  asset_key: string[];
  /**
   * Observed cost for this materialization. `None` when the adapter type isn't billed or the formula couldn't be computed.
   */
  cost_usd?: number | null;
  duration_ms: number;
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
  /**
   * Adapter-reported bytes figure used for cost accounting, summed across all statements executed for this materialization. This is the *billing-relevant* number per adapter, not literal scan volume — so anyone comparing this to a warehouse console should know which column lines up.
   *
   * - **BigQuery:** `statistics.query.totalBytesBilled` (with the 10 MB per-query minimum already applied) — matches the BigQuery console's "Bytes billed" field, **not** "Bytes processed". Fed straight into [`rocky_core::cost::compute_observed_cost_usd`] to produce `cost_usd`. - **Databricks:** when populated, byte count from the statement-execution manifest (`total_byte_count`); `None` today until the manifest plumbing lands. - **Snowflake:** `None` — deferred by design (QUERY_HISTORY round-trip cost; Snowflake cost is duration × DBU, not bytes-driven). - **DuckDB:** `None` — no billed-bytes concept.
   */
  bytes_scanned?: number | null;
  /**
   * Adapter-reported bytes-written figure, summed across all statements. Currently `None` on every adapter — BigQuery doesn't expose a bytes-written figure for query jobs, and the Databricks / Snowflake paths haven't wired it yet. Reserved so future waves can populate it without a schema break.
   */
  bytes_written?: number | null;
  /**
   * Observed dollar cost of this materialization, computed post-hoc from the adapter-appropriate formula (duration × DBU rate for Databricks/Snowflake, bytes × $/TB for BigQuery, zero for DuckDB). `None` when the warehouse type isn't billed or the adapter didn't report the inputs the formula needs. Populated by the run finalizer via `rocky_core::cost::compute_observed_cost_usd`.
   */
  cost_usd?: number | null;
  duration_ms: number;
  /**
   * Warehouse-side job identifiers for the SQL statements rocky issued to materialize this output. Populated for adapters whose REST API surfaces a job reference per statement (BigQuery today via `jobReference.jobId`).
   *
   * Useful for cross-checking rocky's reported figures against the warehouse's own statistics — e.g., feeding a job ID into `bq show -j <id>` and comparing `totalBytesBilled` to [`Self::bytes_scanned`]. Empty `Vec` for adapters that don't surface a job concept (DuckDB) or haven't wired it yet (Databricks, Snowflake).
   */
  job_ids?: string[];
  metadata: MaterializationMetadata;
  /**
   * Partition window this materialization targeted, present only when the model's strategy is `time_interval`. `None` for unpartitioned strategies (full_refresh, incremental, merge).
   */
  partition?: PartitionInfo | null;
  rows_copied?: number | null;
  /**
   * Wall-clock timestamp captured at the moment the engine began executing this model. Used by `RunOutput::to_run_record` to build accurate per-model windows on the persisted `ModelExecution` — replaces the prior lossy reconstruction (`finished_at - duration`) that mis-ordered parallel runs. `finished_at` is derived as `started_at + duration_ms`; keeping one source of truth avoids drift between the two.
   */
  started_at: string;
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
/**
 * Row-quarantine outcome for a single table processed by the quality pipeline. Emitted when `[pipeline.x.checks.quarantine]` is enabled and the table has at least one error-severity row-level assertion that lowers to a boolean predicate.
 *
 * Row counts are reported when the warehouse adapter supplies them. Adapters that cannot count rows written by a `CREATE OR REPLACE TABLE` leave the counts as `None`.
 */
export interface QuarantineOutput {
  /**
   * Dagster-style asset key path (`[catalog, schema, table]`) of the source table the quarantine acted on.
   */
  asset_key: string[];
  /**
   * Error message from the first failing statement, if any.
   */
  error?: string | null;
  /**
   * Quarantine mode that was applied. One of `"split"`, `"tag"`, `"drop"` (matches [`rocky_core::config::QuarantineMode`]).
   */
  mode: string;
  /**
   * `true` when every quarantine statement executed successfully. `false` means a partial failure — inspect `error` for details.
   */
  ok: boolean;
  /**
   * Fully-qualified name of the `__quarantine` output table. Empty for `mode = "drop"` (failing rows discarded) and `mode = "tag"`.
   */
  quarantine_table: string;
  /**
   * Number of rows in the `__quarantine` output, when the adapter can report it.
   */
  quarantined_rows?: number | null;
  /**
   * Number of rows in the `__valid` output, when the adapter can report it.
   */
  valid_rows?: number | null;
  /**
   * Fully-qualified `catalog.schema.table` name of the `__valid` output table. Empty for `mode = "tag"` (source is rewritten in place).
   */
  valid_table: string;
  [k: string]: unknown;
}
