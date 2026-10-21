/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/cost.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * How a [`StoredBytes`] figure was measured.
 */
export type StoredBytesBasis = "data_files" | "allocated_blocks";
/**
 * Where a recorded run wrote its results, as `rocky history` and `rocky cost` report it (#2201).
 */
export type RunScopeKind = "production" | "shadow" | "branch" | "unrecorded";

/**
 * JSON output for `rocky cost <run_id|latest>`.
 *
 * Historical per-run cost attribution read from the embedded state store's [`rocky_core::state::RunRecord`]. Re-derives per-model cost via [`rocky_core::cost::compute_observed_cost_usd`] — the same formula [`RunOutput::populate_cost_summary`] applies at the end of a live run. The per-model and per-run totals make the "what did my last run cost?" question answerable from the recorded run alone, without re-materialising tables.
 *
 * # Adapter type resolution
 *
 * The `RunRecord` only carries `config_hash`, not the adapter type. The command loads `rocky.toml` to resolve the billed-warehouse type. When the config can't be loaded (working-dir mismatch, missing file, parse error), the output degrades gracefully: `adapter_type` stays `None` and `cost_usd` is `None` on every model, but durations and byte counts are still populated from the stored record.
 *
 * # BigQuery note
 *
 * Because [`rocky_core::state::ModelExecution::bytes_scanned`] is persisted, this command can return a real cost figure for BigQuery runs even though the live `rocky run` path currently reports `cost_usd: None` for BQ (adapter bytes-scanned plumbing is a follow-up).
 */
export interface CostOutput {
  /**
   * Adapter type the cost formula was parameterised against, for audit. Mirrors `AdapterConfig.type`. `None` when the config couldn't be loaded or the adapter isn't a billed warehouse.
   */
  adapter_type?: string | null;
  command: string;
  duration_ms: number;
  finished_at: string;
  /**
   * Grouped cost rollup, present only when `--by <dimension>` is passed. `--by model` produces one group per model; `--by tenant` produces one group per tenant (models with no recorded tenant land in an `"<unattributed>"` bucket). `None` (and omitted from JSON) for a plain `rocky cost` invocation, so the default output shape is unchanged. `per_model` is always present regardless of grouping.
   */
  groups?: CostGroup[] | null;
  per_model: PerModelCostHistorical[];
  run_id: string;
  /**
   * Where the reported run wrote (#2201). `rocky cost latest` reports the newest production run: shadow and branch runs are skipped, and a run recorded before runs carried a scope is eligible and reads `unrecorded` here. An explicit run id reports that run, whatever its scope.
   */
  run_scope?: RunScopeKind & string;
  started_at: string;
  status: string;
  /**
   * Sum of per-model `bytes_scanned`. `None` when no model reported bytes scanned.
   */
  total_bytes_scanned?: number | null;
  /**
   * Sum of per-model `bytes_written`. `None` when no model reported bytes written.
   */
  total_bytes_written?: number | null;
  /**
   * Sum of every per-model `cost_usd` that produced a number. `None` when no model produced a cost.
   */
  total_cost_usd?: number | null;
  /**
   * Wall-clock time summed across every model execution.
   */
  total_duration_ms: number;
  trigger: string;
  version: string;
  [k: string]: unknown;
}
/**
 * One grouped row in [`CostOutput::groups`], emitted when `rocky cost` is run with `--by <dimension>`.
 *
 * Each group sums the per-model figures of the executions that share the grouping key (a tenant, or a model name). The cost roll-up uses the same `compute_observed_cost_usd` figures as [`PerModelCostHistorical`], so a `--by tenant` total equals the sum of its members' `cost_usd`.
 */
export interface CostGroup {
  /**
   * Dimension the grouping was performed on: `"tenant"` or `"model"`.
   */
  dimension: string;
  /**
   * The grouping key's value — the tenant name, the model name, or the literal `"<unattributed>"` for the `--by tenant` bucket that collects executions with no recorded tenant.
   */
  key: string;
  /**
   * Number of model executions rolled into this group.
   */
  model_count: number;
  /**
   * Sum of the group's member `bytes_scanned`. `None` when no member reported bytes scanned.
   */
  total_bytes_scanned?: number | null;
  /**
   * Sum of the group's member `bytes_written`. `None` when no member reported bytes written.
   */
  total_bytes_written?: number | null;
  /**
   * Sum of every member's `cost_usd` that produced a number. `None` when no member produced a cost.
   */
  total_cost_usd?: number | null;
  /**
   * Wall-clock time summed across the group's member executions.
   */
  total_duration_ms: number;
  [k: string]: unknown;
}
/**
 * A single model's cost attribution inside [`CostOutput`].
 *
 * Distinct from [`ModelCostEntry`] (which lives on [`RunCostSummary`]) because the historical surface carries the richer fields the state store actually persists: model name (not asset-key vector), row/byte counts, and the recorded per-model status.
 */
export interface PerModelCostHistorical {
  /**
   * Adapter-reported bytes figure used for cost accounting. This is the *billing-relevant* number per adapter, not literal scan volume:
   *
   * - **BigQuery:** `totalBytesBilled` — includes the 10 MB per-query minimum floor; matches the BigQuery console's "Bytes billed" field, **not** "Bytes processed". - **Databricks:** when populated, byte count from the statement-execution manifest (`total_byte_count`); `None` today until the manifest plumbing lands. - **Snowflake:** `None` — deferred by design (QUERY_HISTORY round-trip cost; Snowflake cost is duration × DBU, not bytes-driven). - **DuckDB:** `None` — no billed-bytes concept.
   */
  bytes_scanned?: number | null;
  /**
   * Adapter-reported bytes-written figure. Currently `None` on every adapter — BigQuery doesn't expose a bytes-written figure for query jobs, and the Databricks / Snowflake paths haven't wired it yet.
   */
  bytes_written?: number | null;
  /**
   * Observed cost for this execution. `None` when the adapter isn't a billed warehouse, the config couldn't be loaded, or the formula inputs were unavailable (e.g. BigQuery without `bytes_scanned`).
   */
  cost_usd?: number | null;
  duration_ms: number;
  model_name: string;
  rows_affected?: number | null;
  status: string;
  /**
   * The bytes the model's output table stored right after this execution wrote it, read back from the persisted `rocky_core::state::ModelExecution::stored_bytes`. Read `stored_bytes_basis` to tell an exact figure from an estimate.
   *
   * `None` (and omitted from JSON) means unknown: a view, a replicated table, a `time_interval` partition, an adapter that cannot measure it (every adapter except DuckDB today), an in-memory DuckDB database, a failed measurement, or a run recorded before this field existed. There is no run total: the bases differ, so a sum would mix exact and estimated figures.
   */
  stored_bytes?: number | null;
  /**
   * How `stored_bytes` was measured: `data_files` (exact Parquet data-file bytes, content-addressed outputs) or `allocated_blocks` (an estimate: DuckDB storage blocks times the block size, which overstates a small table). Present exactly when `stored_bytes` is.
   */
  stored_bytes_basis?: StoredBytesBasis | null;
  /**
   * Tenant this execution was attributed to, read back from the persisted `rocky_core::state::ModelExecution::tenant`. Present only for replication executions whose source schema declared a `{tenant}` component; `None` (and omitted from JSON) otherwise.
   */
  tenant?: string | null;
  /**
   * What the execution read from each upstream table, read back from the persisted `rocky_core::state::ModelExecution::upstream_reads`. It is a measurement, not a cost: `est_bytes` is an estimate from fixed-width column types, and nothing here splits an upstream's cost.
   *
   * Omitted when not measured: a non-DuckDB adapter, a shadow, branch or backfill run, a failed profile read, or a run recorded before this field existed.
   */
  upstream_reads?: UpstreamReads | null;
  [k: string]: unknown;
}
/**
 * The upstream reads of one model execution.
 */
export interface UpstreamReads {
  /**
   * One entry per upstream table, sorted by `table`. The model's own target is left out.
   */
  reads?: UpstreamRead[];
  /**
   * Scans that name no table: a file (Parquet, CSV, Iceberg) or a table function such as `range`. They are counted, not attributed.
   */
  unattributed_scans?: number;
  [k: string]: unknown;
}
/**
 * What one model execution read from one upstream table.
 *
 * A measurement of the scan, not a price: a later step may use it to split the upstream's cost across its consumers.
 */
export interface UpstreamRead {
  /**
   * Estimate: `rows_scanned` times the summed fixed width of the projected columns, per scan. `None` when a scan has pushed-down filters (their columns are unknown, so the width would be biased), a projected column is variable-width (text, blob, list, struct), a column type could not be read, or no columns were named. It is in-memory width, not bytes on disk.
   */
  est_bytes?: number | null;
  /**
   * The output columns of the scans, first seen first, with no repeats. Columns used only in filters are not included: DuckDB lists those under the scan's pushed-down filters, not its projections. Empty when the profile did not name them.
   */
  projected_columns?: string[];
  /**
   * Rows the scan operators of this execution read from the table, summed over every scan of it (a self-join counts both). Not rows returned. It can be far below the table's row count: DuckDB skips row groups its zone maps rule out. A read through a view is credited to the view's base table. On a first-run `merge`, the rows count twice: the populating CTAS reads them, then the MERGE reads them again.
   */
  rows_scanned: number;
  /**
   * The upstream table as `catalog.schema.table`, in the names the warehouse reports. On DuckDB the catalog is the database name (`memory` for an in-memory database).
   */
  table: string;
  [k: string]: unknown;
}
