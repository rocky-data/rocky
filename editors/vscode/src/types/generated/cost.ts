/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/cost.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

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
 * Because [`rocky_core::state::ModelExecution::bytes_scanned`] is persisted, this command can return a real cost figure for BigQuery runs even though the live `rocky run` path currently reports `cost_usd: None` for BQ (adapter bytes-scanned plumbing is a follow-up wave).
 */
export interface CostOutput {
  /**
   * Adapter type the cost formula was parameterised against, for audit. Mirrors `AdapterConfig.type`. `None` when the config couldn't be loaded or the adapter isn't a billed warehouse.
   */
  adapter_type?: string | null;
  command: string;
  duration_ms: number;
  finished_at: string;
  per_model: PerModelCostHistorical[];
  run_id: string;
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
  [k: string]: unknown;
}
