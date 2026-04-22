/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/trace.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky trace <run_id|latest>`.
 *
 * Sibling to [`ReplayOutput`] but with offset-relative timings so downstream consumers (Dagster asset Gantt, custom dashboards) can render the run as a timeline without re-deriving the run start.
 */
export interface TraceOutput {
  command: string;
  finished_at: string;
  /**
   * Number of concurrent lanes the scheduler used during this run. `1` for fully sequential pipelines, `>1` when the DAG had independent models that the executor materialized in parallel.
   */
  lane_count: number;
  models: TraceModelEntry[];
  run_duration_ms: number;
  run_id: string;
  started_at: string;
  status: string;
  trigger: string;
  version: string;
  [k: string]: unknown;
}
/**
 * One model execution entry inside [`TraceOutput`]. `start_offset_ms` is the wall-clock offset from the run start; `lane` identifies the concurrency lane for Gantt rendering (entries on the same lane never overlap in time).
 */
export interface TraceModelEntry {
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
  duration_ms: number;
  /**
   * Greedy first-fit concurrency lane. Populated by the renderer; deserializing clients don't need to supply it.
   */
  lane?: number;
  model_name: string;
  rows_affected?: number | null;
  sql_hash: string;
  start_offset_ms: number;
  status: string;
  [k: string]: unknown;
}
