/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/replay_execute.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky replay --at <run_id> --execute [--verify]`.
 *
 * Re-execution surface (single-model, DuckDB): for the targeted model(s) it reconstructs the recipe from the recorded [`rocky_core::state::ProvenanceRecord`] — never the working tree — re-executes the recipe's `SELECT` on an ephemeral in-memory DuckDB engine, and re-derives the output artifact's blake3. With `--verify` it compares that digest against the recorded output hash and emits a per-model verdict.
 *
 * Nothing is materialized to any warehouse schema: the re-executed rows are hashed in memory and discarded, so no production identity is ever touched (isolation is vacuous for a self-contained `SELECT`). Re-execution is limited to a *self-contained* recipe — one whose SQL reads no recorded upstream tables. A recipe with content upstreams is reported `non_replayable` here, because resolving inputs to their replayed upstream outputs is multi-model DAG replay (a later phase): recorded upstream bytes are never read, and current data is never silently substituted.
 */
export interface ReplayExecuteOutput {
  /**
   * Number of models whose re-execution reproduced the recorded output byte-for-byte. Always `0` when `--verify` was not requested.
   */
  bit_exact_count: number;
  command: string;
  /**
   * Total number of models considered (after any `--model` filter).
   */
  model_count: number;
  models: ReplayExecuteModelOutput[];
  run_id: string;
  /**
   * Recorded run status (`success`, `partial_failure`, ...).
   */
  status: string;
  /**
   * Whether `--verify` was requested (blake3 comparison performed).
   */
  verified: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * Per-model re-execution verdict inside a [`ReplayExecuteOutput`].
 */
export interface ReplayExecuteModelOutput {
  /**
   * The blake3 re-derived by this replay execution; `null` when the recipe was not executed (a `non_replayable` verdict).
   */
  computed_hash?: string | null;
  model_name: string;
  /**
   * Static-scan non-determinism flag (via `rocky_sql::determinism`). When `true`, a `diverged` verdict is expected rather than a failure.
   */
  nondeterministic: boolean;
  /**
   * Human-readable reasons for a `non_replayable` verdict, or a note on an expected `diverged`; empty on a clean `bit_exact`.
   */
  reasons: string[];
  /**
   * The recorded output blake3 carried on the provenance record; `null` when the record held no output hash.
   */
  recorded_hash?: string | null;
  /**
   * Rows produced by the re-execution; `null` when not executed.
   */
  rows?: number | null;
  /**
   * The verdict for this model. One of:
   *
   * - `bit_exact` — re-execution reproduced the recorded output blake3 (reachable only through a successful re-execution whose digest matched the recording); - `diverged` — re-execution succeeded but the blake3 differs (expected when `nondeterministic` is set; a genuine reproducibility gap otherwise); - `executed` — re-executed without `--verify`, so no comparison was made; - `non_replayable` — the recording alone was insufficient to re-execute the model (see `reasons`).
   *
   * A `value_equal_order_diff` verdict is reserved for a later order-insensitive refinement; v1 compares the raw blake3 only, so a row-order difference reports `diverged` (over-sensitivity fails safe).
   */
  verdict: string;
  [k: string]: unknown;
}
