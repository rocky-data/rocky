/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/replay_execute.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky replay --at <run_id> --execute [--verify]`.
 *
 * Re-execution surface. Each model's recipe is reconstructed from its recorded [`rocky_core::state::ProvenanceRecord`] — never the working tree — re-executed, and its output artifact's blake3 re-derived. With `--verify` that digest is compared against the recorded output hash and a per-model verdict is emitted. Execution runs on an in-memory DuckDB engine by default; with `--warehouse` it runs on the configured live warehouse instead, materializing replayed outputs into an isolated replay schema (see `replay_schema`) and encoding the recomputed artifact with the live table's physical column mapping so the digest is directly comparable to what the content-addressed writer recorded.
 *
 * Two modes share this shape:
 *
 * - `--model <m>` re-executes a single, *self-contained* recipe on a throwaway engine. A recipe with recorded content upstreams is `non_replayable` in this mode — resolving those inputs is the DAG mode below. - no `--model` re-executes the *whole run* in topological order on one shared engine, materializing each upstream's **replayed** output so a downstream `SELECT` reads the replayed bytes (never the recorded object-store bytes, never production). A downstream whose in-run upstream could not be replayed is `non_replayable` (fail-closed cascade); an upstream not produced by any model in this run, or a mutable-source watermark, is likewise `non_replayable` rather than substituted.
 *
 * No production identity is ever touched in either mode: the local path's entire in-memory engine is an ephemeral replay namespace discarded after the run, and the warehouse path writes only into the isolated `replay_schema`, dropped after the run unless `--keep` is passed.
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
  /**
   * Isolated warehouse schema the replayed outputs were materialized into (`--warehouse` only; absent on the local re-execution path). Replay never writes to the production location of any recorded target — every replayed table lands in this namespace, one schema per catalog touched.
   */
  replay_schema?: string | null;
  /**
   * Whether the replay namespace was removed after the run (`--warehouse` only). `true` when no replay schema remains on the warehouse; `false` when `--keep` was passed or a drop failed.
   */
  replay_schema_dropped?: boolean | null;
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
