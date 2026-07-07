/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/replay_check.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky replay --at <run_id> --check`.
 *
 * Read-only replayability audit: classifies every model in a recorded run as replayable or not, using only the state-store ledger — the model's [`rocky_core::state::ProvenanceRecord`] (which embeds the canonical `ModelIr`) and the content-addressed artifact rows. Nothing is executed and the working tree is never read: the recording is the source of truth.
 *
 * A model is `replayable` when its provenance record exists, its embedded IR parses under the current engine, and every declared input resolves from the ledger. The `nondeterministic` flag on each model is orthogonal to the verdict — a nondeterministic model is still replayable, but a future re-execution may legitimately diverge.
 */
export interface ReplayCheckOutput {
  command: string;
  /**
   * Total number of models considered (after any `--model` filter).
   */
  model_count: number;
  models: ReplayCheckModelOutput[];
  /**
   * `true` iff every model in the run is replayable.
   */
  replayable: boolean;
  /**
   * Number of those models classified replayable.
   */
  replayable_count: number;
  run_id: string;
  /**
   * Recorded run status (`success`, `partial_failure`, ...).
   */
  status: string;
  version: string;
  [k: string]: unknown;
}
/**
 * Per-model replayability verdict inside a [`ReplayCheckOutput`].
 */
export interface ReplayCheckModelOutput {
  /**
   * Whether a provenance record was found for this `(run, model)`.
   */
  has_provenance: boolean;
  /**
   * Per-input resolvability, one entry per declared upstream.
   */
  inputs: ReplayCheckInputOutput[];
  /**
   * Whether the embedded canonical `ModelIr` deserialized under the current engine. `false` both when there is no provenance and when a provenance record's IR failed to parse (an IR forward-compat break).
   */
  ir_parseable: boolean;
  model_name: string;
  /**
   * Static-scan non-determinism flag (via `rocky_sql::determinism`): the model's SQL contains a volatile construct (`now()`, `current_timestamp`, `random()`, an unresolved function, ...). Orthogonal to `verdict`.
   */
  nondeterministic: boolean;
  /**
   * Match-strength label carried on the provenance record (`strong` or `heuristic`); `null` when no provenance was found.
   */
  proof_class?: string | null;
  /**
   * Human-readable reasons the model is not replayable; empty when replayable.
   */
  reasons: string[];
  /**
   * `replayable` or `non_replayable`.
   */
  verdict: string;
  [k: string]: unknown;
}
/**
 * Resolvability of one declared upstream inside a [`ReplayCheckModelOutput`].
 */
export interface ReplayCheckInputOutput {
  /**
   * `content` (a recorded blake3 in the artifact ledger) or `watermark` (a freshness signal over a mutable source).
   */
  kind: string;
  /**
   * Why the input does not resolve; `null` when resolvable.
   */
  reason?: string | null;
  /**
   * Whether this input resolves from the ledger for a deterministic replay.
   */
  resolvable: boolean;
  /**
   * Fully-qualified `catalog.schema.table` identity of the upstream.
   */
  upstream_key: string;
  [k: string]: unknown;
}
