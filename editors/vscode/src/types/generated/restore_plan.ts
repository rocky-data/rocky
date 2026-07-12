/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/restore_plan.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky restore <target>` (plan mode).
 *
 * The plan has been written to the plan store; this reports its id and the resolved tombstone(s). Restoration is symmetric-caution gated like gc: the operator must `rocky review <plan-id> --approve` and then `rocky apply <plan-id>` — the plan itself writes nothing.
 */
export interface RestorePlanOutput {
  command: string;
  /**
   * Operator caveats (re-derivation source, hash-exactness gate, no-overwrite rule).
   */
  notes: string[];
  /**
   * The persisted plan id — pass to `rocky review` then `rocky apply`.
   */
  plan_id: string;
  /**
   * Number of tombstoned artifacts the plan restores.
   */
  restoration_count: number;
  /**
   * The resolved restorations.
   */
  restorations: RestorePlanRestoration[];
  /**
   * Always `true`: a restore plan is unconditionally review-gated.
   */
  review_required: boolean;
  /**
   * The user-supplied target the resolution matched.
   */
  target: string;
  /**
   * Total tombstoned bytes the plan restores.
   */
  total_bytes: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One tombstoned artifact scheduled for restoration inside a persisted `rocky restore` plan ([`RestorePlan`]).
 *
 * Captures the tombstone's full identity — enough to (a) re-locate the exact live tombstone at apply time (the plan is advisory; apply re-resolves against the live custody ledger), and (b) report the restoration to the reviewer. Identity is always the full `(run, model, path, hash)` tuple.
 */
export interface RestorePlanRestoration {
  /**
   * Content hash (hex) of the evicted bytes — the identity the restore re-computes and asserts equality against **before any write**.
   */
  blake3_hash: string;
  /**
   * Delta commit version the artifact was attached to, for correlation.
   */
  commit_version: number;
  /**
   * When the artifact was evicted (RFC 3339) — with the hash, this is the tombstone's ledger key.
   */
  evicted_at: string;
  /**
   * Object-store path the restore re-materializes to (the tombstoned location).
   */
  file_path: string;
  /**
   * The `rocky gc` plan that authorized the eviction (custody back-link).
   */
  gc_plan_id: string;
  /**
   * Input-closure hash captured on the tombstone; `null` when unrecorded.
   */
  input_hash?: string | null;
  /**
   * Input match-strength (`strong` / `heuristic`); `null` when unrecorded.
   */
  input_proof_class?: string | null;
  /**
   * Model that produced the evicted artifact.
   */
  model_name: string;
  /**
   * Recipe-identity hash captured on the tombstone; `null` when unrecorded.
   */
  recipe_hash?: string | null;
  /**
   * Run that produced it — half of the provenance key the restore replays from.
   */
  run_id: string;
  /**
   * Physical size of the evicted artifact in bytes.
   */
  size_bytes: number;
  [k: string]: unknown;
}
