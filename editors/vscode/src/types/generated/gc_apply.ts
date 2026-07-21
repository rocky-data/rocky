/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/gc_apply.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky apply <gc-plan>`.
 *
 * Reports the eviction outcome per planned artifact. Deletion is the highest -stakes operation, so the output is exhaustive: what was evicted (with its tombstone), what was refused (with the live failing checks), and what was an idempotent no-op (already evicted by a prior apply).
 */
export interface GcApplyOutput {
  /**
   * Content hashes of plan entries that were already absent from the ledger (a prior apply evicted them) — idempotent no-ops, not failures.
   */
  already_evicted: string[];
  /**
   * Total bytes evicted.
   */
  bytes_evicted: number;
  /**
   * Total bytes refused.
   */
  bytes_refused: number;
  command: string;
  /**
   * Artifacts evicted — tombstone written, ledger row retired.
   */
  evicted: GcEvictedOutput[];
  /**
   * Count of evicted artifacts.
   */
  evicted_count: number;
  /**
   * Operator caveats (e.g. physical-reclamation reachability). Each eviction's tombstone records everything `rocky restore <target>` needs to rebuild the artifact and verify it hash-exact.
   */
  notes: string[];
  plan_id: string;
  /**
   * Artifacts refused because they were no longer derivable at apply time.
   */
  refused: GcRefusedOutput[];
  /**
   * Count of refused artifacts.
   */
  refused_count: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One artifact successfully evicted by `rocky apply <gc-plan>` — a tombstone was recorded and the ledger row retired.
 */
export interface GcEvictedOutput {
  blake3_hash: string;
  model_name: string;
  /**
   * `true` when the bytes were physically deleted through the object-store adapter; `false` when the physical delete was deferred or failed (a safe leaked orphan — the tombstone still records everything `rocky restore <target>` needs to rebuild and verify the artifact).
   */
  physical_reclaimed: boolean;
  /**
   * Human-readable physical-reclamation outcome (`deleted`, `deferred: …`, or `failed: …`).
   */
  physical_status: string;
  run_id: string;
  size_bytes: number;
  /**
   * Always `true` on this list — the durable restore tombstone was written atomically with the ledger-row retirement before anything else happened.
   */
  tombstone_recorded: boolean;
  [k: string]: unknown;
}
/**
 * One artifact `rocky apply <gc-plan>` **refused** to evict because it was no longer derivable at apply time (the fail-closed re-verification caught ledger drift since plan time — e.g. a new reference appeared).
 */
export interface GcRefusedOutput {
  blake3_hash: string;
  /**
   * The eligibility checks as re-evaluated at apply time, so the refusal is auditable rather than asserted.
   */
  failed_checks: GcCheckOutput[];
  model_name: string;
  /**
   * Why the eviction was refused (the failing check summary).
   */
  reason: string;
  run_id: string;
  size_bytes: number;
  [k: string]: unknown;
}
/**
 * One printed eligibility check inside a [`GcCandidateOutput`].
 */
export interface GcCheckOutput {
  /**
   * Stable check id: `recipe_recorded`, `recipe_produces_output`, `replayable`, `unreferenced`, `policy_allows`, or `age_threshold`.
   */
  check: string;
  /**
   * Why the check reached that verdict — the auditable justification.
   */
  detail: string;
  /**
   * Whether the check passed. A candidate is derivable only when all six are `true`.
   */
  passed: boolean;
  [k: string]: unknown;
}
