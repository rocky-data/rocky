/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/restore_apply.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky apply <restore-plan>`.
 *
 * Reports the restoration outcome per planned tombstone: restored (rebuilt hash-exact, or verified already-present), refused (with the exact fail-closed reason), or already restored (idempotent no-op verified against the live bytes).
 */
export interface RestoreApplyOutput {
  /**
   * Content hashes of plan entries whose tombstone was already consumed by a prior restore and whose live bytes re-verified — idempotent no-ops.
   */
  already_restored: string[];
  /**
   * Total bytes restored (including verified-already-present bytes).
   */
  bytes_restored: number;
  command: string;
  /**
   * Operator caveats (re-derivation engine, verification order, overwrite rule).
   */
  notes: string[];
  plan_id: string;
  /**
   * Artifacts refused by a fail-closed guard; the ledger was not reinstated for them. Usually pre-write, but verified content-addressed bytes may already be on disk (a lost ledger-reinstatement race) and are safe to reuse on re-apply — a refusal means the ledger was not changed, not that nothing was written.
   */
  refused: RestoreRefusedOutput[];
  /**
   * Count of refused artifacts.
   */
  refused_count: number;
  /**
   * Artifacts restored — hash-verified bytes at the tombstoned path and a reinstated ledger row.
   */
  restored: RestoredOutput[];
  /**
   * Count of restored artifacts.
   */
  restored_count: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One artifact `rocky apply <restore-plan>` **refused** to restore: the ledger row was NOT reinstated (no restore of record). Most refusals are pre-write — a fail-closed verification caught a mismatch before anything was written — but a refusal can also follow a successful, hash-verified content-addressed write whose atomic ledger reinstatement then lost a race. In that case the verified bytes are already at the tombstoned path and are safe to reuse on a re-run (the re-apply verifies and completes idempotently). A refusal therefore guarantees only that the ledger was not changed, not that nothing was written.
 */
export interface RestoreRefusedOutput {
  blake3_hash: string;
  model_name: string;
  /**
   * Why the restoration was refused. A recomputed-hash mismatch is surfaced honestly — it means the tombstone's rebuild claim failed, which is itself a finding, never papered over.
   */
  reason: string;
  run_id: string;
  size_bytes: number;
  [k: string]: unknown;
}
/**
 * One artifact successfully restored by `rocky apply <restore-plan>` — the recomputed blake3 matched the tombstoned hash, the bytes are present at the tombstoned path, and the ledger row was reinstated.
 */
export interface RestoredOutput {
  blake3_hash: string;
  /**
   * `true` when the restore physically wrote the rebuilt bytes to the tombstoned path; `false` when verified bytes were already present there (an idempotent "already present, verified" restore — the ledger row was still reinstated if missing).
   */
  bytes_written: boolean;
  file_path: string;
  /**
   * Always `true` on this list: the rebuilt bytes' blake3 equalled the tombstoned hash **before** any write became visible.
   */
  hash_verified: boolean;
  model_name: string;
  run_id: string;
  size_bytes: number;
  /**
   * Human-readable restoration outcome.
   */
  status: string;
  [k: string]: unknown;
}
