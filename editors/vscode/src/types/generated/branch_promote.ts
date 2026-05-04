/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/branch_promote.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Where the approval signature was produced.
 *
 * Reserved for future CI / OIDC paths. Today only the `Local` variant is emitted by the CLI.
 */
export type ApproverSource = "local" | "ci_oidc" | "pat";
/**
 * Algorithm tag for an [`ApprovalSignature`].
 *
 * The discriminator is on disk from day one so future "real" cryptographic signing variants slot in without migrating existing artifacts.
 */
export type SignatureAlgorithm = "blake3_canonical_json";
/**
 * Categorical kind of an [`AuditEvent`] emitted by `branch promote`.
 */
export type AuditEventKind = ("promote_started" | "promote_completed" | "promote_failed") | "approval_skipped";

/**
 * JSON output for `rocky branch promote`.
 */
export interface BranchPromoteOutput {
  /**
   * Approval artifacts loaded from disk that failed verification, with the reason for rejection. Surfaced even on a successful promote so operators can spot stale artifacts to clean up.
   */
  approvals_rejected: RejectedApproval[];
  /**
   * Approval artifacts that satisfied the gate at promote time. Empty when the gate was disabled (`required = false`) or skipped.
   */
  approvals_used: ApprovalArtifact[];
  /**
   * Audit-trail events emitted during this invocation, in order. At minimum: `PromoteStarted` plus one of `PromoteCompleted` / `PromoteFailed`. `ApprovalSkipped` precedes `PromoteStarted` when the gate was bypassed.
   */
  audit: AuditEvent[];
  branch: string;
  branch_state_hash: string;
  command: string;
  /**
   * True when every target's SQL succeeded.
   */
  success: boolean;
  /**
   * One entry per managed target the promote attempted, in dispatch order.
   */
  targets: PromoteTarget[];
  version: string;
  [k: string]: unknown;
}
/**
 * An approval artifact that was loaded from disk but rejected.
 */
export interface RejectedApproval {
  approval_id: string;
  detail: string;
  /**
   * One of: `bad_signature`, `state_hash_mismatch`, `expired`, `signer_not_allowed`, `parse_error`.
   */
  reason: string;
  [k: string]: unknown;
}
/**
 * On-disk approval record for a single approver against a single branch.
 *
 * One file per approver under `./.rocky/approvals/<branch>/<approval_id>.json`. `branch_state_hash` binds the approval to the branch's content-addressed state at sign time; if the branch's hash changes (config bytes change), every existing artifact for that branch becomes stale and `branch promote` will refuse to honour it.
 */
export interface ApprovalArtifact {
  /**
   * Sortable monotonic identifier — timestamp prefix + random tail.
   */
  approval_id: string;
  approver: ApproverIdentity;
  branch: string;
  branch_state_hash: string;
  message?: string | null;
  signature: ApprovalSignature;
  signed_at: string;
  [k: string]: unknown;
}
/**
 * Identity of an approver, captured at sign time.
 *
 * Email is sourced from `git config user.email`; name from `git config user.name`. Hostname is best-effort from the `hostname` crate and surfaced as an audit aid only — it is not part of the trust boundary.
 */
export interface ApproverIdentity {
  email: string;
  host: string;
  name?: string | null;
  source: ApproverSource;
  [k: string]: unknown;
}
/**
 * Signature attached to an [`ApprovalArtifact`].
 */
export interface ApprovalSignature {
  algorithm: SignatureAlgorithm;
  /**
   * Hex-encoded digest. For `Blake3CanonicalJson`, this is the 32-byte blake3 hash printed as 64 lowercase hex characters.
   */
  digest: string;
  [k: string]: unknown;
}
/**
 * Single audit-trail event emitted during `branch promote`.
 *
 * Routed to stdout JSON only in v1; persistent audit storage is a follow-up.
 */
export interface AuditEvent {
  actor: ApproverIdentity;
  at: string;
  branch: string;
  branch_state_hash: string;
  kind: AuditEventKind;
  /**
   * Free-form context. Populated for `ApprovalSkipped` to record the origin of the skip ("--skip-approval CLI flag" or "ROCKY_BRANCH_APPROVAL_SKIP=1"); empty for routine state events.
   */
  reason?: string | null;
  [k: string]: unknown;
}
/**
 * One per-target promote step in [`BranchPromoteOutput::targets`].
 */
export interface PromoteTarget {
  /**
   * Adapter / SQL error text when `succeeded` is `false`.
   */
  error?: string | null;
  /**
   * Fully-qualified branch source the promote read from (catalog.branch_schema.table).
   */
  source: string;
  /**
   * SQL statement dispatched to the adapter for this target.
   */
  statement: string;
  /**
   * Whether the per-target SQL succeeded. Failures abort the run; on a failure this is `false` for the failing target and absent for any targets that never started.
   */
  succeeded: boolean;
  /**
   * Fully-qualified production target (catalog.schema.table).
   */
  target: string;
  [k: string]: unknown;
}
