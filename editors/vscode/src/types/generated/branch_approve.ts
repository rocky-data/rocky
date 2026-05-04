/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/branch_approve.schema.json
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
 * JSON output for `rocky branch approve`.
 */
export interface ApproveOutput {
  artifact: ApprovalArtifact;
  /**
   * Where the artifact landed on disk.
   */
  artifact_path: string;
  command: string;
  version: string;
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
