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
 * A single typed semantic change between two `ProjectIr` snapshots.
 *
 * Each variant carries the minimum identifying context (model + column + before/after values) needed for a CLI / PR-preview surface to render a useful message without re-loading either IR.
 */
export type BreakingChange =
  | {
      kind: "model_removed";
      model: string;
      [k: string]: unknown;
    }
  | {
      kind: "model_added";
      model: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      data_type: string;
      kind: "column_dropped";
      model: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      data_type: string;
      kind: "column_added";
      model: string;
      nullable: boolean;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_type_changed";
      model: string;
      /**
       * `true` when the new type cannot represent every value of the old type (Int64 → Int32, Decimal precision shrink, Timestamp → Date).
       */
      narrowing: boolean;
      new_type: string;
      old_type: string;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_nullability_changed";
      model: string;
      new_nullable: boolean;
      old_nullable: boolean;
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_reordered";
      model: string;
      new_index: number;
      old_index: number;
      [k: string]: unknown;
    }
  | {
      kind: "materialization_strategy_changed";
      model: string;
      new_strategy: string;
      old_strategy: string;
      [k: string]: unknown;
    }
  | {
      /**
       * Which key changed: `unique_key`, `timestamp_column`, `partition_by`, `time_column`, `granularity`, `target_lag`, `update_columns`, `storage_prefix`, or `partition_columns`.
       */
      key_kind: string;
      kind: "materialization_key_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "replication_columns_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "partition_by_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      kind: "target_renamed";
      model: string;
      new: string;
      old: string;
      [k: string]: unknown;
    }
  | {
      kind: "source_changed";
      model: string;
      new: string[];
      old: string[];
      [k: string]: unknown;
    }
  | {
      column: string;
      kind: "column_mask_changed";
      model: string;
      new_strategy?: string | null;
      old_strategy?: string | null;
      [k: string]: unknown;
    }
  | {
      kind: "lakehouse_format_changed";
      model: string;
      new: string;
      old: string;
      [k: string]: unknown;
    }
  | {
      kind: "sql_body_changed";
      model: string;
      [k: string]: unknown;
    };
/**
 * Severity classification for a single semantic change.
 */
export type BreakingSeverity = "breaking" | "warning" | "info";
/**
 * Categorical kind of an [`AuditEvent`] emitted by `branch promote`.
 */
export type AuditEventKind =
  | ("promote_started" | "promote_completed" | "promote_failed")
  | "approval_skipped"
  | "breaking_changes_blocked"
  | "breaking_changes_allowed"
  | "breaking_changes_gate_skipped";

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
  /**
   * Semantic breaking-change findings produced by the pre-promote gate. Empty when the gate ran and found no breaking changes; absent when the gate was skipped (compile failure on either side). When present and non-empty either the promote was blocked or `--allow-breaking` was set — see the audit trail for which.
   */
  breaking_changes?: BreakingFinding[] | null;
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
 * Email is sourced from `git config user.email`; name from `git config user.name`. Hostname is best-effort from the `hostname` crate and surfaced as an audit aid only — it is not part of the trust boundary. Set `ROCKY_SCRUB_HOST` in the environment to replace the hostname with `"redacted"` when recording public demos.
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
  /**
   * Breaking-change findings carried by `BreakingChangesBlocked` and `BreakingChangesAllowed` events. Always absent for other kinds.
   */
  breaking_changes?: BreakingFinding[] | null;
  kind: AuditEventKind;
  /**
   * Free-form context. Populated for `ApprovalSkipped` to record the origin of the skip, and for `BreakingChangesGateSkipped` to record why the gate could not run (e.g. "base ref did not compile"). Empty for routine state events.
   */
  reason?: string | null;
  [k: string]: unknown;
}
/**
 * A classified finding produced by [`diff_project_ir`].
 */
export interface BreakingFinding {
  change: BreakingChange;
  severity: BreakingSeverity;
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
