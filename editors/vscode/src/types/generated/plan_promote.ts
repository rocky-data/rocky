/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/plan_promote.schema.json
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
  | "breaking_changes_gate_skipped"
  | "promote_plan_created";

/**
 * Persisted payload for a `rocky plan promote` run plan.
 *
 * Written to `.rocky/plans/<plan_id>.json` by `rocky plan promote` and read back by `rocky apply <plan_id>` (dispatched as `PlanKind::Promote`).
 *
 * ## What is persisted vs re-derived
 *
 * - **Persisted**: branch name + refs, state hash at plan time, approval artifacts used/rejected, breaking-change findings, per-target SQL statements, and the plan-time audit events. - **Re-derived at apply time**: nothing related to approvals or breaking-change gate — those gates ran at plan time and their outcomes are captured here. The warehouse adapter is resolved at apply time (to call `execute_statement`), but discovery is NOT re-run.
 *
 * ## Branch state drift at apply time
 *
 * `branch_state_hash` reflects the branch metadata + config bytes at plan time. If the branch's config or metadata changes between `rocky plan promote` and `rocky apply`, the persisted hash differs from the live hash. By default, `rocky apply` does **not** re-check the hash — the gates already ran at plan time, and that is the point of the plan/apply split. Operators who need a strict re-check can re-run `rocky plan promote` to produce a fresh plan.
 *
 * Warehouse table contents are not covered by `branch_state_hash` in v1 — if a branch's tables are mutated between plan and apply, `rocky apply` will promote whatever data is in the branch schema at apply time.
 */
export interface PromotePlan {
  /**
   * Whether `--allow-breaking` was set at plan time.
   */
  allow_breaking?: boolean;
  /**
   * Approval artifacts loaded from disk that failed verification at plan time.
   */
  approvals_rejected: RejectedApproval[];
  /**
   * Approval artifacts that satisfied the gate at plan time.
   */
  approvals_used: ApprovalArtifact[];
  /**
   * Git ref that `base_ref` was resolved to at plan time (e.g. `"main"`).
   */
  base_ref: string;
  /**
   * Branch name being promoted.
   */
  branch_name: string;
  /**
   * Content-addressed hash of the branch metadata + config bytes at plan time. Stored for audit purposes; not re-validated at apply time.
   */
  branch_state_hash: string;
  /**
   * Semantic breaking-change findings produced by the pre-promote gate at plan time. Empty when the gate ran and found no breaking changes; absent when the gate was skipped (compile failure on either side).
   */
  breaking_changes?: BreakingFinding[] | null;
  /**
   * When this plan was persisted.
   */
  created_at: string;
  /**
   * Git HEAD SHA at plan time — informational for audit purposes.
   */
  head_ref: string;
  /**
   * Plan-time audit events (approvals gate + breaking-change gate outcomes). Apply-time events (`PromoteStarted`, `PromoteCompleted`, `PromoteFailed`) are appended in `BranchPromoteOutput.audit` at apply time.
   */
  plan_audit: AuditEvent[];
  /**
   * Per-model SQL plan, in dispatch order. SQL is persisted verbatim so `rocky apply` executes the exact statements generated at plan time.
   */
  targets: PromoteTargetPlan[];
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
 * A classified finding produced by [`diff_project_ir`].
 */
export interface BreakingFinding {
  change: BreakingChange;
  severity: BreakingSeverity;
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
 * Per-model promote step captured in a [`PromotePlan`].
 *
 * Mirrors the shape of [`PromoteTarget`] but contains only plan-time fields (`target`, `source`, `statement`). Execution outcome (`succeeded`, `error`) is added at apply time and lives on [`PromoteTarget`].
 *
 * `statement` is persisted verbatim so `rocky apply` executes the **exact** SQL generated at plan time — skipping re-discovery and ensuring the `plan_id` digest is invalidated if the SQL would differ.
 */
export interface PromoteTargetPlan {
  /**
   * Fully-qualified branch source the promote will read from (catalog.branch_schema.table).
   */
  source: string;
  /**
   * `CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>` SQL, dialect-quoted at plan time.
   */
  statement: string;
  /**
   * Fully-qualified production target (catalog.schema.table).
   */
  target: string;
  [k: string]: unknown;
}
