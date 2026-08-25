/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/review_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Where the approval signature was produced.
 *
 * Reserved for future CI / OIDC paths. Today only the `Local` variant is emitted by the CLI.
 */
export type ApproverSource = "local" | "ci_oidc" | "pat";

/**
 * JSON output for `rocky review <plan-id> --status` — the marker oracle.
 *
 * A read-only projection of the plan's review state: whether a well-formed sign-off marker naming the plan exists, who approved it and when, and the plan's product binding when it carries one. The fulfillment runner polls this instead of probing the marker path (the marker file itself stays an engine-internal artifact).
 */
export interface ReviewStatusOutput {
  /**
   * Best-effort git identity of the approver, from the marker.
   */
  approver?: ApproverIdentity | null;
  /**
   * Count of breaking-severity findings the approver signed off on.
   */
  breaking_change_count?: number | null;
  /**
   * Always `"review_status"`.
   */
  command: string;
  /**
   * The plan's kind (e.g. `"ai_authored"`, `"run"`, `"backfill"`).
   */
  kind: string;
  /**
   * The plan whose review state was read (64-char blake3 hex).
   */
  plan_id: string;
  /**
   * Product identity from the plan payload, when the plan is product-bound (opaque; never parsed by the engine).
   */
  product_id?: string | null;
  /**
   * Whether a well-formed sign-off marker naming this plan exists. `false` means the plan awaits review; a malformed or mismatched marker is an ERROR from the command, never reported as a status.
   */
  reviewed: boolean;
  /**
   * When the approval was recorded, from the marker.
   */
  reviewed_at?: string | null;
  /**
   * Approved-spec digest from the plan payload, when product-bound. Applying such a plan requires `rocky apply --expect-spec-digest`.
   */
  spec_digest?: string | null;
  version: string;
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
