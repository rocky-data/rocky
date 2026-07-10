/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/policy_freeze.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The verdict a policy rule (or the default posture) yields.
 *
 * Ordered by restrictiveness for incomparable-rule tie-breaking: `Deny` is a hard override (handled separately), and among non-deny verdicts `RequireReview` is more restrictive than `Allow`.
 */
export type PolicyEffect = "allow" | "require_review" | "deny";
/**
 * Who is attempting an action.
 *
 * `agent` is a non-human caller (an AI harness authoring, applying, or remediating). `human` is a person. In v0 the principal is supplied explicitly (`rocky policy check --principal …`); auto-detection is a later phase.
 */
export type PolicyPrincipal = "human" | "agent";

/**
 * JSON output for `rocky policy freeze` / `rocky policy unfreeze` — the kill switch.
 *
 * A freeze records a decision entry per matched principal into the existing policy-decision ledger; at the enforcement seam an active freeze forces `deny`. `unfreeze` records a superseding entry that lifts it. No config file is rewritten and no new table is created.
 */
export interface PolicyFreezeOutput {
  /**
   * `policy_freeze` or `policy_unfreeze`.
   */
  command: string;
  /**
   * One entry per affected principal.
   */
  entries: PolicyFreezeEntry[];
  /**
   * `true` when this call lifted a freeze rather than establishing one.
   */
  lifted: boolean;
  /**
   * RFC 3339 wall clock when the freeze/unfreeze was recorded.
   */
  recorded_at: string;
  /**
   * The scope selector the freeze targets (`any`, `layer=…`, `tag=…`, …).
   */
  scope: string;
  version: string;
  [k: string]: unknown;
}
/**
 * One principal's freeze/unfreeze record inside a [`PolicyFreezeOutput`].
 */
export interface PolicyFreezeEntry {
  /**
   * Composite ledger key citation (`"{timestamp}|{plan_id}|{scope}"`).
   */
  decision_ref: string;
  /**
   * The recorded effect (`deny` for a freeze, `allow` for an unfreeze).
   */
  effect: PolicyEffect;
  /**
   * The freeze decision's `plan_id`.
   */
  plan_id: string;
  /**
   * The principal whose actions were frozen/unfrozen.
   */
  principal: PolicyPrincipal;
  /**
   * Human-readable description of the freeze/unfreeze.
   */
  reason: string;
  [k: string]: unknown;
}
