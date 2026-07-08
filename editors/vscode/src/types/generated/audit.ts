/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/audit.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The class of action a policy rule governs.
 *
 * `read` is always allowed (short-circuit). The mutating verbs (`propose` … `quarantine`) name coarse operations; `schema_change.additive`, `schema_change.breaking`, and `value_change` are *refinements* of the apply/promote verbs — a rule naming a bare verb (`apply`/`promote`) matches those refinements too, but a rule naming a refinement matches only that exact refinement.
 */
export type PolicyCapability =
  | "read"
  | "propose"
  | "apply"
  | "promote"
  | "backfill"
  | "gc"
  | "retry"
  | "quarantine"
  | "schema_change.additive"
  | "schema_change.breaking"
  | "value_change";
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
 * JSON output for `rocky audit` — the agent-policy decision ledger.
 *
 * Lists every policy decision recorded at a mutating enforcement seam (`rocky apply` / promote), oldest first. Reads are never recorded, so this is exclusively the trail of *governed mutations* the plane evaluated.
 */
export interface AuditOutput {
  command: string;
  /**
   * Every recorded policy decision, oldest first.
   */
  decisions: AuditDecisionEntry[];
  version: string;
  [k: string]: unknown;
}
/**
 * One recorded policy decision in the [`AuditOutput`] ledger.
 */
export interface AuditDecisionEntry {
  /**
   * The capability that was evaluated.
   */
  capability: PolicyCapability;
  /**
   * The resolved verdict (`allow` / `require_review` / `deny`).
   */
  effect: PolicyEffect;
  /**
   * The model the decision was about.
   */
  model: string;
  /**
   * The plan the decision governed.
   */
  plan_id: string;
  /**
   * Who was acting (`human` / `agent`).
   */
  principal: PolicyPrincipal;
  /**
   * Human-readable explanation of how the effect was reached.
   */
  reason: string;
  /**
   * Index of the winning `[[policy.rules]]` entry, or `null` for the default posture.
   */
  rule_id?: number | null;
  /**
   * RFC 3339 timestamp when the decision was recorded.
   */
  timestamp: string;
  [k: string]: unknown;
}
