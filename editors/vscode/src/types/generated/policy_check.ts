/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/policy_check.schema.json
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
  | "restore"
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
 * JSON output for `rocky policy check`.
 *
 * Explain-mode only: reports the effect the agent policy plane *would* resolve for a `(principal, capability, model)` triple, the winning rule (if any), and why. It does not gate any real command in v0.
 */
export interface PolicyCheckOutput {
  /**
   * The capability that was checked.
   */
  capability: PolicyCapability;
  command: string;
  /**
   * Resolved effect: `allow`, `require_review`, or `deny`.
   */
  effect: PolicyEffect;
  /**
   * Zero-based index of the winning rule in `[[policy.rules]]`, or `null` when the decision came from a short-circuit (`read`) or the default posture (no rule matched).
   */
  matched_rule?: number | null;
  /**
   * The model that was checked.
   */
  model: string;
  /**
   * The compiled model attributes the matcher read.
   */
  model_attributes: PolicyModelAttributes;
  /**
   * The principal that was checked (`human` / `agent`).
   */
  principal: PolicyPrincipal;
  /**
   * Human-readable explanation of how the effect was reached.
   */
  reason: string;
  version: string;
  [k: string]: unknown;
}
/**
 * The compiled attributes of the checked model, echoed back so the explain output is self-contained.
 */
export interface PolicyModelAttributes {
  /**
   * Distinct column-classification values present on the model.
   */
  classifications: string[];
  /**
   * Whether the model sits behind a contract (best-effort: a sibling `.contract.toml` exists).
   */
  contracted: boolean;
  /**
   * Direct downstream-consumer count (models that `depends_on` this one). Informational — the `max_downstreams` ceiling reads [`Self::reachable_downstreams`].
   */
  downstreams: number;
  /**
   * Medallion/semantic layer (the model's `layer` tag), if any.
   */
  layer?: string | null;
  /**
   * Transitive downstream reachability — the full blast radius (direct + indirect), excluding the model itself. This is what a rule's `max_downstreams` ceiling is compared against. `null` when the blast radius could not be computed (the ceiling then fails closed).
   */
  reachable_downstreams?: number | null;
  /**
   * Model-level governance tags.
   */
  tags: {
    [k: string]: string;
  };
  [k: string]: unknown;
}
