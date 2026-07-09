/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/policy_test.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The verdict a policy rule (or the default posture) yields.
 *
 * Ordered by restrictiveness for incomparable-rule tie-breaking: `Deny` is a hard override (handled separately), and among non-deny verdicts `RequireReview` is more restrictive than `Allow`.
 */
export type PolicyEffect = "allow" | "require_review" | "deny";
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
 * Who is attempting an action.
 *
 * `agent` is a non-human caller (an AI harness authoring, applying, or remediating). `human` is a person. In v0 the principal is supplied explicitly (`rocky policy check --principal …`); auto-detection is a later phase.
 */
export type PolicyPrincipal = "human" | "agent";

/**
 * JSON output for `rocky policy test` — the scenario-assertion runner.
 *
 * Runs every `[[policy.tests]]` scenario through the real policy evaluator and reports, per scenario, whether the resolved effect matched the expectation. A non-empty `failed` count fails the command (non-zero exit), so a policy edit that would silently open a hole is caught in CI.
 */
export interface PolicyTestOutput {
  command: string;
  /**
   * How many scenarios resolved to a different effect than expected.
   */
  failed: number;
  /**
   * How many scenarios resolved to their expected effect.
   */
  passed: number;
  /**
   * Per-scenario results, in declaration order.
   */
  results: PolicyTestResult[];
  /**
   * Total scenarios asserted.
   */
  total: number;
  version: string;
  [k: string]: unknown;
}
/**
 * The outcome of one `[[policy.tests]]` scenario.
 */
export interface PolicyTestResult {
  /**
   * The effect the evaluator actually resolved.
   */
  actual: PolicyEffect;
  /**
   * The capability that was checked.
   */
  capability: PolicyCapability;
  /**
   * The effect the scenario expected.
   */
  expected: PolicyEffect;
  /**
   * Zero-based index of the rule that decided `actual`, or `null` when the decision came from a short-circuit (`read`) or the default posture.
   */
  matched_rule?: number | null;
  /**
   * The synthetic model name the scenario targeted.
   */
  model: string;
  /**
   * The scenario's name.
   */
  name: string;
  /**
   * `true` when the resolved effect equalled `expected`.
   */
  passed: boolean;
  /**
   * The principal that was checked.
   */
  principal: PolicyPrincipal;
  /**
   * The evaluator's explanation of how `actual` was reached — the decisive context on a failure.
   */
  reason: string;
  [k: string]: unknown;
}
