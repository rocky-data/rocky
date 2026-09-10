/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/policy_show.schema.json
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
 * JSON output for `rocky policy show`, and the body of `GET /api/v1/policy`: the policy plane as configured and as it stands.
 *
 * The rules carry their position in `[[policy.rules]]` as `id`, the only identity a rule has today and the number `rocky policy check` reports as `matched_rule`. `freezes` is every freeze in force, from the decision ledger and, when the project keeps them, the durable freeze markers; `freeze_sources` says which of the two was read. A source that exists but cannot be read is an error, never an empty list: an empty list would say "nothing is frozen" for a plane whose freezes could not be read at all.
 */
export interface PolicyRulesOutput {
  /**
   * Always `"policy_show"`.
   */
  command: string;
  /**
   * Whether `rocky.toml` carries a `[policy]` block. `false` is the default posture: no rules, `default_agent_effect` as the engine defaults it.
   */
  configured: boolean;
  /**
   * The effect an agent gets when no rule matches.
   */
  default_agent_effect: PolicyEffect;
  freeze_sources: PolicyFreezeSources;
  /**
   * Every freeze in force, ledger entries first, then markers.
   */
  freezes: PolicyFreezeInForce[];
  policy_version: number;
  /**
   * The rules in file order.
   */
  rules: PolicyRuleEntry[];
  version: string;
  [k: string]: unknown;
}
/**
 * Which freeze sources the report read.
 */
export interface PolicyFreezeSources {
  /**
   * How the decision ledger was read.
   *
   * - `"read"` — a local backend, read in full. - `"absent"` — a local backend with no state store yet. Proven absent, not assumed: a path that exists but cannot be read is an error. - `"local_mirror"` — a remote `[state]` backend. What was read is the local mirror, which may be stale or empty; the remote authority was NOT downloaded, because this is a read-only route and the download replaces the local ledger. A freeze recorded by another pod can be missing here while an apply, which does download first, still denies. - `"not_consulted"` — no `[policy]` block, so nothing is in force and the enforcement gate reads no ledger either.
   */
  ledger: string;
  /**
   * How the durable freeze markers were read.
   *
   * - `"read"` — the `[state]` backend has a durable object tier, read in full. Reads are NOT gated on `freeze_marker_writes`: that flag gates writes only, and an existing marker stays enforced after it is turned off, so a reader that honoured it would hide a live freeze. - `"not_configured"` — the backend keeps no durable object tier. - `"not_consulted"` — no `[policy]` block, as above.
   */
  markers: string;
  [k: string]: unknown;
}
/**
 * One freeze in force.
 */
export interface PolicyFreezeInForce {
  /**
   * The marker's id.
   */
  freeze_id?: string | null;
  /**
   * The ledger decision's plan id.
   */
  plan_id?: string | null;
  /**
   * The frozen principal. Absent ONLY on a marker whose body could not be read: the loader widens such a marker to scope `any` and to both principals so it fails closed. It is not a marker that deliberately froze both, and a reader must not present it as one — the `reason` says the body was unreadable.
   */
  principal?: PolicyPrincipal | null;
  reason: string;
  /**
   * The scope selector as given to `rocky policy freeze`; `any` is every model.
   */
  scope: string;
  /**
   * When the freeze was recorded, when the source recorded it.
   */
  since?: string | null;
  /**
   * `"ledger"` (a `rocky policy freeze` decision) or `"marker"` (a durable freeze marker in the remote object tier).
   */
  source: string;
  [k: string]: unknown;
}
/**
 * One `[[policy.rules]]` entry.
 */
export interface PolicyRuleEntry {
  /**
   * The rolling failure ceiling that degrades this rule's effect.
   */
  autonomy_budget?: PolicyAutonomyBudgetOutput | null;
  capability: PolicyCapability;
  effect: PolicyEffect;
  /**
   * Zero-based position in `[[policy.rules]]`; the `matched_rule` that `rocky policy check` reports.
   */
  id: number;
  principal: PolicyPrincipal;
  scope: PolicyRuleScopeOutput;
  /**
   * Post-apply verification: the named checks that must pass after a mutation this rule governs. A failing or absent named check halts the apply. Two rules that differ only here govern differently, so the document carries it; without it a reader cannot tell them apart.
   *
   * A rule's `conditions` is deliberately NOT carried. The engine parses it and never evaluates it, its shape is unbounded, and `${VAR}` in a config string is resolved before parsing — so an authored condition can hold a resolved secret that no key-based redaction could find. It decides nothing, so nothing is lost by leaving it out.
   */
  verify_after?: string[];
  [k: string]: unknown;
}
/**
 * A rule's autonomy budget: `failures` within `window` degrade its effect.
 */
export interface PolicyAutonomyBudgetOutput {
  failures: number;
  window: string;
  [k: string]: unknown;
}
/**
 * What a rule matches. Every field is as authored; an empty list or `None` means the field does not narrow the rule.
 */
export interface PolicyRuleScopeOutput {
  any: boolean;
  classifications: string[];
  contracted?: boolean | null;
  exclude_classifications: string[];
  layer?: string | null;
  /**
   * The blast-radius ceiling: the rule matches only a model with at most this many downstreams.
   */
  max_downstreams?: number | null;
  models: string[];
  tags: {
    [k: string]: string;
  };
  [k: string]: unknown;
}
