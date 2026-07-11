/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/audit_for.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Whether a brief section's underlying query succeeded and had data.
 *
 * The digest is composed section-by-section from independent typed queries over the state store and the decision ledger. Each section fails closed: a query that returns nothing renders as [`SectionAvailability::NoData`], and a source that is not wired into the state store at all renders as [`SectionAvailability::Unavailable`] with a note — never as a smoothed-over "all clear". A brief that claims more than the ledger holds poisons the whole surface, so the marker is machine-readable, not prose.
 */
export type SectionAvailability = "available" | "no_data" | "unavailable";
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
 * What `rocky audit --for <selector>` resolved its selector to.
 *
 * The selector is resolved in priority order: a 64-char hex string with a plan file on disk is a [`AuditSubjectKind::Plan`]; a string matching a `run_id` in the run ledger is a [`AuditSubjectKind::Run`]; a string the decision ledger keys rows by is likewise a [`AuditSubjectKind::Plan`] (the plan file may be gone, or the id may be a decision-only custody key that never had one); anything else is treated as a [`AuditSubjectKind::Model`] name.
 */
export type AuditSubjectKind = "model" | "run" | "plan";

/**
 * JSON output for `rocky audit --for <table|run|plan>` — the custody chain.
 *
 * A one-query drill-down assembled link by link from the data Rocky already records: who proposed the change and what the policy plane decided ([`Self::decisions`]), what the plan changed ([`Self::plan`]), which runs materialized it ([`Self::runs`]), what a post-apply verification found ([`Self::verify_after`]), and what sits downstream in its blast radius ([`Self::blast_radius`]).
 *
 * Every link fails closed the same way the estate brief does: a link whose signal is genuinely not recorded renders [`SectionAvailability::Unavailable`] with a note, never a fabricated or assumed value. Post-apply verification outcomes are persisted as decision-ledger custody rows (non-empty `verify_after`), so [`Self::verify_after`] renders them when they exist and says "not recorded" when none exist for the subject. The run ledger is not keyed to policy decisions, so a `run` selector cannot join back to a decision. The blast radius is recomputed from the current compiled graph (a live query, not a stored snapshot).
 */
export interface AuditForOutput {
  /**
   * What sits downstream of the subject — the CLL blast radius.
   */
  blast_radius: AuditChainBlastRadius;
  command: string;
  /**
   * Who proposed the change and what the policy plane decided — the persisted decision ledger, scoped to this subject.
   */
  decisions: AuditChainDecisions;
  /**
   * What the governing plan changed (its embedded per-model change classification).
   */
  plan: AuditChainPlan;
  /**
   * Whether the selector matched anything at all (a decision, a run, a plan file, or a model in the graph). `false` when nothing referenced it — every link is then empty and says why.
   */
  resolved: boolean;
  /**
   * Runs that materialized the subject.
   */
  runs: AuditChainRuns;
  /**
   * The selector as supplied on the command line.
   */
  subject: string;
  /**
   * What the selector resolved to.
   */
  subject_kind: AuditSubjectKind;
  /**
   * What a post-apply verification found — the verification custody rows recorded against the subject's plan.
   */
  verify_after: AuditChainVerify;
  version: string;
  [k: string]: unknown;
}
/**
 * Blast-radius link of the custody chain: the models that transitively consume the subject, recomputed from the current compiled graph.
 */
export interface AuditChainBlastRadius {
  availability: SectionAvailability;
  /**
   * Direct downstream consumers (one hop).
   */
  direct: string[];
  /**
   * The model the blast radius was computed for.
   */
  model?: string | null;
  note?: string | null;
  /**
   * Size of the transitive closure.
   */
  total: number;
  /**
   * All transitive downstream consumers (the full closure, sorted).
   */
  transitive: string[];
  [k: string]: unknown;
}
/**
 * Decision link of the custody chain: the policy-decision ledger scoped to the subject (by model for a model selector, by plan_id for a plan selector).
 */
export interface AuditChainDecisions {
  availability: SectionAvailability;
  /**
   * The matching decisions, newest first.
   */
  entries: AuditDecisionEntry[];
  note?: string | null;
  total: number;
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
/**
 * Plan link of the custody chain: what the governing plan changed, read from the plan file's embedded change-classification.
 */
export interface AuditChainPlan {
  availability: SectionAvailability;
  /**
   * The per-model change classification the plan carried — the persisted stand-in for the typed diff. The full field-level `diff_project_ir` output is not persisted for run plans, only this classification is.
   */
  changes: AuditPlanChange[];
  /**
   * Whether the base↔head change classification was available when the plan was authored. `false` means every planned model was treated as a breaking change (fail-closed), and `changes` reflects that.
   */
  diff_available: boolean;
  /**
   * The plan kind (`run` / `ai_authored` / …).
   */
  kind?: string | null;
  note?: string | null;
  /**
   * The plan that governed the subject (the newest one, for a model selector). `null` when no plan file could be located.
   */
  plan_id?: string | null;
  /**
   * The plan's authoring principal.
   */
  principal?: PolicyPrincipal | null;
  [k: string]: unknown;
}
/**
 * One model's change classification inside [`AuditChainPlan`].
 */
export interface AuditPlanChange {
  /**
   * The change class the plan recorded for this model (`schema_change.additive` / `schema_change.breaking` / `value_change` / a bare verb).
   */
  capability: PolicyCapability;
  model: string;
  [k: string]: unknown;
}
/**
 * Run link of the custody chain: runs that materialized the subject.
 */
export interface AuditChainRuns {
  availability: SectionAvailability;
  note?: string | null;
  /**
   * Matching runs, newest first.
   */
  runs: AuditRunEntry[];
  total: number;
  [k: string]: unknown;
}
/**
 * One run inside [`AuditChainRuns`].
 */
export interface AuditRunEntry {
  finished_at: string;
  run_id: string;
  started_at: string;
  status: string;
  /**
   * Best-effort caller identity recorded on the run.
   */
  triggering_identity?: string | null;
  [k: string]: unknown;
}
/**
 * Verify-after link of the custody chain.
 *
 * Post-apply verification outcomes are persisted to the decision ledger as custody rows with a non-empty `verify_after` check list — the two-step `rocky apply` gate writes one per verified apply, and the drift auto-apply path writes them under its `autoapply-verify:<run_id>` plan id. This link renders those rows for the subject's plan. When none exist the link says so plainly ("not recorded") — never a smoothed-over "verification passed".
 */
export interface AuditChainVerify {
  availability: SectionAvailability;
  /**
   * The verification outcomes, newest first.
   */
  entries: AuditVerifyEntry[];
  note?: string | null;
  /**
   * Count of verification custody rows found for the subject.
   */
  total: number;
  [k: string]: unknown;
}
/**
 * One post-apply verification outcome inside [`AuditChainVerify`] — a decision-ledger custody row with a non-empty `verify_after` check list.
 *
 * The ledger records the required check names, the aggregate verdict (`allow` = every named check passed, `deny` = a check failed or was absent and the apply halted), and a human-readable reason; per-check pass/fail detail beyond that lives only in the reason string, which is rendered verbatim rather than re-parsed.
 */
export interface AuditVerifyEntry {
  /**
   * The named post-apply checks the verification required.
   */
  checks: string[];
  /**
   * Whether the verification passed (`allow` custody row) or failed (`deny`).
   */
  passed: boolean;
  /**
   * The plan id the custody row was filed under (the applied plan, or the auto-apply path's `autoapply-verify:<run_id>`).
   */
  plan_id: string;
  /**
   * The recorded outcome, verbatim from the custody row.
   */
  reason: string;
  /**
   * RFC 3339 timestamp when the verification outcome was recorded.
   */
  timestamp: string;
  [k: string]: unknown;
}
