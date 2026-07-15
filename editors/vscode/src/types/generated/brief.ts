/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/brief.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Whether a brief section's underlying query succeeded and had data.
 *
 * The digest is composed section-by-section from independent typed queries over the state store and the decision ledger. Each section fails closed: a query that returns nothing renders as [`SectionAvailability::NoData`], and a source that is not wired into the state store at all renders as [`SectionAvailability::Unavailable`] with a note — never as a smoothed-over "all clear". A brief that claims more than the ledger holds poisons the whole surface, so the marker is machine-readable, not prose.
 */
export type SectionAvailability = "available" | "no_data" | "unavailable";
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
 * The verdict a policy rule (or the default posture) yields.
 *
 * Ordered by restrictiveness for incomparable-rule tie-breaking: `Deny` is a hard override (handled separately), and among non-deny verdicts `RequireReview` is more restrictive than `Allow`.
 */
export type PolicyEffect = "allow" | "require_review" | "deny";
/**
 * How the digest window was resolved.
 */
export type BriefSinceMode = "last" | "24h" | "7d";

/**
 * JSON output for `rocky brief` — the governor's estate digest.
 *
 * A typed projection of the state store and the policy-decision ledger over a time window: agent activity by principal, runs, drift, freshness, quality, cost, and the ranked queue of decisions still awaiting a human. It is composed template-first from typed queries — there is no narration layer — and every line item carries a ledger citation (`run_id`, `plan_id`, or the composite `decision_ref`) so a claim can be independently checked against `rocky audit` / `rocky replay`.
 */
export interface BriefOutput {
  /**
   * Agent- and human-authored policy decisions in the window, grouped by principal and effect.
   */
  agent_activity: BriefAgentActivitySection;
  /**
   * Autonomy-budget degradations and active policy freezes — the dynamic tightening currently in force across the estate.
   */
  autonomy: BriefAutonomySection;
  command: string;
  /**
   * Cost and budget burn across the window's runs.
   */
  cost: BriefCostSection;
  /**
   * Schema drift observed in the window.
   */
  drift: BriefDriftSection;
  /**
   * Decisions that resolved to `require_review` and still need a human, ranked.
   */
  escalations: BriefEscalationsSection;
  /**
   * Freshness / SLO status in the window.
   */
  freshness: BriefFreshnessSection;
  /**
   * RFC 3339 wall clock when the digest was generated (the window's upper bound).
   */
  generated_at: string;
  /**
   * Data-quality status in the window.
   */
  quality: BriefQualitySection;
  /**
   * Pipeline runs in the window, with the ones needing attention listed.
   */
  runs: BriefRunsSection;
  /**
   * How the window was resolved.
   */
  since_mode: BriefSinceMode;
  /**
   * RFC 3339 lower bound of the window. `null` only for a first-ever `--since last` with no stored cursor — the digest then spans all of recorded history.
   */
  since_timestamp?: string | null;
  version: string;
  [k: string]: unknown;
}
/**
 * Agent-activity section — the policy-decision ledger rolled up by principal.
 */
export interface BriefAgentActivitySection {
  allow: number;
  availability: SectionAvailability;
  /**
   * One roll-up per acting principal (`human` / `agent`).
   */
  by_principal: BriefPrincipalActivity[];
  /**
   * Every decision in the window, newest first, each fully cited.
   */
  decisions: BriefDecisionEntry[];
  deny: number;
  note?: string | null;
  require_review: number;
  total: number;
  [k: string]: unknown;
}
/**
 * Per-principal decision counts inside [`BriefAgentActivitySection`].
 */
export interface BriefPrincipalActivity {
  allow: number;
  deny: number;
  principal: PolicyPrincipal;
  require_review: number;
  total: number;
  [k: string]: unknown;
}
/**
 * One recorded policy decision, cited for the digest.
 *
 * `decision_ref` is the composite ledger key (`"{timestamp}|{plan_id}|{model}"`) — the stable identity a governor drills into via `rocky audit`. `plan_id` and `rule_id` are the other two citations: which plan the decision governed and which `[[policy.rules]]` entry won.
 */
export interface BriefDecisionEntry {
  capability: PolicyCapability;
  /**
   * Composite ledger key that uniquely identifies this decision.
   */
  decision_ref: string;
  effect: PolicyEffect;
  /**
   * The model the decision was about.
   */
  model: string;
  /**
   * The plan the decision governed.
   */
  plan_id: string;
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
 * Autonomy section — rules whose autonomy budget is currently exhausted (degraded to `require_review`) and policy freezes in force.
 *
 * This is a *current-state* projection over the whole decision ledger — each budget uses its own configured window, independent of the digest's `--since`. It fails closed: `unavailable` when the ledger or config can't be read, `no_data` when nothing is degraded or frozen.
 */
export interface BriefAutonomySection {
  /**
   * Policy freezes currently in force.
   */
  active_freezes: BriefActiveFreeze[];
  availability: SectionAvailability;
  /**
   * Rules whose budget is exhausted right now — each auto-degraded to `require_review` until its failures age out of the window.
   */
  degraded_rules: BriefDegradedRule[];
  note?: string | null;
  [k: string]: unknown;
}
/**
 * An active policy freeze inside [`BriefAutonomySection`].
 */
export interface BriefActiveFreeze {
  /**
   * RFC 3339 wall clock when the freeze was recorded — the citation.
   */
  frozen_at: string;
  /**
   * The freeze decision's `plan_id`.
   */
  plan_id: string;
  principal: PolicyPrincipal;
  /**
   * The scope selector the freeze targets.
   */
  scope: string;
  [k: string]: unknown;
}
/**
 * A budget-exhausted (degraded) rule inside [`BriefAutonomySection`].
 */
export interface BriefDegradedRule {
  /**
   * Verify-after failures counted in the window.
   */
  failures: number;
  /**
   * The rule's configured failure ceiling.
   */
  limit: number;
  /**
   * Index of the degraded `[[policy.rules]]` entry.
   */
  rule_id: number;
  /**
   * The rule's configured window (`7d`, `24h`, …).
   */
  window: string;
  [k: string]: unknown;
}
/**
 * Cost section — per-run cost re-derived over the window, plus budget burn.
 */
export interface BriefCostSection {
  /**
   * The billed-warehouse adapter cost was computed against, if resolvable from `rocky.toml`.
   */
  adapter_type?: string | null;
  availability: SectionAvailability;
  /**
   * Budget-burn status against the configured per-run `[budget] max_usd`, when one is set.
   */
  budget?: BriefBudgetStatus | null;
  note?: string | null;
  /**
   * Per-run cost in the window, priciest first. Each cites its `run_id`.
   */
  per_run: BriefRunCost[];
  run_count: number;
  total_bytes_scanned?: number | null;
  /**
   * Summed observed cost across the window. `null` when no run produced enough data to compute a cost (e.g. DuckDB, which has no billing).
   */
  total_cost_usd?: number | null;
  total_duration_ms: number;
  [k: string]: unknown;
}
/**
 * Budget-burn status against the configured per-run ceiling.
 */
export interface BriefBudgetStatus {
  /**
   * The configured `[budget] max_usd` per-run ceiling.
   */
  max_usd_per_run: number;
  /**
   * How many runs in the window exceeded the ceiling.
   */
  runs_over_budget: number;
  worst_run_cost_usd?: number | null;
  /**
   * The priciest run in the window (its citation), when a cost was computed.
   */
  worst_run_id?: string | null;
  [k: string]: unknown;
}
/**
 * Per-run cost inside [`BriefCostSection`].
 */
export interface BriefRunCost {
  bytes_scanned?: number | null;
  cost_usd?: number | null;
  duration_ms: number;
  run_id: string;
  [k: string]: unknown;
}
/**
 * Drift section — schema drift recorded in the state store.
 */
export interface BriefDriftSection {
  availability: SectionAvailability;
  /**
   * Drift events in the window, newest first.
   */
  events: BriefDriftEntry[];
  note?: string | null;
  [k: string]: unknown;
}
/**
 * A single schema-drift observation inside [`BriefDriftSection`].
 */
export interface BriefDriftEntry {
  /**
   * Human-readable description of the change.
   */
  change: string;
  /**
   * The DAG graph hash the change was observed against — the citation for this drift snapshot.
   */
  graph_hash: string;
  timestamp: string;
  [k: string]: unknown;
}
/**
 * Escalations section — `require_review` decisions still awaiting a human.
 */
export interface BriefEscalationsSection {
  availability: SectionAvailability;
  note?: string | null;
  /**
   * The pending decisions, ranked, each fully cited.
   */
  pending: BriefDecisionEntry[];
  /**
   * How the queue is ordered. `"recency"` in v0; blast-radius ranking (CLL × classification × staleness) is a later phase.
   */
  ranking: string;
  total: number;
  [k: string]: unknown;
}
/**
 * Freshness / SLO section, derived from recorded quality snapshots.
 */
export interface BriefFreshnessSection {
  availability: SectionAvailability;
  /**
   * Per-model freshness lag, worst first. Each cites its `run_id`.
   */
  models: BriefFreshnessEntry[];
  note?: string | null;
  [k: string]: unknown;
}
/**
 * A single model's freshness reading inside [`BriefFreshnessSection`].
 */
export interface BriefFreshnessEntry {
  freshness_lag_seconds: number;
  model_name: string;
  observed_at: string;
  run_id: string;
  [k: string]: unknown;
}
/**
 * Quality / check section, derived from recorded quality snapshots.
 */
export interface BriefQualitySection {
  availability: SectionAvailability;
  /**
   * Per-model quality readings, newest first. Each cites its `run_id`.
   */
  models: BriefQualityEntry[];
  note?: string | null;
  [k: string]: unknown;
}
/**
 * A single model's quality reading inside [`BriefQualitySection`].
 */
export interface BriefQualityEntry {
  /**
   * The highest per-column null rate observed for the model, if any columns were profiled.
   */
  max_null_rate?: number | null;
  model_name: string;
  observed_at: string;
  row_count: number;
  run_id: string;
  [k: string]: unknown;
}
/**
 * Runs section — the run ledger rolled up by status.
 */
export interface BriefRunsSection {
  /**
   * Runs that did not fully succeed, newest first — the exception view. Each cites its `run_id`.
   */
  attention: BriefRunEntry[];
  availability: SectionAvailability;
  failed: number;
  note?: string | null;
  partial_failure: number;
  succeeded: number;
  total: number;
  [k: string]: unknown;
}
/**
 * A single run needing attention inside [`BriefRunsSection`].
 */
export interface BriefRunEntry {
  /**
   * Models within the run that did not report `success`.
   */
  failed_models: BriefFailedModel[];
  finished_at: string;
  /**
   * The run's identifier — the citation to drill in via `rocky replay`.
   */
  run_id: string;
  started_at: string;
  status: string;
  trigger: string;
  [k: string]: unknown;
}
/**
 * A non-successful model execution inside a [`BriefRunEntry`].
 */
export interface BriefFailedModel {
  model_name: string;
  status: string;
  [k: string]: unknown;
}
