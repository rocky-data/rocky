/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/audit_scorecard.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Whether a brief section's underlying query succeeded and had data.
 *
 * The digest is composed section-by-section from independent typed queries over the state store and the decision ledger. Each section fails closed: a query that returns nothing renders as [`SectionAvailability::NoData`], and a source that is not wired into the state store at all renders as [`SectionAvailability::Unavailable`] with a note — never as a smoothed-over "all clear". A brief that claims more than the ledger holds poisons the whole surface, so the marker is machine-readable, not prose.
 */
export type SectionAvailability = "available" | "no_data" | "unavailable";
/**
 * The dimension a scorecard groups policy decisions by.
 *
 * Each maps to a field the ledger persists on every decision: `principal` groups by who acted (`agent` / `human`), `rule` by the winning `[[policy.rules]]` index, and `scope` by the concrete model the decision was about (the ledger records the matched model, not the rule's scope predicates).
 */
export type ScorecardDimension = "principal" | "rule" | "scope";

/**
 * JSON output for `rocky audit --scorecard` — a trust-calibration digest.
 *
 * A decisions-by-group aggregation over the persisted policy-decision ledger, windowed by `--window`. It is the evidence base for widening or tightening autonomy, and it informs human judgment only — nothing here is wired to any automatic policy change.
 *
 * Only metrics the ledger actually persists are computed. Post-apply verification outcomes *are* persisted (custody rows with a non-empty `verify_after` check list), so [`Self::verify_after`] reports their pass rate when any fall in the window. Reverts and escalation-resolution latency are not persisted; they are declared in [`Self::unavailable_metrics`] as `unavailable` with the reason, never faked into a number — and `verify_after_pass_rate` joins that list only when no verification row falls in the window. A ledger read failure renders the whole scorecard `unavailable` rather than a smoothed-over zero.
 */
export interface AuditScorecardOutput {
  /**
   * Whether the scorecard could be composed at all: `unavailable` when the ledger could not be read (fail-closed), `no_data` when no decision falls in the window, `available` otherwise.
   */
  availability: SectionAvailability;
  /**
   * The grouping dimension (`--by`).
   */
  by: ScorecardDimension;
  command: string;
  /**
   * One row per group, ranked by decision count descending.
   */
  groups: ScorecardGroup[];
  note?: string | null;
  /**
   * Total decisions in the window, across all groups.
   */
  total_decisions: number;
  /**
   * Metrics the persisted ledger cannot support for this window, declared plainly rather than computed. Each is `unavailable` with the reason it cannot be derived.
   */
  unavailable_metrics: ScorecardUnavailableMetric[];
  /**
   * Aggregate of the post-apply verification custody rows in the window (rows with a non-empty `verify_after` check list). `null` when no verification row falls in the window — then `verify_after_pass_rate` is declared in [`Self::unavailable_metrics`] instead.
   */
  verify_after?: ScorecardVerifyAfter | null;
  version: string;
  /**
   * The window as requested (`all`, `30d`, `24h`, …).
   */
  window: string;
  /**
   * Lower bound of the window (RFC 3339), or `null` for all-time.
   */
  window_start?: string | null;
  [k: string]: unknown;
}
/**
 * One group's decision aggregate in an [`AuditScorecardOutput`].
 *
 * Every rate is a ratio over [`Self::total`] and is independently recomputable from [`Self::decision_refs`], which lists the exact ledger keys (`timestamp|plan_id|model`) that composed the group — so the aggregate summarizes citable rows, never an unverifiable number.
 */
export interface ScorecardGroup {
  /**
   * `allow / total` — the proposal acceptance rate.
   */
  acceptance_rate: number;
  /**
   * Decisions the plane allowed outright.
   */
  allow: number;
  /**
   * The ledger keys that composed this group (`timestamp|plan_id|model`), newest first — the citations backing every count above.
   */
  decision_refs: string[];
  /**
   * `deny / total` — the denial rate.
   */
  denial_rate: number;
  /**
   * Decisions the plane denied.
   */
  deny: number;
  /**
   * The group label: a principal (`agent` / `human`), a rule (`rule 3` / `default posture`), or a model/scope name — per the scorecard's `by`.
   */
  key: string;
  /**
   * Decisions the plane escalated to human review.
   */
  require_review: number;
  /**
   * `require_review / total` — the escalation rate.
   */
  review_rate: number;
  /**
   * Decisions in this group within the window.
   */
  total: number;
  [k: string]: unknown;
}
/**
 * A metric the scorecard cannot compute from the persisted ledger for this window.
 *
 * Declared explicitly (not silently omitted) so the honesty is machine-readable: a consumer sees the metric name, that it is `unavailable`, and exactly why.
 */
export interface ScorecardUnavailableMetric {
  /**
   * Always [`SectionAvailability::Unavailable`].
   */
  availability: SectionAvailability;
  /**
   * The metric identifier (`reverts`, `escalation_latency`, or `verify_after_pass_rate` when no verification row falls in the window).
   */
  metric: string;
  /**
   * Why the metric cannot be derived from the persisted ledger.
   */
  note: string;
  [k: string]: unknown;
}
/**
 * The verify-after aggregate inside an [`AuditScorecardOutput`]: pass/fail counts over the post-apply verification custody rows in the window.
 *
 * Each row's aggregate verdict is its recorded `effect` (`allow` = every named check passed, `deny` = a check failed or was absent and the apply halted), so the pass rate summarizes exactly what the ledger persists.
 */
export interface ScorecardVerifyAfter {
  /**
   * Rows whose verdict was `deny` (a named check failed or was absent).
   */
  failed: number;
  /**
   * `passed / total`.
   */
  pass_rate: number;
  /**
   * Rows whose verdict was `allow` (every named check passed).
   */
  passed: number;
  /**
   * Verification custody rows in the window.
   */
  total: number;
  [k: string]: unknown;
}
