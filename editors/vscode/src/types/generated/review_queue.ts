/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/review_queue.schema.json
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
 * Who is attempting an action.
 *
 * `agent` is a non-human caller (an AI harness authoring, applying, or remediating). `human` is a person. In v0 the principal is supplied explicitly (`rocky policy check --principal …`); auto-detection is a later phase.
 */
export type PolicyPrincipal = "human" | "agent";

/**
 * JSON output for `rocky review --queue` — the pending-review work queue.
 *
 * Lists every `require_review` policy decision that has not yet been satisfied by a sign-off marker, ranked so the change most in need of a human floats to the top. The rank blends three signals: how much sits downstream of the model (blast radius), how disruptive the change class is (a breaking schema change outranks an additive one), and how long the escalation has waited (staleness). Each entry carries the exact `rocky review <plan_id> --approve` invocation that clears it — the queue is a triage view; the approval path is unchanged.
 */
export interface ReviewQueueOutput {
  command: string;
  /**
   * Count of outstanding `require_review` ledger rows excluded from the queue because their `plan_id` resolves to no persisted plan file — decision-only custody rows (e.g. refused drafts, auto-apply evaluations) that nothing could approve. They stay in the audit ledger; they are just not approvable queue items.
   */
  excluded_non_plan_rows: number;
  /**
   * The pending escalations, highest-priority first.
   */
  pending: ReviewQueueEntry[];
  /**
   * Human-readable description of the ordering, e.g. `"blast_radius × classification × staleness"`.
   */
  ranking: string;
  /**
   * Count of pending escalations in the queue.
   */
  total: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One pending `require_review` escalation inside [`ReviewQueueOutput`].
 */
export interface ReviewQueueEntry {
  /**
   * The exact command that clears this escalation.
   */
  approve_command: string;
  /**
   * Count of models that transitively consume this one — the blast radius the ranking weighs. `null` when the model could not be located in the compiled graph (e.g. it was removed), in which case the ranking treats the blast radius as zero and this entry says so.
   */
  blast_radius?: number | null;
  /**
   * The capability that was evaluated (its `schema_change.*` refinement is the change class the ranking weighs).
   */
  capability: PolicyCapability;
  /**
   * The change-class weight the ranking used (breaking > bare verb > additive / value-only).
   */
  classification_weight: number;
  /**
   * Composite ledger key (`"{timestamp}|{plan_id}|{model}"`) — the stable identity a governor drills into via `rocky audit --for`.
   */
  decision_ref: string;
  /**
   * The model the escalation is about.
   */
  model: string;
  /**
   * The plan whose approval clears this escalation.
   */
  plan_id: string;
  /**
   * Who authored the change (`human` / `agent`).
   */
  principal: PolicyPrincipal;
  /**
   * Human-readable explanation of how `require_review` was reached.
   */
  reason: string;
  /**
   * Index of the winning `[[policy.rules]]` entry, or `null` when the escalation came from the default posture.
   */
  rule_id?: number | null;
  /**
   * The composite priority score. Higher sorts first. Reported so a consumer can re-rank or explain the ordering without recomputing it.
   */
  score: number;
  /**
   * How long the escalation has waited, in whole seconds.
   */
  staleness_seconds: number;
  /**
   * RFC 3339 timestamp when the decision was recorded.
   */
  timestamp: string;
  [k: string]: unknown;
}
