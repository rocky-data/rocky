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
 * Where a [`PrincipalId`] came from.
 *
 * P1 knows four sources, and every one is self-asserted. RV4-P2 adds signed sources (a local key, CI OIDC). C2 adds `serve_token` for the HTTP API. A new variant is a change in meaning for an older binary, so the phase that adds one bumps the state schema.
 */
export type PrincipalIdSource = "flag" | "env" | "mcp_profile" | "default";

/**
 * JSON output for `rocky audit` — the agent-policy decision ledger.
 *
 * Lists every policy decision recorded at a mutating enforcement seam (`rocky apply` / promote), oldest first. Reads are never recorded, so this is exclusively the trail of *governed mutations* the plane evaluated.
 */
export interface AuditOutput {
  command: string;
  /**
   * Every recorded policy decision, oldest first. Under `product`, only the rows whose `model` is that product's output model. Under `filter`, only the rows that match it.
   */
  decisions: AuditDecisionEntry[];
  /**
   * The `--actor` / `--since` filter applied, absent when neither was given.
   */
  filter?: AuditFilter | null;
  /**
   * The product the ledger was filtered to (`--product <name>`), absent when the whole ledger is listed.
   */
  product?: AuditProductScope | null;
  /**
   * How many rows in range carry no principal id (written before ids existed) and so an `--actor <id>` filter dropped them. `0` without an `--actor` filter, and under `--actor unrecorded`, which lists them.
   */
  unattributed_skipped: number;
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
   * The model the decision was about. A resolved `${VAR}` value prints as `${NAME}` (#1919).
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
   * The id of the actor behind the decision (RV4-P1). `null` means unrecorded: the row was written before ids existed. `unnamed` means nobody named the actor.
   */
  principal_id?: string | null;
  /**
   * Where the id came from (`flag`, `env`, `mcp_profile`, `default`). `null` when the id is unrecorded.
   */
  principal_id_source?: PrincipalIdSource | null;
  /**
   * Whether anything verified the id. Always `false` today: ids are self-asserted until signed approvals exist.
   */
  principal_id_verified: boolean;
  /**
   * Human-readable explanation of how the effect was reached. A resolved `${VAR}` value prints as `${NAME}` (#1919).
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
 * The `--actor` / `--since` filter of `rocky audit` (RV4-P1).
 */
export interface AuditFilter {
  /**
   * The principal id the rows were filtered to. `unrecorded` selects the rows with no id.
   */
  actor?: string | null;
  /**
   * The inclusive lower bound, as an RFC 3339 UTC timestamp. A row is kept when its timestamp is at or after this.
   */
  since?: string | null;
  [k: string]: unknown;
}
/**
 * The product filter of `rocky audit --product <name>`, resolved from the product's spec before the ledger is read.
 *
 * A product owns exactly one output model (`product.output.model`, which defaults to the product name), and the ledger records one row per plan per model, so the product's rows are the rows about that model. No plan file or journal is consulted.
 */
export interface AuditProductScope {
  /**
   * The product name as given.
   */
  name: string;
  /**
   * The output model the rows were filtered to.
   */
  output_model: string;
  [k: string]: unknown;
}
