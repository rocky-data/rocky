/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/gc_plan.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky gc --derivable` (plan mode — no `--dry-run`).
 *
 * The plan has been written to the plan store; this reports its id and the scoped proposal. Deletion is symmetric-caution gated: the operator must `rocky review <plan-id> --approve` and then `rocky apply <plan-id>` — the plan itself never deletes.
 */
export interface GcPlanOutput {
  command: string;
  /**
   * Number of derivable artifacts proposed for eviction.
   */
  eviction_count: number;
  /**
   * The proposed evictions.
   */
  evictions: GcPlanEviction[];
  /**
   * Operator caveats (e.g. re-verification at apply, scope). Each eviction records what a restore will need; `rocky restore` itself is a planned follow-up.
   */
  notes: string[];
  /**
   * The persisted plan id — pass to `rocky review` then `rocky apply`.
   */
  plan_id: string;
  /**
   * Always `true`: a `gc` plan is unconditionally review-gated before apply.
   */
  review_required: boolean;
  /**
   * Total bytes proposed for reclamation.
   */
  total_bytes: number;
  version: string;
  [k: string]: unknown;
}
/**
 * One artifact scheduled for eviction inside a persisted `rocky gc` plan ([`GcPlan`]).
 *
 * Captures the identity of a single derivable artifact — enough to (a) re-locate its exact ledger row at apply time, (b) re-verify eligibility against the live ledger, and (c) write a complete restore tombstone. The recipe-identity triple is captured at plan time from the producing execution.
 */
export interface GcPlanEviction {
  /**
   * Content hash (hex) of the artifact bytes — the eviction unit and the identity a restore would re-compute and compare against.
   */
  blake3_hash: string;
  /**
   * Delta commit version the artifact was attached to.
   */
  commit_version: number;
  /**
   * Environment hash of the producing execution; `null` when unrecorded.
   */
  env_hash?: string | null;
  /**
   * Object-store path of the artifact — the byte location a physical reclamation deletes and a restore would re-materialize to.
   */
  file_path: string;
  /**
   * Hash-scheme version of the producing execution; `null` when unrecorded.
   */
  hash_scheme?: string | null;
  /**
   * Input-closure hash of the producing execution; `null` when unrecorded.
   */
  input_hash?: string | null;
  /**
   * Input match-strength (`strong` / `heuristic`); `null` when unrecorded.
   */
  input_proof_class?: string | null;
  /**
   * Model that produced the artifact.
   */
  model_name: string;
  /**
   * Recipe-identity hash of the producing execution; `null` for a pre-identity build.
   */
  recipe_hash?: string | null;
  /**
   * Run that produced it — half of the provenance key a restore would replay from.
   */
  run_id: string;
  /**
   * Physical size of the artifact in bytes.
   */
  size_bytes: number;
  /**
   * When the artifact was written (RFC 3339).
   */
  written_at: string;
  [k: string]: unknown;
}
