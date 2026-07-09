/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/gc.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky gc --derivable --dry-run`.
 *
 * A read-only inventory of Rocky-managed content-addressed artifacts that are provably rebuildable — *derivable* — and therefore reclamation candidates. Nothing is deleted or planned for deletion: this surface is inventory-only, so the whole product is the report.
 *
 * The candidate universe is the content-addressed artifact ledger, grouped by content hash (each distinct hash is one physical artifact; managed bytes count each hash once). An artifact is `derivable` only when **all five** eligibility checks hold — recipe recorded, replayable, unreferenced, policy allows, past the age threshold. Every check fails closed (any doubt keeps the artifact non-derivable) and is reported per candidate, so each verdict is auditable rather than asserted.
 *
 * Scope caveat surfaced in [`Self::notes`]: refcounts see *Rocky's* pointers only. A warehouse-side reference Rocky never recorded (a BI extract, a notebook `SELECT INTO`) is invisible here; the age threshold is the mitigation, and this release measures written-age, not read-recency.
 */
export interface GcReportOutput {
  /**
   * Number of distinct content hashes considered.
   */
  artifact_count: number;
  /**
   * One entry per distinct content hash, newest write first.
   */
  candidates: GcCandidateOutput[];
  command: string;
  /**
   * Physical bytes of the derivable subset (distinct content hashes whose five checks all pass).
   */
  derivable_bytes: number;
  /**
   * How many of those are derivable.
   */
  derivable_count: number;
  /**
   * [`Self::derivable_bytes`] as a percentage of [`Self::managed_bytes`]; `null` when there are no managed bytes (nothing to divide by).
   */
  derivable_pct?: number | null;
  /**
   * Total physical bytes of Rocky-managed artifacts, counting each distinct content hash once.
   */
  managed_bytes: number;
  /**
   * The minimum written-age (in days) an artifact must reach to pass the age/activity check.
   */
  min_age_days: number;
  /**
   * Report-wide caveats an operator must read before trusting the numbers (Rocky-external references, estimate labeling, unwired policy plane).
   */
  notes: string[];
  /**
   * Whether read-activity tracking backed the age/activity check. Always `false` in this release: the age check is written-age only (see [`GcCheckOutput`]), stated conservatively rather than inferred.
   */
  read_tracking_available: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * One reclamation candidate inside a [`GcReportOutput`] — a single content-addressed artifact (identified by its content hash) with its five printed eligibility checks.
 */
export interface GcCandidateOutput {
  /**
   * Content hash (hex) of the artifact bytes — the reclamation unit.
   */
  blake3_hash: string;
  /**
   * The five eligibility checks, each with its pass/fail and a human-readable justification. Order is stable: `recipe_recorded`, `replayable`, `unreferenced`, `policy_allows`, `age_threshold`.
   */
  checks: GcCheckOutput[];
  /**
   * `true` iff every one of [`Self::checks`] passed.
   */
  derivable: boolean;
  /**
   * Input match-strength label carried on the provenance record (`strong` or `heuristic`); `null` when no provenance was found.
   */
  input_proof_class?: string | null;
  /**
   * Model that produced the artifact.
   */
  model_name: string;
  /**
   * Estimated cost to rebuild the artifact via replay.
   */
  rebuild_cost: GcRebuildCostOutput;
  /**
   * The recipe-identity key (hex) of the producing execution — the "what exact program produced this?" id. `null` when the producing execution predates recipe-identity capture; the provenance record still carries the canonical program.
   */
  recipe_id?: string | null;
  /**
   * How many ledger rows point at these bytes. `1` means Rocky holds a single reference (reclaimable on that axis); `> 1` means the bytes are shared (a branch or replayed run) and must never be evicted.
   */
  refcount: number;
  /**
   * Run that produced it (joins to the provenance + execution records).
   */
  run_id: string;
  /**
   * Physical size of the artifact in bytes.
   */
  size_bytes: number;
  /**
   * When the artifact was written (RFC 3339). Doubles as the conservative last-access proxy: Rocky has no read-tracking on this adapter, so write-time is the only defensible recency signal.
   */
  written_at: string;
  [k: string]: unknown;
}
/**
 * One printed eligibility check inside a [`GcCandidateOutput`].
 */
export interface GcCheckOutput {
  /**
   * Stable check id: `recipe_recorded`, `replayable`, `unreferenced`, `policy_allows`, or `age_threshold`.
   */
  check: string;
  /**
   * Why the check reached that verdict — the auditable justification.
   */
  detail: string;
  /**
   * Whether the check passed. A candidate is derivable only when all five are `true`.
   */
  passed: boolean;
  [k: string]: unknown;
}
/**
 * Estimated rebuild cost for a [`GcCandidateOutput`].
 *
 * Derived from the *recorded* build's metrics via the same cost model `rocky cost` uses — a replay re-runs the recipe, so its original execution's duration and scanned bytes are the honest predictor. Always an estimate ([`Self::estimated`] is always `true`), never a measured rebuild.
 */
export interface GcRebuildCostOutput {
  /**
   * Always `true`: this figure is modeled from the recorded build, not measured by re-running it.
   */
  estimated: boolean;
  /**
   * Estimated USD to rebuild, priced by `compute_observed_cost_usd` over the recorded metrics; `null` when the config or adapter can't price it (no `rocky.toml`, or a non-billed adapter).
   */
  estimated_usd?: number | null;
  /**
   * Bytes scanned by the recorded build; `null` when the adapter didn't report a figure (mirrors [`rocky_core::state::ModelExecution::bytes_scanned`]).
   */
  source_bytes_scanned?: number | null;
  /**
   * Duration of the recorded build in milliseconds — the wall-clock a replay would repeat.
   */
  source_duration_ms: number;
  [k: string]: unknown;
}
