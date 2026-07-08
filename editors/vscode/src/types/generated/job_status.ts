/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/job_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The kind of long-running operation a job wraps.
 */
export type JobKind = "run" | "plan" | "apply";
/**
 * Lifecycle state of a job.
 */
export type JobState = "queued" | "running" | "succeeded" | "failed";

/**
 * Status of a `rocky serve` long-running job (`GET /api/v1/jobs/{id}`).
 *
 * The presentation type over `rocky-core`'s `PersistedJob`: it carries the same lifecycle facts with typed [`JobKind`]/[`JobState`] enums for embedder ergonomics, and — once the job is terminal — the canonical `RunOutput` / `PlanOutput` / `ApplyOutput` the underlying subprocess emitted, embedded **verbatim** in [`result`](Self::result). An embedder switches on [`kind`](Self::kind) to know which schema `result` conforms to (the same `run` / `plan` / `apply` schemas the CLI exports).
 */
export interface JobStatus {
  /**
   * Failure detail when [`state`](Self::state) is [`JobState::Failed`], else `null`.
   */
  error?: string | null;
  /**
   * When the job reached a terminal state (RFC 3339), or `null` if not yet.
   */
  finished_at?: string | null;
  /**
   * Opaque job identifier (as returned by `POST /api/v1/jobs/{kind}`).
   */
  job_id: string;
  /**
   * Which operation the job wraps.
   */
  kind: JobKind;
  /**
   * The advisory, spoofable `X-Rocky-Principal` recorded for audit, or `null`. Never an authorization input under the single-shared-secret auth ceiling.
   */
  principal?: string | null;
  /**
   * The canonical output of the underlying `rocky <kind>` command, embedded verbatim once the job is terminal (`null` while queued/running). Its shape is the `run` / `plan` / `apply` schema selected by [`kind`](Self::kind).
   */
  result?: {
    [k: string]: unknown;
  };
  /**
   * When execution started (RFC 3339), or `null` if not yet.
   */
  started_at?: string | null;
  /**
   * Current lifecycle state.
   */
  state: JobState;
  /**
   * When the job was submitted (RFC 3339).
   */
  submitted_at: string;
  [k: string]: unknown;
}
