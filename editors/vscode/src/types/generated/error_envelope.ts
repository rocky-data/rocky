/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/error_envelope.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Structured error body returned by the `rocky serve` HTTP API.
 *
 * Every non-2xx `/api/v1` response carries this envelope: the HTTP status line carries the error *class* (`400`/`401`/`404`/`409`/`500`/`503`) and the body carries a stable machine token plus a human message and an optional actionable hint. Embedders switch on [`code`](Self::code) and surface [`message`](Self::message) / [`remediation_hint`](Self::remediation_hint) to operators.
 *
 * Stable codes emitted today: `engine_not_ready` (no compile available yet), `engine_busy` (state locked by a running job — retryable), `model_not_found`, `job_not_found`, `mutation_in_progress` (a `run`/`apply` job already holds the mutation permit — carries [`running_job_id`](Self::running_job_id)), `bad_request`, `unauthorized`, `internal_error`.
 */
export interface ErrorEnvelope {
  /**
   * Stable machine token, e.g. `"model_not_found"`.
   */
  code: string;
  /**
   * Human-readable description of what went wrong.
   */
  message: string;
  /**
   * Actionable next step, or `null` when none applies.
   */
  remediation_hint?: string | null;
  /**
   * The `job_id` of the run/apply job currently holding the mutation permit, on a `409 mutation_in_progress` (and, when known, on a `503 engine_busy`). Omitted for every other error. Additive, serde-defaulted — embedders that predate it simply never see it.
   */
  running_job_id?: string | null;
  [k: string]: unknown;
}
