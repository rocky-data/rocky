/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/history.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky history` (all runs view).
 *
 * When invoked with `--model <name>`, the dispatch returns `ModelHistoryOutput` instead. The two shapes share the version/command header but differ in their primary collection field, which is why they are separate types rather than one enum.
 */
export interface HistoryOutput {
  command: string;
  count: number;
  runs: RunHistoryRecord[];
  version: string;
  [k: string]: unknown;
}
/**
 * One run from the state store, mirroring `rocky_core::state::RunRecord` with serializable field types (no enums or non-JSON-friendly Rust types).
 *
 * The governance audit fields (`triggering_identity`, `session_source`, `git_commit`, `git_branch`, `idempotency_key`, `target_catalog`, `hostname`, `rocky_version`) are only populated in the `rocky history --audit` shape; the default shape continues to emit just the existing 7 fields. The `#[serde(skip_serializing_if = "…")]` attributes keep the default payload byte-identical to schema v5 unless `--audit` flips the fields on.
 */
export interface RunHistoryRecord {
  /**
   * Duration in milliseconds (`finished_at - started_at`).
   */
  duration_ms: number;
  /**
   * `git symbolic-ref --short HEAD` at claim time, or `None` on detached HEAD.
   */
  git_branch?: string | null;
  /**
   * `git rev-parse HEAD` at claim time.
   */
  git_commit?: string | null;
  /**
   * Machine hostname that produced the run.
   */
  hostname?: string | null;
  /**
   * The `--idempotency-key` value supplied to `rocky run`, if any.
   */
  idempotency_key?: string | null;
  /**
   * Per-model execution details for this run.
   */
  models: RunModelRecord[];
  models_executed: number;
  /**
   * `CARGO_PKG_VERSION` of the `rocky` binary, or `"<pre-audit>"` on schema-v5 rows that predate the audit trail.
   */
  rocky_version?: string | null;
  run_id: string;
  /**
   * Session origin — `"cli"`, `"dagster"`, `"lsp"`, or `"http_api"`. Emitted as the lowercase variant string so JSON consumers can match on it without knowing the Rust enum shape.
   */
  session_source?: string | null;
  started_at: string;
  status: string;
  /**
   * Best-available target catalog — the `catalog_template` for replication pipelines, the literal `target.catalog` for transformation/quality/snapshot/load.
   */
  target_catalog?: string | null;
  trigger: string;
  /**
   * Resolved caller identity (Unix `$USER` / Windows `$USERNAME`). `None` in CI / container contexts where no human caller is discernible.
   */
  triggering_identity?: string | null;
  [k: string]: unknown;
}
/**
 * Per-model execution record embedded in [`RunHistoryRecord`].
 */
export interface RunModelRecord {
  duration_ms: number;
  model_name: string;
  rows_affected?: number | null;
  status: string;
  [k: string]: unknown;
}
