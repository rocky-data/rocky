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
   * The pipeline this run executed (`rocky run --pipeline <name>`), when recorded. `None` for model-only or backfill runs. Not audit-gated — it is an operational join key, always emitted when present.
   */
  pipeline?: string | null;
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
   * The scheduler submission id — `rocky tick` stamps it via `ROCKY_SUBMISSION_ID`, so this is the join key to a tick's `TickOutput.executed[].submission_id`. `None` for manually launched runs.
   */
  submission_id?: string | null;
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
  /**
   * The recipe-identity triple recorded for this execution, when present. See [`RecipeIdentityView`].
   */
  recipe_identity?: RecipeIdentityView | null;
  rows_affected?: number | null;
  status: string;
  [k: string]: unknown;
}
/**
 * The recipe-identity triple surfaced on a model record — the answer to "what exact program, over what inputs, in what environment produced this?".
 *
 * Read back from the persisted [`rocky_core::state::ModelExecution`]. Every field is optional: a record written before the triple was captured (state schema predating it) or a failed execution carries none of them, and the input side is absent on the default run path (which observes no inputs). The whole object is omitted from JSON when nothing was recorded — see [`Self::from_execution`] — so output for pre-triple records is unchanged.
 */
export interface RecipeIdentityView {
  /**
   * The **environment** key: blake3 (hex) over the engine version and the adapter / dialect identity. Excludes the hostname by construction.
   */
  env_hash?: string | null;
  /**
   * The hash-scheme tag (`"v1"`) in force when the triple was computed, so a future canonicalisation change is an explicit new scheme rather than a silent history fork.
   */
  hash_scheme?: string | null;
  /**
   * The **input** key: blake3 (hex) over the run's observed input identities. Present only when the run actually observed inputs (the `--skip-unchanged` gate's upstream freshness signatures, or the content-addressed reuse spine); absent on the default run path.
   */
  input_hash?: string | null;
  /**
   * Strength of [`Self::input_hash`]: `"strong"` (every observed upstream is a content hash — offline byte-verifiable) or `"heuristic"` (at least one is a freshness signature, attesting freshness rather than byte-identity). Carried so a weak input hash is never presented as a content claim. `None` whenever [`Self::input_hash`] is `None`.
   */
  input_proof_class?: string | null;
  /**
   * The program **identity** key: blake3 (hex) of the canonical `ModelIr` JSON. Stable across environments and engine versions for the same program text. The value `rocky history --recipe <hash>` filters on.
   */
  recipe_hash?: string | null;
  [k: string]: unknown;
}
