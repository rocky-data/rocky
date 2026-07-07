/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/model_history.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky history --model <name>`.
 */
export interface ModelHistoryOutput {
  command: string;
  count: number;
  executions: ModelExecutionRecord[];
  model: string;
  /**
   * Rolling statistics over the most recent N successful executions. Present only when `--rolling-stats` is passed.
   */
  rolling_stats?: RollingStats | null;
  version: string;
  [k: string]: unknown;
}
/**
 * One model execution from the state store, mirroring `rocky_core::state::ModelExecution`.
 */
export interface ModelExecutionRecord {
  duration_ms: number;
  /**
   * The recipe-identity triple recorded for this execution, when present. See [`RecipeIdentityView`].
   */
  recipe_identity?: RecipeIdentityView | null;
  rows_affected?: number | null;
  sql_hash: string;
  started_at: string;
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
/**
 * Rolling statistics computed over the most recent N successful executions of a model. Populated by `rocky history --model <name> --rolling-stats`.
 *
 * Statistics use population standard deviation (divided by N, not N-1), so `std_dev` is exactly 0 when all samples are equal.
 */
export interface RollingStats {
  /**
   * Rolling statistics for the `duration_ms` dimension.
   */
  duration_ms: RollingDimension;
  /**
   * Composite health score in `[0.0, 1.0]`.
   *
   * Computed as `1.0 - clamp((max(|z_rows|, |z_duration|) - 2.0) / 4.0, 0.0, 1.0)`. A score of `1.0` means both z-scores are within 2σ of the mean; `0.0` means at least one z-score is 6σ or more.
   */
  health_score: number;
  /**
   * Rolling statistics for the `rows_affected` dimension. Computed only over executions where `rows_affected` is not null.
   */
  rows_affected: RollingDimension;
  /**
   * Actual number of successful executions used (≤ window; may be smaller when model history is shorter than the requested window).
   */
  samples: number;
  /**
   * Maximum number of executions requested for the rolling window.
   */
  window: number;
  [k: string]: unknown;
}
/**
 * Per-dimension rolling statistics (mean, std dev, latest z-score).
 */
export interface RollingDimension {
  /**
   * Z-score of the most recent execution relative to the window.
   *
   * `None` when fewer than 2 samples are available or when `std_dev` is exactly 0 (all samples are equal — no meaningful deviation).
   */
  latest_z_score?: number | null;
  /**
   * Population mean over the sample window.
   */
  mean: number;
  /**
   * Population standard deviation (÷N) over the sample window.
   */
  std_dev: number;
  [k: string]: unknown;
}
