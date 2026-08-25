/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/recipe_history.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky history --recipe <hash>` — every recorded execution of one exact program (`recipe_hash`), across all runs.
 *
 * The "what produced this?" query: given a `recipe_hash` (read from any model record's [`RecipeIdentityView`]), list every time that exact program ran — newest run first — with the run it belonged to and its per-execution input / environment identity.
 */
export interface RecipeHistoryOutput {
  command: string;
  count: number;
  executions: RecipeExecutionRecord[];
  /**
   * The `recipe_hash` the history was filtered on, echoed back.
   */
  recipe_hash: string;
  version: string;
  [k: string]: unknown;
}
/**
 * One execution of a given `recipe_hash`, embedded in [`RecipeHistoryOutput`].
 */
export interface RecipeExecutionRecord {
  duration_ms: number;
  /**
   * Model name as recorded in the run.
   */
  model_name: string;
  /**
   * The recipe-identity triple for this execution. Its `recipe_hash` matches the top-level filter; `input_hash` / `env_hash` can differ run-to-run for the same program (different inputs, different engine version).
   */
  recipe_identity?: RecipeIdentityView | null;
  rows_affected?: number | null;
  /**
   * The run this execution belonged to.
   */
  run_id: string;
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
