/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/model_list.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The compiled model list for `GET /api/v1/models`.
 *
 * One entry per model in the in-memory compile result, sorted by model name (the graph itself iterates in topological order). Bounded by the project's model count: the whole list is what an estate screen renders, so there is no pagination and no per-request cap beyond that.
 */
export interface ModelListOutput {
  /**
   * `models.len()`, repeated so a consumer can assert it received the whole list.
   */
  count: number;
  /**
   * Every compiled model.
   */
  models: ModelListEntry[];
  [k: string]: unknown;
}
/**
 * One row of [`ModelListOutput`].
 */
export interface ModelListEntry {
  /**
   * Number of inferred output columns.
   */
  columns: number;
  /**
   * Direct downstream model names.
   */
  downstream: string[];
  /**
   * Whether the model uses `SELECT *`, so its schema depends on upstream.
   */
  has_star: boolean;
  /**
   * Model name.
   */
  name: string;
  /**
   * Direct upstream model names.
   */
  upstream: string[];
  [k: string]: unknown;
}
