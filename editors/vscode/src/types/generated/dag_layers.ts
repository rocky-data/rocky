/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/dag_layers.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The execution layers for `GET /api/v1/dag/layers`.
 *
 * Bounded by the project's model count: every model appears in exactly one layer.
 */
export interface DagLayersOutput {
  /**
   * Topologically sorted layers. Models in one layer have no mutual dependencies and can run in parallel.
   */
  layers: string[][];
  /**
   * Number of models across all layers.
   */
  total_models: number;
  [k: string]: unknown;
}
