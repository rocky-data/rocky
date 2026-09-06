/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/docs.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Output for `rocky docs --output json`.
 */
export interface DocsOutput {
  command: string;
  duration_ms: number;
  models_count: number;
  output_path: string;
  pipelines_count: number;
  version: string;
  [k: string]: unknown;
}
