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
  /**
   * Files written, relative to `output_path` (for `html`, the file name).
   */
  files: string[];
  /**
   * What was written: `site` (a directory), `html` (one file) or `parquet` (a directory of tables).
   */
  format: string;
  models_count: number;
  output_path: string;
  pipelines_count: number;
  /**
   * External tables the models read.
   */
  sources_count: number;
  version: string;
  [k: string]: unknown;
}
