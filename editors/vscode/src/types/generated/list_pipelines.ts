/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/list_pipelines.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky list pipelines`.
 */
export interface ListPipelinesOutput {
  command: string;
  pipelines: ListPipelineEntry[];
  version: string;
  [k: string]: unknown;
}
export interface ListPipelineEntry {
  concurrency: string;
  depends_on: string[];
  name: string;
  pipeline_type: string;
  source_adapter?: string | null;
  target_adapter: string;
  [k: string]: unknown;
}
