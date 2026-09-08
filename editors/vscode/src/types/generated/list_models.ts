/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/list_models.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky list models`.
 */
export interface ListModelsOutput {
  command: string;
  models: ListModelEntry[];
  version: string;
  [k: string]: unknown;
}
export interface ListModelEntry {
  depends_on: string[];
  has_contract: boolean;
  name: string;
  strategy: string;
  target: string;
  [k: string]: unknown;
}
