/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/list_sources.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky list sources`.
 */
export interface ListSourcesOutput {
  command: string;
  sources: ListSourceEntry[];
  version: string;
  [k: string]: unknown;
}
export interface ListSourceEntry {
  adapter: string;
  catalog?: string | null;
  components: string[];
  discovery_adapter?: string | null;
  pipeline: string;
  schema_prefix?: string | null;
  [k: string]: unknown;
}
