/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/list_adapters.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky list adapters`.
 */
export interface ListAdaptersOutput {
  adapters: ListAdapterEntry[];
  command: string;
  version: string;
  [k: string]: unknown;
}
export interface ListAdapterEntry {
  adapter_type: string;
  host?: string | null;
  name: string;
  [k: string]: unknown;
}
