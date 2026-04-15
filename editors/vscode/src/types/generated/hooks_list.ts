/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/hooks_list.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky hooks list`.
 */
export interface HooksListOutput {
  hooks: HookEntry[];
  total: number;
  [k: string]: unknown;
}
export interface HookEntry {
  command: string;
  env_keys: string[];
  event: string;
  on_failure: string;
  timeout_ms: number;
  [k: string]: unknown;
}
