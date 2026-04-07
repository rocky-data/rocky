/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai "<intent>"`.
 */
export interface AiGenerateOutput {
  attempts: number;
  command: string;
  format: string;
  intent: string;
  name: string;
  source: string;
  version: string;
  [k: string]: unknown;
}
