/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai_explain.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai explain`.
 */
export interface AiExplainOutput {
  command: string;
  explanations: AiExplanation[];
  version: string;
  [k: string]: unknown;
}
export interface AiExplanation {
  intent: string;
  model: string;
  saved: boolean;
  [k: string]: unknown;
}
