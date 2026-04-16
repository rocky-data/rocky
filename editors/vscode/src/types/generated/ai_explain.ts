/* eslint-disable */

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
