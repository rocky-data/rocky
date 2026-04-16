/* eslint-disable */

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
