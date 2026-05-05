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
  /**
   * Path of the body file (`.rocky` or `.sql`) written to the models directory, when emission succeeded.
   */
  body_path?: string | null;
  command: string;
  format: string;
  intent: string;
  name: string;
  /**
   * Path of the `.toml` sidecar written next to the body, when emission succeeded. The sidecar carries the materialization strategy and target coordinates so Rocky's model loader picks the generated model up without manual editing.
   */
  sidecar_path?: string | null;
  source: string;
  version: string;
  [k: string]: unknown;
}
