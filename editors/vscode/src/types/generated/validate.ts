/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/validate.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky validate`.
 */
export interface ValidateOutput {
  adapters: ValidateAdapterStatus[];
  command: string;
  messages: ValidateMessage[];
  models: ValidateModelsStatus;
  pipelines: ValidatePipelineStatus[];
  valid: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * Status of a single adapter in the validate output.
 */
export interface ValidateAdapterStatus {
  adapter_type: string;
  name: string;
  ok: boolean;
  [k: string]: unknown;
}
/**
 * A structured diagnostic message from `rocky validate`.
 */
export interface ValidateMessage {
  /**
   * Machine-readable code (e.g. "V001", "L001").
   */
  code: string;
  /**
   * Config field path associated with this message, if applicable.
   */
  field?: string | null;
  /**
   * File path associated with this message, if applicable.
   */
  file?: string | null;
  message: string;
  /**
   * One of "ok", "warn", "error", "lint".
   */
  severity: string;
  [k: string]: unknown;
}
/**
 * Status of the models directory in the validate output.
 */
export interface ValidateModelsStatus {
  count: number;
  dag_valid: boolean;
  found: boolean;
  [k: string]: unknown;
}
/**
 * Status of a single pipeline in the validate output.
 */
export interface ValidatePipelineStatus {
  catalog_template: string;
  name: string;
  ok: boolean;
  pipeline_type: string;
  schema_template: string;
  strategy: string;
  [k: string]: unknown;
}
