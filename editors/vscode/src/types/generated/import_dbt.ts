/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/import_dbt.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky import-dbt`.
 *
 * The `report` field is the per-model migration report from `rocky_compiler::import::report::generate_report`. We hold it as `serde_json::Value` (typed as `any` in JSON Schema) to avoid pulling `JsonSchema` derives all the way through `rocky-compiler::import`. The downstream Pydantic/TS bindings will see it as a free-form object.
 */
export interface ImportDbtOutput {
  command: string;
  dbt_version?: string | null;
  failed: number;
  failed_details: ImportDbtFailure[];
  import_method: string;
  imported: number;
  imported_models: string[];
  macros_detected: number;
  project_name?: string | null;
  /**
   * Free-form per-model migration report payload.
   */
  report: {
    [k: string]: unknown;
  };
  sources_found: number;
  sources_mapped: number;
  tests_converted: number;
  tests_converted_custom: number;
  tests_found: number;
  tests_skipped: number;
  version: string;
  warning_details: ImportDbtWarning[];
  warnings: number;
  [k: string]: unknown;
}
export interface ImportDbtFailure {
  name: string;
  reason: string;
  [k: string]: unknown;
}
export interface ImportDbtWarning {
  category: string;
  message: string;
  model: string;
  suggestion?: string | null;
  [k: string]: unknown;
}
