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
  /**
   * Metadata about the emitted Rocky repo. Present when `--output-dir` triggered repo emission (the default for `rocky import-dbt`). Absent when emission was skipped or failed before disk writes.
   */
  emission?: ImportDbtEmission | null;
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
/**
 * Files-on-disk metadata for the runnable Rocky repo emitted by `rocky import-dbt --output-dir <out>`.
 *
 * Consumers (Dagster, vscode) treat this block as the contract for "where the importer wrote things." Absence of the block on `ImportDbtOutput` means no repo was emitted (e.g. dry-run mode in a follow-up).
 */
export interface ImportDbtEmission {
  /**
   * Rocky adapter type written into `[adapter]` (`duckdb` / `databricks` / …).
   */
  adapter_type: string;
  /**
   * Path to the generated `MIGRATION-NOTES.md`.
   */
  migration_notes_path: string;
  /**
   * Number of dbt model files seen but not translated (failed entries).
   */
  models_skipped_count: number;
  /**
   * Number of dbt models successfully translated and written under `models/`.
   */
  models_translated_count: number;
  /**
   * dbt profile `type` value the importer mapped from. Useful when the caller passed `--target-adapter` and we want to surface the original.
   */
  original_dbt_adapter_type: string;
  /**
   * Resolved output directory.
   */
  out_dir: string;
  /**
   * Env vars referenced by the emitted `[adapter]` block. Surfaced in `MIGRATION-NOTES.md` under "Required env vars".
   */
  required_env_vars: string[];
  /**
   * Path to the generated `rocky.toml`.
   */
  rocky_toml_path: string;
  /**
   * Number of files copied from `<dbt_project>/seeds/` into `<out>/seeds/`.
   */
  seeds_copied_count: number;
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
