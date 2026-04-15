/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/validate_migration.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky validate-migration`.
 */
export interface ValidateMigrationOutput {
  command: string;
  dbt_version?: string | null;
  models_failed: number;
  models_imported: number;
  project_name?: string | null;
  total_contracts: number;
  total_tests: number;
  total_warnings: number;
  validations: ModelValidationOutput[];
  version: string;
  [k: string]: unknown;
}
export interface ModelValidationOutput {
  compile_ok: boolean;
  contracts_generated: number;
  dbt_description?: string | null;
  model: string;
  present_in_dbt: boolean;
  present_in_rocky: boolean;
  rocky_intent?: string | null;
  test_count: number;
  warnings: string[];
  [k: string]: unknown;
}
