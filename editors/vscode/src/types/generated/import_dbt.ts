/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/import_dbt.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Typed structured warning for dbt config that Rocky can't translate automatically. Each variant carries the source payload so the downstream UI (Dagster, VS Code) can route specific kinds into per-kind UI affordances without parsing free-form text.
 */
export type ImportDbtStructuredWarning =
  | {
      action: string;
      dbt_materialization: string;
      kind: "unsupported_materialization";
      model: string;
      [k: string]: unknown;
    }
  | {
      kind: "dropped_databricks_tags";
      model: string;
      tags: {
        [k: string]: string;
      };
      [k: string]: unknown;
    }
  | {
      hook_kind: ImportDbtHookKind;
      kind: "dropped_hook";
      model: string;
      sql: string;
      [k: string]: unknown;
    }
  | {
      dbt_value: string;
      kind: "dropped_on_schema_change";
      model: string;
      rocky_equivalent: string;
      [k: string]: unknown;
    }
  | {
      first_call_site_line: number;
      kind: "unresolvable_macro";
      macro_name: string;
      model: string;
      [k: string]: unknown;
    }
  | {
      kind: "microbatch_missing_event_time";
      model: string;
      [k: string]: unknown;
    }
  | {
      kind: "microbatch_mapped";
      mapped_to: string;
      model: string;
      [k: string]: unknown;
    }
  | {
      construct: string;
      detail: string;
      kind: "dropped_construct";
      name: string;
      [k: string]: unknown;
    }
  | {
      constraints: number;
      contract_path: string;
      kind: "dropped_contract";
      model: string;
      typed_columns: number;
      [k: string]: unknown;
    };
/**
 * Lifecycle hook kind for [`ImportDbtStructuredWarning::DroppedHook`].
 */
export type ImportDbtHookKind = "pre" | "post";

/**
 * JSON output for `rocky import-dbt`.
 *
 * The `report` field is the per-model migration report from `rocky_compiler::import::report::generate_report`. We hold it as `serde_json::Value` (typed as `any` in JSON Schema) to avoid pulling `JsonSchema` derives all the way through `rocky-compiler::import`. The downstream Pydantic/TS bindings will see it as a free-form object.
 */
export interface ImportDbtOutput {
  command: string;
  /**
   * Number of dbt resources the importer does not translate that were detected and skipped (snapshots, metrics, semantic models, exposures).
   */
  constructs_dropped?: number;
  /**
   * Number of dbt models whose enforced `contract` (column `data_type`s / `constraints`) was dropped on import. Rocky enforces contracts via a `{model}.contract.toml` sidecar the importer does not auto-generate.
   */
  contracts_dropped?: number;
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
  /**
   * Typed structured warnings — payload-carrying variants for dbt config Rocky can't auto-translate (dropped tags, dropped hooks, unresolvable macros, etc.). Coexists with `warning_details` — orchestrators that don't know about this field still see the flat string warnings under `warning_details`.
   */
  structured_warnings?: ImportDbtStructuredWarning[];
  tests_converted: number;
  tests_converted_custom: number;
  tests_found: number;
  tests_skipped: number;
  /**
   * Number of dbt unit tests written as Rocky `[[test]]` blocks in per-model sidecar TOML files.
   */
  unit_tests_converted?: number;
  /**
   * Total dbt unit-test definitions (`manifest.unit_tests`) seen across all imported manifests.
   */
  unit_tests_found?: number;
  /**
   * Number of dbt unit tests dropped — orphan target model, non-`dict` fixture format, or otherwise unsupported shape.
   */
  unit_tests_skipped?: number;
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
