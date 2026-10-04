/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/package_update.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky package update`.
 */
export interface PackageUpdateOutput {
  command: string;
  diagnostics: PackageDiagnostic[];
  lockfile: string;
  packages: PackageVendorReport[];
  version: string;
  [k: string]: unknown;
}
/**
 * A finding from `rocky package`: `E055` (refused) or `W055` (vendored, but something needs a look).
 */
export interface PackageDiagnostic {
  code: string;
  message: string;
  /**
   * The model the finding is about, when there is one.
   */
  model?: string | null;
  /**
   * The vendored file the finding is about, when there is one.
   */
  path?: string | null;
  /**
   * `error` or `warning`.
   */
  severity: string;
  [k: string]: unknown;
}
/**
 * What `add` / `update` did for one package.
 */
export interface PackageVendorReport {
  /**
   * Rocky adapter type the package was compiled against.
   */
  adapter: string;
  /**
   * Whether dbt built empty upstream relations before compiling.
   */
  build_empty: boolean;
  dbt_version: string;
  failed_models: PackageFailedModel[];
  /**
   * Vendored files deleted because upstream removed them.
   */
  files_deleted: string[];
  /**
   * Files you edited that changed upstream: the new version is at `<path>.incoming`.
   */
  files_incoming: string[];
  /**
   * Files you edited that were left as they are.
   */
  files_kept_edited: string[];
  files_unchanged: number;
  /**
   * Files written in place.
   */
  files_written: string[];
  /**
   * dbt Hub name (`fivetran/stripe`).
   */
  hub: string;
  /**
   * Dependency packages whose models were vendored alongside.
   */
  includes: string[];
  /**
   * dbt `incremental` models that did not stay incremental.
   */
  incremental_fallbacks: string[];
  /**
   * Every vendored model, sorted.
   */
  models: string[];
  /**
   * Models new in this run (all of them on `add`).
   */
  models_added: string[];
  /**
   * Models the previous version had and this one does not.
   */
  models_removed: string[];
  /**
   * dbt project name of the package; its directory under `models/packages/`.
   */
  name: string;
  sources: PackageSourceOutput[];
  /**
   * Schema the vendored models build into.
   */
  target_schema: string;
  tests_dropped: PackageDroppedTest[];
  /**
   * dbt generic tests mapped to Rocky `[[tests]]`.
   */
  tests_mapped: number;
  vars_hash: string;
  /**
   * Version `dbt deps` resolved.
   */
  version: string;
  /**
   * Version requirement the package was added with; empty means latest.
   */
  version_spec: string;
  [k: string]: unknown;
}
/**
 * A package model that could not be vendored.
 */
export interface PackageFailedModel {
  name: string;
  reason: string;
  [k: string]: unknown;
}
/**
 * A raw source table a vendored package reads.
 */
export interface PackageSourceOutput {
  catalog: string;
  /**
   * `<source_name>.<table>` as the package declares it.
   */
  name: string;
  schema: string;
  table: string;
  [k: string]: unknown;
}
/**
 * A dbt test on a package model that was not mapped to Rocky.
 */
export interface PackageDroppedTest {
  attached_to?: string | null;
  reason: string;
  test: string;
  [k: string]: unknown;
}
