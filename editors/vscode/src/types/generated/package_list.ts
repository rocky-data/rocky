/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/package_list.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky package list`.
 */
export interface PackageListOutput {
  command: string;
  count: number;
  lockfile: string;
  packages: PackageListEntry[];
  version: string;
  [k: string]: unknown;
}
/**
 * One vendored package, as `rocky package list` reports it.
 */
export interface PackageListEntry {
  adapter: string;
  compiled_at: string;
  dbt_version: string;
  /**
   * Pending `<path>.incoming` files from an update.
   */
  files_incoming: string[];
  /**
   * Vendored files that are gone.
   */
  files_missing: string[];
  /**
   * Vendored files whose content differs from what Rocky wrote.
   */
  files_modified: string[];
  hub: string;
  includes: string[];
  /**
   * `compile-only`, `build-empty` or `compiled`; replayed by `update`.
   */
  mode: string;
  models: string[];
  name: string;
  sources: PackageSourceOutput[];
  target_schema: string;
  version: string;
  version_spec: string;
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
