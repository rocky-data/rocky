/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/package_remove.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky package remove`.
 */
export interface PackageRemoveOutput {
  command: string;
  files_deleted: string[];
  lockfile: string;
  name: string;
  version: string;
  [k: string]: unknown;
}
