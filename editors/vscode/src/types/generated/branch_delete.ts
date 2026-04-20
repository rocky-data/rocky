/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/branch_delete.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky branch delete`.
 */
export interface BranchDeleteOutput {
  command: string;
  name: string;
  /**
   * Whether a record was actually removed (false if the branch didn't exist).
   */
  removed: boolean;
  version: string;
  [k: string]: unknown;
}
