/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/branch_list.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky branch list`.
 */
export interface BranchListOutput {
  branches: BranchEntry[];
  command: string;
  total: number;
  version: string;
  [k: string]: unknown;
}
/**
 * A single branch record in JSON output.
 */
export interface BranchEntry {
  created_at: string;
  created_by: string;
  description?: string | null;
  name: string;
  schema_prefix: string;
  [k: string]: unknown;
}
