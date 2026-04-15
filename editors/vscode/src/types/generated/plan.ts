/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/plan.schema.json
 * Run just codegen from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky plan`.
 */
export interface PlanOutput {
  command: string;
  filter: string;
  statements: PlannedStatement[];
  version: string;
  [k: string]: unknown;
}
export interface PlannedStatement {
  purpose: string;
  sql: string;
  target: string;
  [k: string]: unknown;
}
