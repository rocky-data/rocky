/* eslint-disable */

/**
 * JSON output for `rocky history --model <name>`.
 */
export interface ModelHistoryOutput {
  command: string;
  count: number;
  executions: ModelExecutionRecord[];
  model: string;
  version: string;
  [k: string]: unknown;
}
/**
 * One model execution from the state store, mirroring `rocky_core::state::ModelExecution`.
 */
export interface ModelExecutionRecord {
  duration_ms: number;
  rows_affected?: number | null;
  sql_hash: string;
  started_at: string;
  status: string;
  [k: string]: unknown;
}
