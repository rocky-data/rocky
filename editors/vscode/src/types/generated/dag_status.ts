/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/dag_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Status of one node in a recorded DAG execution.
 */
export type DagNodeStatusOutput = "pending" | "running" | "completed" | "failed" | "skipped";

/**
 * The latest recorded DAG execution for `GET /api/v1/dag/status`.
 *
 * Populated by the in-process executor only; there is no CLI counterpart. The route answers `503 engine_not_ready` until one execution has been recorded. Bounded by the node count of that execution.
 */
export interface DagStatusOutput {
  /**
   * When the execution finished (UTC).
   */
  completed_at: string;
  /**
   * The aggregate execution result.
   */
  result: DagExecutionOutput;
  [k: string]: unknown;
}
/**
 * Aggregate result of one DAG execution.
 */
export interface DagExecutionOutput {
  /**
   * Nodes that completed.
   */
  completed: number;
  /**
   * Wall-clock duration of the whole execution, in milliseconds.
   */
  duration_ms: number;
  /**
   * Nodes that failed.
   */
  failed: number;
  /**
   * One record per node the executor visited.
   */
  nodes: DagNodeResultOutput[];
  /**
   * Nodes skipped because an ancestor failed.
   */
  skipped: number;
  /**
   * Number of execution layers.
   */
  total_layers: number;
  /**
   * Number of nodes in the DAG.
   */
  total_nodes: number;
  [k: string]: unknown;
}
/**
 * One node's record inside [`DagExecutionOutput`].
 */
export interface DagNodeResultOutput {
  /**
   * Node duration, in milliseconds.
   */
  duration_ms: number;
  /**
   * Error message when the node failed; omitted otherwise.
   */
  error?: string | null;
  /**
   * Node id.
   */
  id: string;
  /**
   * Node kind, e.g. `model` or `test`.
   */
  kind: string;
  /**
   * Human label.
   */
  label: string;
  /**
   * The execution layer the node ran in.
   */
  layer: number;
  /**
   * Final status of the node.
   */
  status: DagNodeStatusOutput;
  [k: string]: unknown;
}
