/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/dag_run.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Output of `rocky run --dag`: per-node execution results plus aggregate counts.
 */
export interface DagRunOutput {
  command: string;
  /**
   * Nodes that completed successfully.
   */
  completed: number;
  /**
   * Wall-clock duration of the entire DAG execution.
   */
  duration_ms: number;
  /**
   * Nodes whose dispatcher returned an error.
   */
  failed: number;
  /**
   * Per-node execution records, sorted by (layer, id).
   */
  nodes: DagRunNodeOutput[];
  /**
   * Nodes skipped because an upstream failed (or because the dispatcher declined to handle them, e.g. Test nodes).
   */
  skipped: number;
  /**
   * Number of execution layers (Kahn topological depth).
   */
  total_layers: number;
  /**
   * Total nodes across all layers.
   */
  total_nodes: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Per-node record in a [`DagRunOutput`].
 */
export interface DagRunNodeOutput {
  /**
   * Wall-clock duration of this node's execution.
   */
  duration_ms: number;
  /**
   * Failure reason. Present iff `status = "failed"` (or `"skipped"` due to upstream failure).
   */
  error?: string | null;
  /**
   * Node identifier (e.g. `transformation:stg_orders`).
   */
  id: string;
  /**
   * Node kind: `source`, `replication`, `transformation`, `quality`, `snapshot`, `load`, `seed`, `test`.
   */
  kind: string;
  /**
   * Human-readable label (usually the pipeline or model name).
   */
  label: string;
  /**
   * Layer index (0-based; nodes in the same layer ran concurrently).
   */
  layer: number;
  /**
   * Status: `pending`, `running`, `completed`, `failed`, `skipped`.
   */
  status: string;
  [k: string]: unknown;
}
