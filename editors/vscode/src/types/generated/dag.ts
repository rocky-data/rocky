/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/dag.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Materialization strategy for a model, defaulting to full refresh.
 */
export type StrategyConfig =
  | {
      type: "full_refresh";
      [k: string]: unknown;
    }
  | {
      timestamp_column: string;
      type: "incremental";
      [k: string]: unknown;
    }
  | {
      type: "merge";
      unique_key: string[];
      update_columns?: string[] | null;
      [k: string]: unknown;
    }
  | {
      /**
       * Combine N consecutive partitions into one SQL statement when backfilling. Defaults to 1 (atomic per-partition replacement).
       */
      batch_size?: number;
      /**
       * Lower bound for `--missing` discovery, in canonical key format (e.g., `"2024-01-01"` for daily). Required when `--missing` is used.
       */
      first_partition?: string | null;
      /**
       * Partition granularity (`hour`, `day`, `month`, `year`).
       */
      granularity: TimeGrain;
      /**
       * Recompute the previous N partitions on each run, in addition to whatever the CLI selected. Standard handling for late-arriving data.
       */
      lookback?: number;
      /**
       * Column on the model output that holds the partition value. Must be a non-nullable date or timestamp column. Validated by the compiler against the typed output schema.
       */
      time_column: string;
      type: "time_interval";
      [k: string]: unknown;
    }
  | {
      type: "ephemeral";
      [k: string]: unknown;
    }
  | {
      /**
       * Column(s) used to identify the partition to delete.
       */
      partition_by: string[];
      type: "delete_insert";
      [k: string]: unknown;
    }
  | {
      /**
       * Batch granularity (default: Hour).
       */
      granularity?: TimeGrain & string;
      /**
       * Timestamp column for micro-batch boundaries.
       */
      timestamp_column: string;
      type: "microbatch";
      [k: string]: unknown;
    };
/**
 * Partition granularity for `time_interval` materialization.
 *
 * The granularity determines: - The canonical partition key format (see [`TimeGrain::format_str`]). - How `@start_date` / `@end_date` placeholders are computed per partition. - What column types are valid (`hour` requires TIMESTAMP; others accept DATE).
 */
export type TimeGrain = "hour" | "day" | "month" | "year";

/**
 * JSON output for `rocky dag`.
 *
 * Projects the engine's internal [`UnifiedDag`] into an enriched, orchestrator-friendly shape: every pipeline stage becomes a node with its target table coordinates, materialization strategy, freshness SLA, partition shape, and direct upstream dependencies.
 *
 * Consumers (dagster-rocky) can build a complete, connected asset graph from a single `rocky dag --output json` call.
 *
 * [`UnifiedDag`]: rocky_core::unified_dag::UnifiedDag
 */
export interface DagOutput {
  /**
   * Column-level lineage edges across all models. Only populated when `--column-lineage` is passed; empty otherwise.
   */
  column_lineage?: LineageEdgeRecord[];
  command: string;
  /**
   * Directed edges between nodes (from → to).
   */
  edges: DagEdgeOutput[];
  /**
   * Topologically-sorted execution layers. Nodes within the same layer have no mutual dependencies and can execute in parallel.
   */
  execution_layers: string[][];
  /**
   * Every stage in the pipeline as an enriched DAG node.
   */
  nodes: DagNodeOutput[];
  /**
   * Summary counts for the DAG.
   */
  summary: DagSummaryOutput;
  version: string;
  [k: string]: unknown;
}
export interface LineageEdgeRecord {
  source: LineageQualifiedColumn;
  target: LineageQualifiedColumn;
  /**
   * Transform kind: "direct", "cast", "expression", etc. Stringified from `rocky_sql::lineage::TransformKind` to avoid pulling schemars into rocky-sql.
   */
  transform: string;
  [k: string]: unknown;
}
export interface LineageQualifiedColumn {
  column: string;
  model: string;
  [k: string]: unknown;
}
/**
 * One directed edge in the DAG output.
 */
export interface DagEdgeOutput {
  /**
   * Semantic classification: `"data"`, `"check"`, `"governance"`.
   */
  edge_type: string;
  /**
   * Upstream node ID.
   */
  from: string;
  /**
   * Downstream node ID.
   */
  to: string;
  [k: string]: unknown;
}
/**
 * One node in the enriched DAG, projected for orchestrators.
 *
 * Cross-references the engine's internal `UnifiedNode` with model configs, seeds, and pipeline configs to attach the metadata that orchestrators need (target, strategy, freshness, partition shape).
 */
export interface DagNodeOutput {
  /**
   * Upstream node IDs (derived from DAG edges).
   */
  depends_on?: string[];
  /**
   * Per-model freshness expectation from the model sidecar.
   */
  freshness?: ModelFreshnessConfig | null;
  /**
   * Unique identifier: `{kind}:{name}` (e.g. `transformation:stg_orders`).
   */
  id: string;
  /**
   * Node kind: `source`, `load`, `transformation`, `quality`, `snapshot`, `seed`, `test`, `replication`.
   */
  kind: string;
  /**
   * Human-readable label (usually the pipeline or model name).
   */
  label: string;
  /**
   * Partition shape for time-interval models. `None` for unpartitioned strategies.
   */
  partition_shape?: PartitionShapeOutput | null;
  /**
   * Pipeline name from `rocky.toml`, if applicable.
   */
  pipeline?: string | null;
  /**
   * Materialization strategy. Present for transformation nodes.
   */
  strategy?: StrategyConfig | null;
  /**
   * Target table coordinates. Present for transformation, seed, load, and snapshot nodes.
   */
  target?: TargetConfig | null;
  [k: string]: unknown;
}
/**
 * Per-model freshness configuration.
 *
 * Declares the maximum allowed lag between successive materializations of the model. The compiler does not validate this — it's a metadata field consumed by downstream observability tools.
 */
export interface ModelFreshnessConfig {
  /**
   * Maximum lag in seconds before the model is considered stale.
   */
  max_lag_seconds: number;
  [k: string]: unknown;
}
/**
 * Partition shape metadata for time-interval nodes.
 */
export interface PartitionShapeOutput {
  /**
   * First partition key, if declared in the model sidecar.
   */
  first_partition?: string | null;
  /**
   * Time granularity: `"daily"`, `"hourly"`, `"monthly"`, `"yearly"`.
   */
  granularity: string;
  [k: string]: unknown;
}
/**
 * Target table coordinates for a model.
 */
export interface TargetConfig {
  catalog: string;
  schema: string;
  table: string;
  [k: string]: unknown;
}
/**
 * Summary counts for the DAG.
 */
export interface DagSummaryOutput {
  counts_by_kind: {
    [k: string]: number;
  };
  execution_layers: number;
  total_edges: number;
  total_nodes: number;
  [k: string]: unknown;
}
