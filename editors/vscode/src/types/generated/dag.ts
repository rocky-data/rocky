/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/dag.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Severity of a test failure.
 */
export type TestSeverity = "error" | "warning";
/**
 * Materialization strategy for a model, defaulting to full refresh.
 */
export type StrategyConfig =
  | {
      type: "full_refresh";
      [k: string]: unknown;
    }
  | {
      /**
       * The input column `@incremental_filter` compares, when it is not the watermark itself: a qualified column in a join (`"o.updated_at"`) or a source column the model renames (`"_synced_at"`). The bound is still `MAX(<timestamp_column>)` over the target.
       */
      filter_column?: string | null;
      /**
       * Re-read this far below the watermark, e.g. `"3 days"`, to catch late-arriving rows. Pair it with `unique_key`, or the re-read rows are appended again (W046).
       */
      lookback?: IncrementalLookback | null;
      /**
       * What a run does when the model's output columns no longer match the target: `fail` (default) or `append_new_columns`.
       */
      on_schema_change?: OnSchemaChange & string;
      /**
       * The watermark column: an output column of the model whose maximum in the target marks what is already loaded. `watermark` is accepted as an alias.
       */
      timestamp_column?: string | null;
      type: "incremental";
      /**
       * Upsert on these columns with `MERGE` instead of appending.
       */
      unique_key?: string[];
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
    }
  | {
      type: "view";
      [k: string]: unknown;
    }
  | {
      type: "materialized_view";
      [k: string]: unknown;
    }
  | {
      /**
       * Snowflake lag specifier — alphanumeric + space only. Examples: `"1 minute"`, `"5 hours"`, `"downstream"`.
       */
      target_lag: string;
      type: "dynamic_table";
      [k: string]: unknown;
    }
  | {
      /**
       * Logical partition column names. Empty for unpartitioned tables. The runtime asserts this matches the table's declared partition columns at materialization time.
       */
      partition_columns?: string[];
      /**
       * Object-store key prefix that holds `_delta_log/` + Parquet files for the target table. Typically `s3://<bucket>/<path>/<table>` for AWS-backed deployments.
       */
      storage_prefix: string;
      type: "content_addressed";
      [k: string]: unknown;
    }
  | {
      /**
       * Check-strategy columns: a list, or `"all"`.
       */
      check_cols?: SnapshotCheckColsConfig | null;
      /**
       * `"ignore"` (default), `"invalidate"` or `"new_record"`.
       */
      hard_deletes?: SnapshotHardDeletes | null;
      /**
       * dbt's legacy spelling of `hard_deletes = "invalidate"`.
       */
      invalidate_hard_deletes?: boolean | null;
      /**
       * Metadata column names (Rocky defaults; dbt keys accepted).
       */
      snapshot_meta_column_names?: SnapshotMetaColumns | null;
      /**
       * `"timestamp"` or `"check"`.
       */
      strategy?: SnapshotStrategyKind | null;
      type: "snapshot";
      /**
       * Column, or list of columns, identifying a row of the output.
       */
      unique_key?: SnapshotUniqueKey | null;
      /**
       * Timestamp-strategy change column.
       */
      updated_at?: string | null;
      /**
       * SQL expression for `valid_to` on current versions instead of NULL.
       */
      valid_to_current?: string | null;
      [k: string]: unknown;
    };
export type IncrementalLookback = string;
/**
 * What an incremental run does when the model's output columns differ from the existing target's columns.
 */
export type OnSchemaChange = "fail" | "append_new_columns";
/**
 * Partition granularity for `time_interval` materialization.
 *
 * The granularity determines: - The canonical partition key format (see [`TimeGrain::format_str`]). - How `@start_date` / `@end_date` placeholders are computed per partition. - What column types are valid (`hour` requires TIMESTAMP; others accept DATE).
 */
export type TimeGrain = "hour" | "day" | "month" | "year";
/**
 * `check_cols` accepts a list of columns or the string `"all"`.
 */
export type SnapshotCheckColsConfig = string | string[];
/**
 * What a snapshot does with a key that disappears from the model's result.
 */
export type SnapshotHardDeletes = "ignore" | "invalidate" | "new_record";
/**
 * The `is_current` metadata column: a name, or `false` for none.
 *
 * A snapshot table built by dbt has no `is_current` column. Setting `is_current = false` lets Rocky continue such a table: a version is then current when its `valid_to` is NULL (or equals `valid_to_current`).
 */
export type SnapshotFlagColumn = string | boolean;
/**
 * The `strategy` key inside a snapshot `[strategy]` block.
 */
export type SnapshotStrategyKind = "timestamp" | "check";
/**
 * `unique_key` accepts one column name or a list (dbt parity).
 */
export type SnapshotUniqueKey = string | string[];

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
  /**
   * Why `column_lineage` is **not** an answer, when it is not one.
   *
   * An empty `column_lineage` used to mean two different things with no way to tell them apart: a project that genuinely has no column-level lineage, and a project whose lineage could not be computed. A consumer reading zero edges off a project Rocky failed to parse would conclude there is nothing to trace (#1320).
   *
   * `None` means the list is authoritative **for a run that asked for lineage** — including when it is empty, which is the complete answer for a project with no transformation models. `Some` carries a human-readable reason and means the list must not be read as "no lineage".
   *
   * Without `--column-lineage` this stays `None` and the list stays empty, because nothing was computed and nothing failed. A consumer that did not request lineage cannot conclude anything from either field — it knows whether it passed the flag.
   */
  column_lineage_unavailable?: string | null;
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
   * Version of the graph-export contract this payload conforms to.
   *
   * Distinct from `version` (the engine release, which churns every release): `schema_version` identifies the *shape* of the graph export — the node/edge/lineage fields orchestrators build an asset graph from. It is bumped only on a backward-incompatible change to that shape, so an orchestrator can pin against it across engine releases. Additive, backward-compatible field additions do not bump it (and surface through codegen-drift CI instead). Always emitted; older payloads that predate the field are treated as `"1"`.
   */
  schema_version?: string;
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
   * Whether the serving process's current compile covers this node, i.e. whether `GET /api/v1/models/{label}` can serve it (#2011).
   *
   * Set only by `GET /api/v1/dag`, and only on `transformation` nodes. The DAG reads every transformation pipeline's own models directory; `rocky serve` compiles one. A node the compile did not cover is `false`, so a client can draw it without offering a detail link that answers 404. Also `false` while the server holds no compile result (the compile failed or has not finished): the detail route cannot serve the model then either.
   *
   * The two reads are not one snapshot: the graph is read from disk when the route is asked, the compile is the last one `serve` published. A model added a moment ago can be `false` until the next compile.
   *
   * Absent from `rocky dag`, which has no separate compile to compare against, and from every non-transformation node.
   */
  compiled?: boolean | null;
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
 * Declares the maximum allowed lag between successive materializations of the model plus the optional timestamp column used by the runtime freshness check.
 *
 * `rocky freshness` enforces the TTL at run time: it reads `MAX(time_column)` from the model's target table (or, without a `time_column`, the model's last successful build in the state store) and reports `warn`, or `error` when `severity = "error"`. `rocky run` does not gate on it. The compiler checks the `time_column` (E050 when absent from a provably complete output, W050 when not temporal), and soft-warns (W005) when a model has at least one temporal output column but no `freshness` declaration anywhere in scope (per-model or project-level default).
 */
export interface ModelFreshnessConfig {
  /**
   * Maximum lag in seconds before the model is considered stale.
   *
   * Accepts both `max_lag_seconds` (legacy field name, preserved for existing sidecar fixtures + dagster Pydantic + VS Code bindings) and `expected_lag_seconds` (the documented public-facing name matching dbt freshness + SQLMesh defaults). Both deserialize to the same field; the serialized name stays `max_lag_seconds` so existing JSON/codegen consumers keep working unchanged.
   */
  max_lag_seconds: number;
  /**
   * Severity reported when the freshness check trips. Default `warning` keeps the runtime check non-blocking — switch to `error` to fail the pipeline on stale data.
   */
  severity?: TestSeverity | null;
  /**
   * Optional timestamp column used to evaluate freshness at runtime (`MAX(time_column) < NOW() - INTERVAL max_lag_seconds`). When unset the runtime falls back to the model's last-materialization timestamp from the state store.
   */
  time_column?: string | null;
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
 * Names of the metadata columns a snapshot model adds to its rows.
 *
 * The defaults match the `snapshot` pipeline. Each key also accepts the dbt spelling (`dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`, `dbt_is_deleted`) so an imported `snapshot_meta_column_names` block reads unchanged.
 */
export interface SnapshotMetaColumns {
  /**
   * `TRUE` on the current version of each key. Default `is_current`; `false` writes no flag (dbt has no such column).
   */
  is_current?: SnapshotFlagColumn & string;
  /**
   * Deletion marker, written only under `hard_deletes = "new_record"`. Default `is_deleted`.
   */
  is_deleted?: string;
  /**
   * Deterministic per-version id: a hash of the key and `valid_from`. Default `snapshot_id`.
   */
  scd_id?: string;
  /**
   * Optional copy of the version's change timestamp (dbt's `dbt_updated_at`). Not written unless named.
   */
  updated_at?: string | null;
  /**
   * When this version became current. Default `valid_from`.
   */
  valid_from?: string;
  /**
   * When this version stopped being current. Default `valid_to`.
   */
  valid_to?: string;
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
