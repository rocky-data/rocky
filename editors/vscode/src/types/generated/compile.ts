/* eslint-disable */

/**
 * Severity level of a diagnostic.
 *
 * Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay compatible with existing dagster fixtures and the hand-written `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
 */
export type Severity = "Error" | "Warning" | "Info";
/**
 * Confidence level for an incrementality recommendation.
 */
export type Confidence = "high" | "medium" | "low";
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
 * JSON output for `rocky compile`.
 */
export interface CompileOutput {
  command: string;
  compile_timings: PhaseTimings;
  diagnostics: Diagnostic[];
  execution_layers: number;
  /**
   * Expanded SQL for each model after macro substitution. Only populated when `--expand-macros` is passed. Keys are model names, values are the SQL after all `@macro()` calls have been replaced.
   */
  expanded_sql?: {
    [k: string]: string;
  };
  has_errors: boolean;
  models: number;
  /**
   * Per-model details extracted from each model's TOML frontmatter. Empty when the project has no models. Downstream consumers (`dagster-rocky` to surface per-model freshness policies and partition-keyed assets) iterate this list rather than re-loading model frontmatter themselves.
   */
  models_detail?: ModelDetail[];
  version: string;
  [k: string]: unknown;
}
/**
 * Wall-clock duration of each compile phase.
 *
 * Surfaced in `CompileResult` so callers (CLI, Dagster, LSP) can attribute compile time to a specific stage instead of treating compile as a black box. `typecheck_join_keys_ms` is a sub-timer of `typecheck_ms` so we can decide whether the cross-model join-key check needs further optimization without re-instrumenting later.
 */
export interface PhaseTimings {
  contracts_ms: number;
  project_load_ms: number;
  semantic_graph_ms: number;
  total_ms: number;
  /**
   * Portion of `typecheck_ms` spent inside `check_join_keys`.
   */
  typecheck_join_keys_ms: number;
  typecheck_ms: number;
  [k: string]: unknown;
}
/**
 * A compiler diagnostic (error, warning, or informational message).
 */
export interface Diagnostic {
  /**
   * Diagnostic code (e.g., "E001", "W001").
   */
  code: string;
  /**
   * Human-readable message.
   */
  message: string;
  /**
   * Which model this diagnostic relates to.
   */
  model: string;
  /**
   * Severity level.
   */
  severity: Severity;
  /**
   * Source location (if available).
   */
  span?: SourceSpan | null;
  /**
   * Suggested fix (if any).
   */
  suggestion?: string | null;
  [k: string]: unknown;
}
/**
 * Location in a source file.
 */
export interface SourceSpan {
  col: number;
  file: string;
  line: number;
  [k: string]: unknown;
}
/**
 * Per-model summary projected from `rocky_core::models::ModelConfig`.
 *
 * Intentionally excludes fields that change run-to-run (timings, diagnostics) — those live on the run-level outputs. This is the stable, declarative shape of one compiled model, suitable for orchestrators that build asset definitions ahead of execution.
 */
export interface ModelDetail {
  /**
   * How the contract was discovered: `"auto"` (sibling `.contract.toml`), `"explicit"` (via `--contracts` flag), or absent when no contract.
   */
  contract_source?: string | null;
  /**
   * DAG-propagated cost estimate for this model. Populated at compile time using heuristic cardinality propagation (no warehouse round-trip). `None` when no upstream table statistics are available.
   */
  cost_hint?: CostHint | null;
  /**
   * Names of models this model directly depends on. Derived from the model's TOML `depends_on` list or auto-resolved from SQL table references. Empty when the model has no upstream dependencies.
   */
  depends_on?: string[];
  /**
   * Per-model freshness expectation, when declared in the model's TOML frontmatter. `None` when not configured.
   */
  freshness?: ModelFreshnessConfig | null;
  /**
   * When the model uses `full_refresh` and has columns that look monotonic, this hint suggests switching to incremental materialization. `None` when the model already uses an incremental strategy or no candidates were found.
   */
  incrementality_hint?: IncrementalityHint | null;
  name: string;
  /**
   * Materialization strategy as the wire-shape `StrategyConfig` (`{"type": "...", ...}`).
   */
  strategy: StrategyConfig;
  /**
   * Target table coordinates.
   */
  target: TargetConfig;
  [k: string]: unknown;
}
/**
 * Heuristic cost estimate derived from DAG-aware cardinality propagation.
 *
 * These numbers are directional — useful for comparing models within a project and surfacing expensive operations in the LSP, but not precise enough to substitute for a warehouse EXPLAIN. Use `rocky estimate` for warehouse-backed estimates.
 */
export interface CostHint {
  /**
   * Confidence level: `"low"`, `"medium"`, or `"high"`.
   */
  confidence: string;
  /**
   * Estimated total output bytes.
   */
  estimated_bytes: number;
  /**
   * Estimated compute cost in USD.
   */
  estimated_cost_usd: number;
  /**
   * Estimated number of output rows.
   */
  estimated_rows: number;
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
 * A hint that a model could benefit from incremental materialization.
 *
 * Returned by [`infer_incrementality`] when a `full_refresh` model has columns that look monotonic. Surfaced in `rocky compile --output json` as part of each model's detail.
 */
export interface IncrementalityHint {
  /**
   * How confident the detector is in this recommendation.
   */
  confidence: Confidence;
  /**
   * Whether the model is a candidate for incremental materialization.
   */
  is_candidate: boolean;
  /**
   * The column recommended as the watermark / timestamp column.
   */
  recommended_column: string;
  /**
   * Human-readable reasons why this column was chosen.
   */
  signals: string[];
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
