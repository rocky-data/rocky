/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/compile.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Severity level of a diagnostic.
 *
 * Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay compatible with existing dagster fixtures and the hand-written `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
 */
export type Severity = "Error" | "Warning" | "Info";
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
 * JSON output for `rocky compile`.
 */
export interface CompileOutput {
  command: string;
  compile_timings: PhaseTimings;
  /**
   * Diagnostics for the models this result describes. Under `--model`, filtered to those whose owning model name equals the selector exactly — so a diagnostic raised against a different model is absent even when that model is an upstream of the selected one.
   *
   * Attribution is not always a model you can select: import diagnostics (`E033`, `E034`, `W012`) carry the import name and `W011` carries a contract name, none of which are valid `--model` values. A target collision (`E036`) is attached to every participating model, so one diagnostic can appear under several selectors.
   */
  diagnostics: Diagnostic[];
  /**
   * Number of execution layers this result describes.
   *
   * Under `--model` only the layers containing the selected model are counted, so this reports `1`. It is not the selected model's depth in the DAG, and not how many layers a rebuild of it would need — treat it as informative only when no selector is passed.
   */
  execution_layers: number;
  /**
   * Expanded SQL for each model after macro substitution. Only populated when `--expand-macros` is passed. Keys are model names, values are the SQL after all `@macro()` calls have been replaced.
   */
  expanded_sql?: {
    [k: string]: string;
  };
  /**
   * Whether the diagnostics this result describes include error-severity entries — and, for the CLI, whether the command exits non-zero.
   *
   * Under `--model` this is computed from the exact-attribution filter above, so it says nothing about whether the selected model's upstreams compile: an error attributed to another model is filtered out and leaves this `false`, even though `rocky run` would classify that model as a compile error and exclude it from execution. A failure that aborts compilation outright rather than emitting a diagnostic — an unparseable model, a semantic-graph or contract-load failure — fails the command before any scoping applies. To learn whether a model can be built, compile without a selector.
   */
  has_errors: boolean;
  /**
   * Number of models this result describes — the whole project, or `1` when `--model` selects one. Not a project-wide total under a selector.
   */
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
 *
 * `code` and `message` use `Arc<str>` (§P3.5) — cloning a `Diagnostic` in the LSP publish loop becomes a refcount bump. Construction still accepts any `Into<String>` / `&str` via the helper constructors below; the arc wrap happens once at construction time.
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
  name: string;
  /**
   * Materialization strategy as the wire-shape `StrategyConfig` (`{"type": "...", ...}`).
   */
  strategy: StrategyConfig;
  /**
   * Model-level governance tags — the model's own `[tags]` block merged over any config-group `[tags]` baseline (sidecar > group). Free-form key/value strings describing the model as a whole (`domain`, `tier`, `owner`, …). `dagster-rocky` projects these onto the derived asset's Dagster tags, so a governed fan-out declared once on a config group is visible to the orchestrator end-to-end. Empty when none are declared.
   */
  tags?: {
    [k: string]: string;
  };
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
 * Declares the maximum allowed lag between successive materializations of the model plus the optional timestamp column used by the runtime freshness check.
 *
 * The compiler does not enforce the TTL — it's metadata consumed by downstream observability tooling (`dagster-rocky` `FreshnessPolicy`, `rocky doctor --freshness`, etc.). The compiler does however soft-warn (W005) when a model has at least one temporal output column but no `freshness` declaration anywhere in scope (per-model or project-level default).
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
