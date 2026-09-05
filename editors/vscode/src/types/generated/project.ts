/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/project.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The project a sidecar serves, for `GET /api/v1/project`: what the server-rendered dashboard at `/` used to show, as a typed payload.
 *
 * Server-only, no CLI twin. Bounded by construction: names of pipelines and adapters (a handful), counts for models and diagnostics, and the one newest run. The model names are on `GET /api/v1/models` and the DAG.
 */
export interface ProjectOutput {
  /**
   * Every `[adapter.<name>]`, in config order.
   */
  adapters: ProjectAdapterOutput[];
  /**
   * Why the config could not be loaded, when it could not. The lists below are then empty.
   */
  config_error?: string | null;
  /**
   * The bound config's path; `null` when the sidecar has none.
   */
  config_path?: string | null;
  /**
   * The compile's diagnostics, counted.
   */
  diagnostics: ProjectDiagnosticsOutput;
  /**
   * The newest run in the state store the server resolved; `null` when none was recorded.
   */
  last_run?: ProjectRunOutput | null;
  /**
   * Models in the in-memory compile result; `null` before the first compile finishes.
   */
  models_compiled?: number | null;
  /**
   * The directory the bound `rocky.toml` lives in, or `rocky` when no config is bound.
   */
  name: string;
  /**
   * Every `[pipeline.<name>]`, in config order.
   */
  pipelines: ProjectPipelineOutput[];
  [k: string]: unknown;
}
/**
 * One adapter of the bound config.
 */
export interface ProjectAdapterOutput {
  /**
   * The adapter's `type` as configured (`duckdb`, `databricks`, …).
   */
  adapter_type: string;
  name: string;
  [k: string]: unknown;
}
/**
 * The compile's diagnostics, counted. All zero before the first compile.
 */
export interface ProjectDiagnosticsOutput {
  has_errors: boolean;
  total: number;
  warnings: number;
  [k: string]: unknown;
}
/**
 * The newest run, as the dashboard summarised it.
 */
export interface ProjectRunOutput {
  /**
   * RFC 3339.
   */
  finished_at: string;
  models_executed: number;
  run_id: string;
  /**
   * RFC 3339.
   */
  started_at: string;
  /**
   * The run's recorded status (`Success`, `PartialFailure`, `Failure`, …).
   */
  status: string;
  /**
   * What started it (`Manual`, `Schedule`, `Webhook`, …).
   */
  trigger: string;
  [k: string]: unknown;
}
/**
 * One pipeline of the bound config.
 */
export interface ProjectPipelineOutput {
  name: string;
  /**
   * `replication`, `transformation`, `quality`, `snapshot` or `load`.
   */
  pipeline_type: string;
  [k: string]: unknown;
}
