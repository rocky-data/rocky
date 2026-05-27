/**
 * Type-only contract for the Inspector panel, shared by the extension host
 * (`src/commands/inspector.ts`) and the React app (`webview-ui/panels/inspector`).
 * References the generated CLI output types so both sides agree on field shapes.
 */
import type { CatalogColumn, CatalogEdge } from "../../types/generated/catalog";
import type { CostHint, ModelFreshnessConfig } from "../../types/generated/compile";
import type { DeclarativeTestResult } from "../../types/generated/test";
import type { PreviewRowsOutput } from "../../types/generated/preview_rows";

/**
 * Model summary pushed to the webview when the Inspector opens or re-targets.
 * Fans `rocky catalog` (asset + columns + lineage + last-materialized) and
 * `rocky compile` (contract, freshness, materialization, heuristic cost) into
 * one model-scoped payload.
 */
export interface InspectorModelData {
  modelName: string;
  fqn: string;
  /** Asset kind: `"Source" | "Model" | "View" | "MaterializedView"`. */
  kind: string;
  /** Natural-language description from the model's sidecar, if any. */
  intent: string | null;
  lastMaterializedAt: string | null;
  columns: CatalogColumn[];
  upstreamModels: string[];
  downstreamModels: string[];
  /** Column-level lineage edges touching this model (powers the Lineage tab). */
  columnEdges: CatalogEdge[];
  /** `"auto"` | `"explicit"` when a contract exists, else `null`. */
  contractSource: string | null;
  freshness: ModelFreshnessConfig | null;
  /** Materialization strategy discriminant (`"full_refresh"`, `"view"`, …). */
  materialization: string | null;
  /** Heuristic, compile-time cost estimate (directional, not a warehouse EXPLAIN). */
  costHint: CostHint | null;
}

/** Declarative test results scoped to one model (lazy "Tests" tab). */
export interface InspectorTestsData {
  results: DeclarativeTestResult[];
  /** Set when tests could not run (e.g. CLI error); `results` is then empty. */
  unavailable?: string;
}

/** A row preview for one model (lazy "Preview" tab). */
export interface InspectorPreviewData {
  preview?: PreviewRowsOutput;
  /** Set when the preview is unavailable (warehouse-gated, error, …). */
  unavailable?: string;
}

/** Params for the model-scoped lazy RPC requests. */
export interface ModelParam {
  model: string;
}

/** Push type carrying {@link InspectorModelData} (or an error string). */
export type ModelPush =
  | { ok: true; data: InspectorModelData }
  | { ok: false; error: string };
