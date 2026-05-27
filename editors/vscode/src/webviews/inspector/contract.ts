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

/** Params for the model-scoped RPC requests (`model`, `tests`, `preview`, `profile`). */
export interface ModelParam {
  model: string;
}

/**
 * Initial intent pushed when the Inspector view is revealed: which model to
 * load and, optionally, which tab to open. Re-targets after that are
 * webview-driven (a `model` request keyed on the focused model — e.g. a click
 * on a node in the Lineage tab's canvas), so this carries only the first model.
 */
export interface InspectorTarget {
  /** Model to focus; omitted by Show Lineage when no model is resolvable. */
  model?: string;
  tab?: string;
}

/**
 * Result shape the webview holds for the focused model: the resolved
 * {@link InspectorModelData}, or the error string if the `model` request failed.
 */
export type ModelPush =
  | { ok: true; data: InspectorModelData }
  | { ok: false; error: string };
