/**
 * Type-only contract for the Lineage canvas, shared by the extension host
 * (`src/commands/lineage.ts`) and the React app (`webview-ui/panels/lineage`).
 */
import type {
  CostHint,
  ModelFreshnessConfig,
} from "../../types/generated/compile";

/** One model or source node in the project graph. */
export interface GraphNode {
  /** Stable key — the model/source name as it appears in lineage edges. */
  id: string;
  /** Display label (unqualified name). */
  label: string;
  /** Catalog asset kind: `"Source" | "Model" | "View" | "MaterializedView"`. */
  kind: string;
  /** Materialization strategy discriminant from compile, when known. */
  materialization: string | null;
  /** Fully-qualified target identifier. */
  fqn: string;
  /** Heuristic compile-time cost estimate, when available (powers the cost overlay). */
  costHint: CostHint | null;
  /** Declared freshness policy, when configured (powers the freshness overlay). */
  freshness: ModelFreshnessConfig | null;
}

/** A drift action for one table, from `rocky drift`. */
export interface DriftAction {
  table: string;
  action: string;
  reason: string;
}

/** Drift overlay payload — the result of the `"drift"` request. */
export interface DriftData {
  actions: DriftAction[];
  /** Set when drift could not be computed (e.g. no prior run). */
  unavailable?: string;
}

/** A model-level dependency edge (upstream producer → downstream consumer). */
export interface GraphEdge {
  source: string;
  target: string;
}

/** The full project graph, returned by the `"graph"` request. */
export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

/** `"focus"` push: center on and select this model. */
export interface FocusPush {
  model: string;
}

/** Model-scoped request params (`openFile`, `openInspector`). */
export interface ModelParam {
  model: string;
}

/** AI action invoked from a node's right-click menu. */
export type AiAction = "explain" | "test" | "contract" | "build";

/** Params for the `"ai"` request. */
export interface AiActionParam {
  action: AiAction;
  model: string;
}

/** One breaking-change finding, scoped to a model (bare name or fqn). */
export interface BreakingFindingLite {
  model: string;
  severity: string;
}

/** Breaking-change overlay payload — the result of the `"breaking"` request. */
export interface BreakingData {
  baseRef: string;
  findings: BreakingFindingLite[];
  /** Set when the diff could not run (e.g. base ref missing). */
  unavailable?: string;
}

/** One model's outcome in the most recent run. */
export interface ReplayModelLite {
  model: string;
  rows: number | null;
  status: string;
}

/** Last-run overlay payload — the result of the `"replay"` request. */
export interface ReplayData {
  runId?: string;
  models: ReplayModelLite[];
  /** Set when there is no run to replay. */
  unavailable?: string;
}

/** One model's governance posture: classified columns and unmasked exceptions. */
export interface GovernanceModelLite {
  model: string;
  classifiedColumns: number;
  unmaskedColumns: number;
}

/** Governance overlay payload — the result of the `"governance"` request. */
export interface GovernanceData {
  models: GovernanceModelLite[];
  unavailable?: string;
}
