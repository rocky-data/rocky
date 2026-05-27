/**
 * Type-only contract for the Lineage canvas, shared by the extension host
 * (`src/commands/lineage.ts`) and the React app (`webview-ui/panels/lineage`).
 */

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
