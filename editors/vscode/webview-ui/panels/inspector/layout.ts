import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";
import type {
  CostHint,
  ModelFreshnessConfig,
} from "../../../src/types/generated/compile";
import type { GraphData } from "../../../src/webviews/lineage/contract";

/** Layout direction: left-to-right or top-to-bottom. */
export type Direction = "LR" | "TB";

/** Data carried by a model node, including the fields overlays decorate from. */
export interface ModelNodeData extends Record<string, unknown> {
  label: string;
  kind: string;
  materialization: string | null;
  fqn: string;
  costHint: CostHint | null;
  freshness: ModelFreshnessConfig | null;
}

export type ModelFlowNode = Node<ModelNodeData, "model">;

const NODE_WIDTH = 184;
const NODE_HEIGHT = 46;

/**
 * Convert the project graph into positioned ReactFlow nodes + edges using a
 * headless dagre layout. Edges referencing a missing endpoint are dropped so
 * ReactFlow never renders a dangling connection.
 */
export function toFlow(
  data: GraphData,
  direction: Direction,
): { nodes: ModelFlowNode[]; edges: Edge[] } {
  const graph = new dagre.graphlib.Graph();
  graph.setGraph({ rankdir: direction, nodesep: 28, ranksep: 72 });
  graph.setDefaultEdgeLabel(() => ({}));

  const ids = new Set(data.nodes.map((n) => n.id));
  for (const node of data.nodes) {
    graph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  }
  const edges = data.edges.filter(
    (e) => ids.has(e.source) && ids.has(e.target),
  );
  for (const edge of edges) graph.setEdge(edge.source, edge.target);

  dagre.layout(graph);

  const nodes: ModelFlowNode[] = data.nodes.map((node) => {
    const laid = graph.node(node.id);
    return {
      id: node.id,
      type: "model",
      position: { x: laid.x - NODE_WIDTH / 2, y: laid.y - NODE_HEIGHT / 2 },
      data: {
        label: node.label,
        kind: node.kind,
        materialization: node.materialization,
        fqn: node.fqn,
        costHint: node.costHint,
        freshness: node.freshness,
      },
    };
  });

  const flowEdges: Edge[] = edges.map((edge) => ({
    id: `${edge.source}->${edge.target}`,
    source: edge.source,
    target: edge.target,
    type: "smoothstep",
  }));

  return { nodes, edges: flowEdges };
}
