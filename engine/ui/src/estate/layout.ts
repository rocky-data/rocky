/**
 * The DAG's layout is the engine's: `execution_layers` gives each model its
 * column, its position in the layer gives its row, and `edges` gives the
 * lines. No layout library (the stack ruling: the engine computes, React
 * Flow renders). A dangling edge, one whose endpoint is not among the nodes,
 * is dropped rather than drawn to nowhere.
 */

import type { Edge, Node } from "@xyflow/react";
import type { DagNodeOutput, DagOutput } from "@rocky-types/dag";
import { nodeRoute } from "./nodeRoute";

export interface ModelNodeData extends Record<string, unknown> {
  label: string;
  kind: string;
  strategy: string | null;
  target: string | null;
  pipeline: string | null;
  layer: number;
}

export type ModelFlowNode = Node<ModelNodeData, "model">;

export const NODE_WIDTH = 184;
export const NODE_HEIGHT = 46;
const COLUMN_GAP = 96;
const ROW_GAP = 28;

export interface Flow {
  nodes: ModelFlowNode[];
  edges: Edge[];
  /** Edges dropped because an endpoint was not a node. */
  dropped: number;
}

function strategyOf(node: DagNodeOutput): string | null {
  const strategy = node.strategy as { type?: unknown } | null | undefined;
  return strategy && typeof strategy.type === "string" ? strategy.type : null;
}

function targetOf(node: DagNodeOutput): string | null {
  const t = node.target;
  return t ? `${t.catalog}.${t.schema}.${t.table}` : null;
}

function position(column: number, row: number, rowsInColumn: number) {
  const height = rowsInColumn * NODE_HEIGHT + (rowsInColumn - 1) * ROW_GAP;
  return {
    x: column * (NODE_WIDTH + COLUMN_GAP),
    y: row * (NODE_HEIGHT + ROW_GAP) - height / 2,
  };
}

export function layeredFlow(dag: DagOutput): Flow {
  const byId = new Map(dag.nodes.map((n) => [n.id, n]));
  const placed = new Set<string>();
  const nodes: ModelFlowNode[] = [];

  const toNode = (node: DagNodeOutput, column: number, row: number, rows: number): ModelFlowNode => {
    // Only a node the detail route can serve is reachable by keyboard. React
    // Flow honours these per node: `tabIndex`, `onKeyDown` and `role` are all
    // gated on `focusable`, and the global flags apply only where a node
    // leaves them undefined. So the rest leave the tab order entirely.
    const openable = nodeRoute(node).state === "servable";
    return {
      id: node.id,
      type: "model",
      position: position(column, row, rows),
      // The card's own size, stated rather than left to be measured. The
      // minimap draws nothing for a node whose dimensions it does not know
      // (`nodeHasDimensions`), and it never learns them here: the graph is
      // controlled with no `onNodesChange`, so React Flow has nowhere to
      // write a measurement back to. Both values are the ones `ModelNode`
      // and `position()` already use, so nothing moves.
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
      draggable: false,
      focusable: openable,
      selectable: openable,
      // A node that opens a pane is a button. One that does not is marked
      // as such for assistive technology, and takes no role of its own.
      ariaRole: openable ? "button" : undefined,
      domAttributes: openable ? undefined : { "aria-disabled": true },
      data: {
        label: node.label,
        kind: node.kind,
        strategy: strategyOf(node),
        target: targetOf(node),
        pipeline: node.pipeline ?? null,
        layer: column,
      },
    };
  };

  dag.execution_layers.forEach((layer, column) => {
    const ids = layer.filter((id) => byId.has(id) && !placed.has(id));
    ids.forEach((id, row) => {
      placed.add(id);
      nodes.push(toNode(byId.get(id) as DagNodeOutput, column, row, ids.length));
    });
  });

  // A node the layers never mention (the engine lists it but did not
  // schedule it) still shows, in a trailing column, so nothing is hidden.
  const orphans = dag.nodes.filter((n) => !placed.has(n.id));
  orphans.forEach((node, row) => {
    nodes.push(toNode(node, dag.execution_layers.length, row, orphans.length));
  });

  const ids = new Set(nodes.map((n) => n.id));
  const edges: Edge[] = dag.edges
    .filter((e) => ids.has(e.from) && ids.has(e.to))
    .map((e) => ({
      id: `${e.from}->${e.to}`,
      source: e.from,
      target: e.to,
      type: "smoothstep",
    }));

  return { nodes, edges, dropped: dag.edges.length - edges.length };
}
