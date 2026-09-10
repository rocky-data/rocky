import { Background, Controls, MiniMap, ReactFlow, ReactFlowProvider } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";
import type { DagOutput } from "@rocky-types/dag";
import { EmptyState } from "../components";
import { layeredFlow, type ModelNodeData } from "./layout";
import { ModelNode } from "./ModelNode";
import { nodeRoute } from "./nodeRoute";

const nodeTypes = { model: ModelNode };

/**
 * The project's DAG, laid out by the engine's execution layers. Clicking a
 * node that the detail route can serve opens its pane, under the bare model
 * name the route wants. The visually hidden list names every model for
 * assistive technology, and is what the tests read: React Flow only paints
 * once the canvas has a size.
 */
export function DagPanel({ dag, onSelect }: { dag: DagOutput; onSelect: (name: string) => void }) {
  const flow = useMemo(() => layeredFlow(dag), [dag]);
  const dataById = useMemo(
    () => new Map(flow.nodes.map((node) => [node.id, node.data])),
    [flow],
  );

  /** Open the pane if this node has one. Says whether it did. */
  const open = (data: ModelNodeData): boolean => {
    const route = nodeRoute(data);
    if (route.state !== "servable") return false;
    onSelect(route.model);
    return true;
  };

  /**
   * The keyboard path. React Flow routes Enter and Space to selection only:
   * a node's own `onKeyDown` changes the store and never calls `onNodeClick`,
   * which fires from the mouse path alone. It also does not stop the event,
   * so the key arrives here from whichever node holds focus. Nodes with no
   * pane are not focusable, so they never reach this.
   */
  const openFocusedNode = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const node = (event.target as HTMLElement | null)?.closest?.(".react-flow__node");
    const data = dataById.get(node?.getAttribute("data-id") ?? "");
    // Space scrolls the page by default; only swallow it when it opened one.
    if (data && open(data)) event.preventDefault();
  };

  if (dag.nodes.length === 0) {
    return (
      <EmptyState
        title="No models compiled"
        detail="The server's models directory has nothing to graph yet."
      />
    );
  }

  return (
    <div>
      <ul className="sr-only" aria-label="Models in the DAG">
        {flow.nodes.map((node) => (
          <li key={node.id}>
            {node.data.label} (layer {node.data.layer + 1}, {node.data.kind})
          </li>
        ))}
      </ul>
      <div
        className="h-[480px] rounded-md border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900"
        data-testid="dag-canvas"
        onKeyDown={openFocusedNode}
      >
        <ReactFlowProvider>
          <ReactFlow
            nodes={flow.nodes}
            edges={flow.edges}
            nodeTypes={nodeTypes}
            fitView
            nodesDraggable={false}
            nodesConnectable={false}
            proOptions={{ hideAttribution: true }}
            colorMode="system"
            onNodeClick={(_, node) => open(node.data)}
          >
            <Background />
            <MiniMap pannable zoomable />
            <Controls showInteractive={false} />
          </ReactFlow>
        </ReactFlowProvider>
      </div>
      <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
        {dag.summary.total_nodes} models, {dag.summary.total_edges} edges,{" "}
        {dag.execution_layers.length} execution layers
        {flow.dropped > 0 ? `; ${flow.dropped} edge(s) named a node that is not in the graph` : ""}
      </p>
    </div>
  );
}
