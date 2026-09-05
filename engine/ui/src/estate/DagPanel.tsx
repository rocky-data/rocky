import { Background, Controls, MiniMap, ReactFlow, ReactFlowProvider } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";
import type { DagOutput } from "@rocky-types/dag";
import { EmptyState } from "../components";
import { layeredFlow } from "./layout";
import { ModelNode } from "./ModelNode";

const nodeTypes = { model: ModelNode };

/**
 * The project's DAG, laid out by the engine's execution layers. A node click
 * selects the model for the detail pane. The visually hidden list names
 * every model for assistive technology, and is what the tests read: React
 * Flow only paints once the canvas has a size.
 */
export function DagPanel({ dag, onSelect }: { dag: DagOutput; onSelect: (id: string) => void }) {
  const flow = useMemo(() => layeredFlow(dag), [dag]);

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
            onNodeClick={(_, node) => onSelect(node.id)}
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
