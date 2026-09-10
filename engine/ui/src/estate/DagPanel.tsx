import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useRef, type RefObject } from "react";
import type { DagOutput } from "@rocky-types/dag";
import { EmptyState } from "../components";
import { layeredFlow, type ModelNodeData } from "./layout";
import { ModelNode } from "./ModelNode";
import { nodeRoute } from "./nodeRoute";

const nodeTypes = { model: ModelNode };

/**
 * How far a person may zoom out by hand.
 *
 * A node is 184×46, so at 0.1 it is about 18×5 CSS pixels: small, but still a
 * shape you can read the graph from and zoom back out of. This is the
 * *interaction* floor and it is deliberately not the fitting floor.
 */
const MIN_ZOOM = 0.1;

/**
 * How far an automatic fit may zoom out, which has to go further.
 *
 * `fitView` cannot pass `minZoom`, and the library default of 0.5 could not
 * fit a 744px graph into the ~288px of canvas a 320px phone leaves — that
 * needs 0.39, so both end nodes were simply cut off. A deep DAG needs less
 * again. Kept a little under the interaction floor rather than far under it:
 * a fit below `MIN_ZOOM` is clamped back up by the next zoom gesture, so the
 * gap between the two is a discontinuity to be spent sparingly.
 */
const FIT = { minZoom: 0.02 } as const;

/**
 * Re-fit the graph when the canvas changes width.
 *
 * `fitView` runs once, at mount. Opening the detail pane takes the canvas from
 * the full width down to `1fr` beside a 360px column — measured, 990px to
 * 620px at a 1024px viewport — and the graph kept its old zoom, so the last
 * node simply left the canvas. Rotating a phone does the same thing.
 *
 * Width only: the height is fixed, and re-fitting on every height change would
 * fight a scroll. A refit does discard a manual pan or zoom, which is the
 * right trade when the alternative is a graph that is partly off-screen.
 */
function RefitOnResize({ canvas }: { canvas: RefObject<HTMLDivElement | null> }) {
  const { fitView } = useReactFlow();
  const lastWidth = useRef(0);

  useEffect(() => {
    // This panel's own canvas, passed by ref. A `document.querySelector` here
    // would find the FIRST flow in the document, so a second panel would
    // observe the first one's box and reset the first one's viewport.
    const pane = canvas.current;
    if (!pane || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const width = Math.round(entries[0]?.contentRect.width ?? 0);
      if (width === 0 || width === lastWidth.current) return;
      lastWidth.current = width;
      // `fitView` changes the inner transform, not the observed box, so this
      // cannot feed itself.
      void fitView(FIT);
    });
    observer.observe(pane);
    return () => observer.disconnect();
  }, [canvas, fitView]);

  return null;
}

/**
 * The project's DAG, laid out by the engine's execution layers. Clicking a
 * node that the detail route can serve opens its pane, under the bare model
 * name the route wants. The visually hidden list names every model for
 * assistive technology, and is what the tests read: React Flow only paints
 * once the canvas has a size.
 */
export function DagPanel({ dag, onSelect }: { dag: DagOutput; onSelect: (name: string) => void }) {
  const canvasRef = useRef<HTMLDivElement | null>(null);
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
        ref={canvasRef}
        onKeyDown={openFocusedNode}
      >
        <ReactFlowProvider>
          <ReactFlow
            nodes={flow.nodes}
            edges={flow.edges}
            nodeTypes={nodeTypes}
            fitView
            // Two different floors, because they answer different questions:
            // how small a fit may go, and how small a person may drag it.
            fitViewOptions={FIT}
            minZoom={MIN_ZOOM}
            nodesDraggable={false}
            nodesConnectable={false}
            proOptions={{ hideAttribution: true }}
            colorMode="system"
            onNodeClick={(_, node) => open(node.data)}
          >
            <RefitOnResize canvas={canvasRef} />
            <Background />
            <MiniMap pannable zoomable />
            <Controls showInteractive={false} fitViewOptions={FIT} />
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
