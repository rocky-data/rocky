import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from "react";
import type { DagOutput } from "@rocky-types/dag";
import { EmptyState } from "../components";
import { layeredFlow, type ModelFlowNode, type ModelNodeData } from "./layout";
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
 * The library's default floor of 0.5 could not fit a 744px graph into the
 * ~288px of canvas a 320px phone leaves — that needs 0.39, so both end nodes
 * were cut off. A deep DAG needs less again: eleven execution layers already
 * need 0.0965 at that width.
 *
 * This is passed per fit — to `fitViewOptions`, to `fitView()` and to the
 * Controls — so it is not itself the floor for a hand on the canvas. It can
 * still end up being one: a graph deep enough to fit at this clamp makes
 * `floorFor` adopt it, which is the point. What it never does is lower the
 * floor for a graph that fits above it.
 */
const FIT = { minZoom: 0.02 } as const;

/**
 * The floor a fit actually used, so the next zoom gesture does not snap.
 *
 * A fit is allowed below `MIN_ZOOM`, but D3 applies the interaction floor to
 * the next gesture, so an eleven-layer graph fitted at 0.0965 would jump to
 * 0.1 — and a forty-two-layer one from 0.0225 to 0.1, which is 5× and throws
 * away most of the graph the fit had just framed. So the floor follows the
 * fit down. It never rises above `MIN_ZOOM`, so a small graph keeps a floor
 * that stops anyone shrinking a node to a few pixels for no reason.
 */
export function floorFor(fitted: number): number {
  return Math.min(MIN_ZOOM, fitted);
}

/**
 * What counts as "the graph changed", for deciding whether to re-fit.
 *
 * Not the node ids. A node's position comes from its `execution_layers` index
 * and its row (`layout.ts`), so adding a `depends_on` between two models that
 * already exist moves nodes and changes the graph's span while the id set is
 * identical. Keyed on ids alone that reshape would not re-fit, and the graph
 * could reach past the canvas — which is the defect this whole mechanism
 * exists to prevent, arriving by one more route.
 */
export function layoutIdentity(nodes: readonly ModelFlowNode[]): string {
  return nodes.map((n) => `${n.id}@${n.position.x},${n.position.y}`).join("\u0000");
}

/**
 * Re-fit the graph, and report the zoom the fit settled on.
 *
 * Two things make a fit necessary, and both were once missed:
 *
 * - **The canvas changes width.** `fitView` otherwise runs once, at mount.
 *   Opening the detail pane takes the canvas from 990px to 620px at a 1024px
 *   viewport and the graph kept its old zoom, so the last node left the
 *   canvas. Rotating a phone does the same.
 * - **The graph itself changes.** `Refresh` replaces `dag` without remounting
 *   this panel, so a shallow graph can become a deep one at an unchanged
 *   width. Nothing refitted, and the floor stayed where the shallow graph put
 *   it — which brought the zoom snap back by another route.
 *
 * Every fit reports its zoom, because the interaction floor follows it.
 *
 * A fit requested right after a graph change does not resolve immediately:
 * React Flow queues it until its own observer has measured the new nodes.
 * That is why the zoom is read in the promise continuation rather than on the
 * next line. (The dimensions `layout.ts` declares are what let the minimap
 * draw a node; they do not skip that measurement pass.)
 *
 * Width only, not height: the height is fixed, and re-fitting on every height
 * change would fight a scroll. A refit discards a manual pan or zoom, which is
 * the right trade when the alternative is a graph partly off-screen.
 */
function RefitOnChange({
  canvas,
  graph,
  onFitted,
}: {
  canvas: RefObject<HTMLDivElement | null>;
  /** Changes when the graph does, so a replaced DAG refits. */
  graph: string;
  onFitted: (zoom: number) => void;
}) {
  const { fitView, getZoom } = useReactFlow();
  const lastWidth = useRef(0);

  const refit = useCallback(() => {
    void fitView(FIT).then(() => onFitted(getZoom()));
  }, [fitView, getZoom, onFitted]);

  // On mount, and whenever the layout identity changes. `onInit` is not
  // used for this: it
  // gates viewport initialisation and is not guaranteed to follow the initial
  // fit, so a floor taken from it could come from an unfitted viewport.
  useEffect(() => {
    refit();
  }, [graph, refit]);

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
      refit();
    });
    observer.observe(pane);
    return () => observer.disconnect();
  }, [canvas, refit]);

  return null;
}

/**
 * The stock controls, with the fit button reporting what it actually did.
 *
 * `onFitView` runs after the control's own `fitView` call, but that call only
 * QUEUES the transform, so the callback still sees the zoom from before the
 * fit. Reading it there recorded the old value — pressing Fit while zoomed in
 * raised the floor and the queued fit then dropped below it, which is the
 * snap coming back by a third route. So this runs its own awaited fit and
 * reads after that.
 */
function FittingControls({ onFitted }: { onFitted: (zoom: number) => void }) {
  const { fitView, getZoom } = useReactFlow();
  return (
    <Controls
      showInteractive={false}
      fitViewOptions={FIT}
      // Its own awaited fit, not a read after the control's. The control
      // calls `fitView()` and then `onFitView` immediately, but that call
      // only QUEUES the transform, so reading the zoom there returns the one
      // from BEFORE the fit. Pressing this while zoomed in would then record
      // a high floor, and the queued fit would drop below it — the snap
      // again. Both fits target the same bounds, so the extra one is
      // redundant rather than conflicting.
      onFitView={() => void fitView(FIT).then(() => onFitted(getZoom()))}
    />
  );
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
  const [minZoom, setMinZoom] = useState(MIN_ZOOM);
  const onFitted = useCallback((zoom: number) => setMinZoom(floorFor(zoom)), []);
  const flow = useMemo(() => layeredFlow(dag), [dag]);
  // The identity of the LAYOUT, so a Refresh that reshapes it refits.
  const graph = useMemo(() => layoutIdentity(flow.nodes), [flow]);
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
            // The fit may go lower than the hand, and the hand's floor
            // follows it down so the first gesture after a fit does not snap.
            fitViewOptions={FIT}
            minZoom={minZoom}
            nodesDraggable={false}
            nodesConnectable={false}
            proOptions={{ hideAttribution: true }}
            colorMode="system"
            onNodeClick={(_, node) => open(node.data)}
          >
            <RefitOnChange canvas={canvasRef} graph={graph} onFitted={onFitted} />
            <Background />
            <MiniMap pannable zoomable />
            <FittingControls onFitted={onFitted} />
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
