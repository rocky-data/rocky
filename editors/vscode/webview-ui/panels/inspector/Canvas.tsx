import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Edge,
} from "@xyflow/react";
import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type MouseEvent as RFMouseEvent,
} from "react";
import type { AiAction, GraphData } from "../../../src/webviews/lineage/contract";
import { ContextMenu, type MenuItem } from "./ContextMenu";
import { downstreamOf, labelMatches, neighborhood, upstreamOf } from "./graph";
import { toFlow, type ModelFlowNode } from "./layout";
import { ModelNode } from "./nodes/ModelNode";

const nodeTypes = { model: ModelNode };

interface CanvasProps {
  data: GraphData;
  focus: string | null;
  search: string;
  /** Whether the canvas is currently shown; a hidden→shown flip triggers a re-fit. */
  active?: boolean;
  onOpenFile: (model: string) => void;
  onOpenInspector: (model: string) => void;
  onAi: (action: AiAction, model: string) => void;
}

interface MenuState {
  x: number;
  y: number;
  model: string;
}

export function Canvas({
  data,
  focus,
  search,
  active,
  onOpenFile,
  onOpenInspector,
  onAi,
}: CanvasProps) {
  const baseFlow = useMemo(() => toFlow(data, "LR"), [data]);
  const [nodes, setNodes, onNodesChange] = useNodesState<ModelFlowNode>(
    baseFlow.nodes,
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(baseFlow.edges);
  const [focusFilter, setFocusFilter] = useState<Set<string> | null>(null);
  const [menu, setMenu] = useState<MenuState | null>(null);
  const { fitView } = useReactFlow();

  // Reload nodes/edges when the project graph changes.
  useEffect(() => {
    setNodes(baseFlow.nodes);
    setEdges(baseFlow.edges);
  }, [baseFlow, setNodes, setEdges]);

  // Apply search + focus-filter visibility without disturbing positions/selection.
  useEffect(() => {
    const labels = new Map(baseFlow.nodes.map((n) => [n.id, n.data.label]));
    const visible = (id: string): boolean =>
      (!focusFilter || focusFilter.has(id)) &&
      labelMatches(labels.get(id) ?? id, search);
    setNodes((prev) => prev.map((n) => ({ ...n, hidden: !visible(n.id) })));
    setEdges((prev) =>
      prev.map((e) => ({ ...e, hidden: !(visible(e.source) && visible(e.target)) })),
    );
  }, [search, focusFilter, baseFlow, setNodes, setEdges]);

  // A focus push (from `rocky.showLineage <model>`) narrows to that model's
  // neighborhood and frames it — mirroring the old model-centric lineage view.
  useEffect(() => {
    if (!focus) return;
    setFocusFilter(neighborhood(data.edges, focus));
    setNodes((prev) => prev.map((n) => ({ ...n, selected: n.id === focus })));
    const timer = setTimeout(
      () => void fitView({ duration: 400, maxZoom: 1.3 }),
      80,
    );
    return () => clearTimeout(timer);
  }, [focus, data.edges, fitView, setNodes]);

  // Re-fit when the canvas becomes visible. It may have first mounted while its
  // tab was hidden (display:none, 0×0), where ReactFlow can't frame the graph;
  // fitting once it has real dimensions centers it. No maxZoom cap so a small
  // graph fills a maximized panel; padding gives the nodes breathing room.
  useEffect(() => {
    if (!active) return;
    const timer = setTimeout(
      () => void fitView({ duration: 200, padding: 0.2 }),
      250,
    );
    return () => clearTimeout(timer);
  }, [active, fitView]);

  const onNodeDoubleClick = useCallback(
    (_: RFMouseEvent, node: ModelFlowNode) => onOpenFile(node.id),
    [onOpenFile],
  );
  const onNodeContextMenu = useCallback(
    (event: RFMouseEvent, node: ModelFlowNode) => {
      event.preventDefault();
      setMenu({ x: event.clientX, y: event.clientY, model: node.id });
    },
    [],
  );

  const menuItems = useMemo<MenuItem[]>(() => {
    if (!menu) return [];
    const model = menu.model;
    return [
      { label: "Open file", onClick: () => onOpenFile(model) },
      { label: "Inspect this model", onClick: () => onOpenInspector(model) },
      {
        label: "Refocus on node",
        onClick: () => setFocusFilter(neighborhood(data.edges, model)),
      },
      {
        label: "Show upstream",
        onClick: () =>
          setFocusFilter(new Set([model, ...upstreamOf(data.edges, model)])),
      },
      {
        label: "Show downstream",
        onClick: () =>
          setFocusFilter(new Set([model, ...downstreamOf(data.edges, model)])),
      },
      { label: "Show all", onClick: () => setFocusFilter(null) },
      { label: "AI: Explain", onClick: () => onAi("explain", model) },
      { label: "AI: Generate tests", onClick: () => onAi("test", model) },
      { label: "AI: Draft contract", onClick: () => onAi("contract", model) },
      { label: "AI: Build downstream…", onClick: () => onAi("build", model) },
    ];
  }, [menu, data.edges, onOpenFile, onOpenInspector, onAi]);

  return (
    <div className="relative h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeDoubleClick={onNodeDoubleClick}
        onNodeContextMenu={onNodeContextMenu}
        onPaneClick={() => setMenu(null)}
        fitView
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <MiniMap pannable zoomable />
        <Controls />
      </ReactFlow>
      {focusFilter && (
        <button
          type="button"
          onClick={() => setFocusFilter(null)}
          className="absolute left-3 top-3 z-10 rounded border border-vscode-border px-2 py-1 text-xs text-vscode-button-fg shadow"
          style={{ background: "var(--vscode-button-background)" }}
        >
          Show all {data.nodes.length} models
        </button>
      )}
      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          items={menuItems}
          onClose={() => setMenu(null)}
        />
      )}
    </div>
  );
}
