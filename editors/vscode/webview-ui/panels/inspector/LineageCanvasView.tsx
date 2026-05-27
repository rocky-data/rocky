import { ReactFlowProvider } from "@xyflow/react";
import type { AiAction, GraphData } from "../../../src/webviews/lineage/contract";
import { Canvas } from "./Canvas";
import {
  ColorModeContext,
  OverlaysContext,
  type ColorMode,
  type OverlayKind,
} from "./context";
import type { LineageOverlay } from "./overlays/types";
import { Toolbar } from "./Toolbar";

/**
 * Presentational lineage canvas: the Toolbar plus the ReactFlow {@link Canvas},
 * wrapped in the color-mode and overlays contexts. All graph data, overlay
 * selection and handlers are owned by the parent (the Inspector, or — until it
 * is retired — the standalone lineage view), so the canvas can be embedded as a
 * tab and a node click can re-target the surrounding model detail.
 */
export interface LineageCanvasViewProps {
  graph: GraphData;
  colorMode: ColorMode;
  onColorMode: (mode: ColorMode) => void;
  search: string;
  onSearch: (query: string) => void;
  activeOverlays: Set<OverlayKind>;
  onToggleOverlay: (kind: OverlayKind) => void;
  overlays: LineageOverlay[];
  /** Model the canvas frames and selects (the Inspector's focused model). */
  focus: string | null;
  /** Whether the canvas tab is currently shown; a hidden→shown flip re-fits. */
  active?: boolean;
  onOpenFile: (model: string) => void;
  /** Selecting a node re-targets the surrounding detail (no cross-webview hop). */
  onSelectModel: (model: string) => void;
  onAi: (action: AiAction, model: string) => void;
}

export function LineageCanvasView({
  graph,
  colorMode,
  onColorMode,
  search,
  onSearch,
  activeOverlays,
  onToggleOverlay,
  overlays,
  focus,
  active,
  onOpenFile,
  onSelectModel,
  onAi,
}: LineageCanvasViewProps) {
  return (
    <ColorModeContext.Provider value={colorMode}>
      <OverlaysContext.Provider value={overlays}>
        <div className="flex h-full flex-col">
          <Toolbar
            colorMode={colorMode}
            onColorMode={onColorMode}
            search={search}
            onSearch={onSearch}
            activeOverlays={activeOverlays}
            onToggleOverlay={onToggleOverlay}
          />
          <div className="min-h-0 flex-1">
            <ReactFlowProvider>
              <Canvas
                data={graph}
                focus={focus}
                search={search}
                active={active}
                onOpenFile={onOpenFile}
                onOpenInspector={onSelectModel}
                onAi={onAi}
              />
            </ReactFlowProvider>
          </div>
        </div>
      </OverlaysContext.Provider>
    </ColorModeContext.Provider>
  );
}
