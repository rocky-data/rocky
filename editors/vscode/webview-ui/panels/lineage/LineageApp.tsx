import { ReactFlowProvider } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import type {
  AiAction,
  AiActionParam,
  DriftData,
  FocusPush,
  GraphData,
  ModelParam,
} from "../../../src/webviews/lineage/contract";
import { getRpc } from "../../runtime/rpcClient";
import { Canvas } from "./Canvas";
import {
  ColorModeContext,
  OverlaysContext,
  type ColorMode,
  type OverlayKind,
} from "./context";
import { costOverlay } from "./overlays/cost";
import { makeDriftOverlay } from "./overlays/drift";
import { freshnessOverlay } from "./overlays/freshness";
import type { LineageOverlay } from "./overlays/types";
import { Toolbar } from "./Toolbar";

export function LineageApp() {
  const [graph, setGraph] = useState<GraphData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [focus, setFocus] = useState<string | null>(null);
  const [colorMode, setColorMode] = useState<ColorMode>("kind");
  const [search, setSearch] = useState("");
  const [active, setActive] = useState<Set<OverlayKind>>(new Set());
  const [drift, setDrift] = useState<DriftData | null>(null);

  useEffect(() => {
    void getRpc()
      .request<GraphData>("graph")
      .then(setGraph)
      .catch((err) => setError(String(err)));
    return getRpc().onPush<FocusPush>("focus", (payload) =>
      setFocus(payload.model),
    );
  }, []);

  const toggleOverlay = (kind: OverlayKind): void => {
    setActive((prev) => {
      const next = new Set(prev);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });
    // Drift needs an on-demand fetch the first time it's enabled.
    if (kind === "drift" && drift === null) {
      void getRpc()
        .request<DriftData>("drift")
        .then(setDrift)
        .catch((err) => setDrift({ actions: [], unavailable: String(err) }));
    }
  };

  const overlays = useMemo<LineageOverlay[]>(() => {
    const list: LineageOverlay[] = [];
    if (active.has("cost")) list.push(costOverlay);
    if (active.has("freshness")) list.push(freshnessOverlay);
    if (active.has("drift") && drift) list.push(makeDriftOverlay(drift));
    return list;
  }, [active, drift]);

  if (error) {
    return (
      <div className="p-4 text-vscode-error">Could not load lineage: {error}</div>
    );
  }
  if (!graph) {
    return <div className="p-4 text-vscode-desc">Loading lineage…</div>;
  }
  if (graph.nodes.length === 0) {
    return (
      <div className="p-4 text-vscode-desc">
        No models in the project catalog yet. Run a build, then reopen lineage.
      </div>
    );
  }

  const openFile = (model: string): void => {
    void getRpc().request("openFile", { model } satisfies ModelParam);
  };
  const openInspector = (model: string): void => {
    void getRpc().request("openInspector", { model } satisfies ModelParam);
  };
  const runAi = (action: AiAction, model: string): void => {
    void getRpc().request("ai", { action, model } satisfies AiActionParam);
  };

  return (
    <ColorModeContext.Provider value={colorMode}>
      <OverlaysContext.Provider value={overlays}>
        <div className="flex h-full flex-col">
          <Toolbar
            colorMode={colorMode}
            onColorMode={setColorMode}
            search={search}
            onSearch={setSearch}
            activeOverlays={active}
            onToggleOverlay={toggleOverlay}
          />
          <div className="min-h-0 flex-1">
            <ReactFlowProvider>
              <Canvas
                data={graph}
                focus={focus}
                search={search}
                onOpenFile={openFile}
                onOpenInspector={openInspector}
                onAi={runAi}
              />
            </ReactFlowProvider>
          </div>
        </div>
      </OverlaysContext.Provider>
    </ColorModeContext.Provider>
  );
}
