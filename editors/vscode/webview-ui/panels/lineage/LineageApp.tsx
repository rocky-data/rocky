import { useEffect, useState } from "react";
import type {
  AiAction,
  AiActionParam,
  FocusPush,
  ModelParam,
} from "../../../src/webviews/lineage/contract";
import { getRpc } from "../../runtime/rpcClient";
import { LineageCanvasView } from "../inspector/LineageCanvasView";
import { useLineageGraph } from "../inspector/useLineageGraph";

/**
 * Standalone lineage view (bottom panel). Thin shell over the shared graph hook
 * and canvas; a node selection opens the separate Inspector. This view is being
 * folded into the Inspector's Lineage tab and will be retired once the merge
 * lands — until then it reuses the same components so behaviour stays in sync.
 */
export function LineageApp() {
  const {
    graph,
    error,
    colorMode,
    setColorMode,
    search,
    setSearch,
    active,
    toggleOverlay,
    overlays,
  } = useLineageGraph();
  const [focus, setFocus] = useState<string | null>(null);

  useEffect(() => {
    return getRpc().onPush<FocusPush>("focus", (payload) => setFocus(payload.model));
  }, []);

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
    <LineageCanvasView
      graph={graph}
      colorMode={colorMode}
      onColorMode={setColorMode}
      search={search}
      onSearch={setSearch}
      activeOverlays={active}
      onToggleOverlay={toggleOverlay}
      overlays={overlays}
      focus={focus}
      onOpenFile={openFile}
      onSelectModel={openInspector}
      onAi={runAi}
    />
  );
}
