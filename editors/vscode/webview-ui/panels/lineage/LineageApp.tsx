import { ReactFlowProvider } from "@xyflow/react";
import { useEffect, useState } from "react";
import type {
  FocusPush,
  GraphData,
  ModelParam,
} from "../../../src/webviews/lineage/contract";
import { getRpc } from "../../runtime/rpcClient";
import { Canvas } from "./Canvas";
import { ColorModeContext, type ColorMode } from "./context";
import { Toolbar } from "./Toolbar";

export function LineageApp() {
  const [graph, setGraph] = useState<GraphData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [focus, setFocus] = useState<string | null>(null);
  const [colorMode, setColorMode] = useState<ColorMode>("kind");
  const [search, setSearch] = useState("");

  useEffect(() => {
    void getRpc()
      .request<GraphData>("graph")
      .then(setGraph)
      .catch((err) => setError(String(err)));
    return getRpc().onPush<FocusPush>("focus", (payload) =>
      setFocus(payload.model),
    );
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

  return (
    <ColorModeContext.Provider value={colorMode}>
      <div className="flex h-full flex-col">
        <Toolbar
          colorMode={colorMode}
          onColorMode={setColorMode}
          search={search}
          onSearch={setSearch}
        />
        <div className="min-h-0 flex-1">
          <ReactFlowProvider>
            <Canvas
              data={graph}
              focus={focus}
              search={search}
              onOpenFile={openFile}
              onOpenInspector={openInspector}
            />
          </ReactFlowProvider>
        </div>
      </div>
    </ColorModeContext.Provider>
  );
}
