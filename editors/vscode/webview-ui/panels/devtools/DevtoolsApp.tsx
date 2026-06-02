import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import { useState } from "react";
import { getRpc } from "../../runtime/rpcClient";
import { useTheme } from "../../runtime/ThemeProvider";

const NODES: Node[] = [
  { id: "raw", position: { x: 0, y: 40 }, data: { label: "raw_orders" } },
  { id: "stg", position: { x: 220, y: 40 }, data: { label: "stg_orders" } },
];
const EDGES: Edge[] = [{ id: "raw-stg", source: "raw", target: "stg" }];

/**
 * Foundation smoke test, intentionally trivial. Proves, in isolation, the four
 * things most likely to break: Tailwind utilities mapped to `--vscode-*`
 * theme variables, the theme context, a host RPC round-trip, and a code-split
 * ReactFlow canvas rendering under the webview CSP.
 */
export function DevtoolsApp() {
  const theme = useTheme();
  const [echo, setEcho] = useState("(not called yet)");

  async function ping(): Promise<void> {
    const reply = await getRpc().request<string>("echo", "hello from webview");
    setEcho(reply);
  }

  return (
    <div className="flex h-full flex-col gap-3 p-4">
      <header className="flex items-center gap-3">
        <h1 className="text-base font-semibold text-vscode-fg">
          Rocky webview devtools
        </h1>
        <span className="rounded-sm bg-vscode-badge-bg px-2 py-0.5 text-xs text-vscode-badge-fg">
          theme: {theme}
        </span>
      </header>
      <p className="text-sm text-vscode-desc">
        Smoke test for the React + ReactFlow webview foundation. If you can read
        this in your editor colours, click the button and get a reply, and see
        the graph below, the toolchain is sound.
      </p>
      <div className="flex items-center gap-3">
        <button
          type="button"
          onClick={() => void ping()}
          className="rounded-sm bg-vscode-button-bg px-3 py-1 text-sm text-vscode-button-fg hover:bg-vscode-button-hover"
        >
          Ping host (echo RPC)
        </button>
        <span className="text-sm text-vscode-fg">→ {echo}</span>
      </div>
      <div className="min-h-0 flex-1 rounded-sm border border-vscode-border">
        <ReactFlow nodes={NODES} edges={EDGES} fitView>
          <Background />
          <MiniMap />
          <Controls />
        </ReactFlow>
      </div>
    </div>
  );
}
