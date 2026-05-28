import "../../styles/tailwind.generated.css";
// ReactFlow's base stylesheet for the Lineage tab's canvas (node/edge layout,
// MiniMap, Controls). base.css themes it via the --xy-* vars; without this the
// MiniMap renders as an unstyled black box mis-placed at the top-left (regressed
// when the canvas moved into this panel in the lineage/inspector consolidation).
import "@xyflow/react/dist/style.css";
// Register the TailwindPlus Elements custom elements eagerly (at entry-eval,
// before React mounts) so the command palette's <el-dialog> is defined when the
// Inspector reffs it. The /react wrappers would otherwise lazy-import this.
import "@tailwindplus/elements";
import { lazy } from "react";
import { createRoot } from "react-dom/client";
import { AppShell } from "../../runtime/AppShell";

// Lazy so the panel body lands in a code-split chunk. The Inspector pulls in
// ReactFlow for the Lineage tab's canvas.
const InspectorApp = lazy(() =>
  import("./InspectorApp").then((m) => ({ default: m.InspectorApp })),
);

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <AppShell>
      <InspectorApp />
    </AppShell>,
  );
}
