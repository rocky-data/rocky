import "../../styles/tailwind.generated.css";
import { lazy } from "react";
import { createRoot } from "react-dom/client";
import { AppShell } from "../../runtime/AppShell";

// Lazy so the panel body lands in a code-split chunk. The Inspector has no
// ReactFlow dependency, so it shares the React/runtime chunk with the devtools
// panel while pulling none of ReactFlow's weight.
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
