import "../../styles/tailwind.generated.css";
import "@xyflow/react/dist/style.css";
import { lazy } from "react";
import { createRoot } from "react-dom/client";
import { AppShell } from "../../runtime/AppShell";

// Lazy-loaded so the panel and its ReactFlow dependency land in a code-split
// chunk, exercising the chunk-loading path the <base href> resolves against.
// AppShell supplies the Suspense boundary the lazy import needs.
const DevtoolsApp = lazy(() =>
  import("./DevtoolsApp").then((m) => ({ default: m.DevtoolsApp })),
);

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <AppShell>
      <DevtoolsApp />
    </AppShell>,
  );
}
