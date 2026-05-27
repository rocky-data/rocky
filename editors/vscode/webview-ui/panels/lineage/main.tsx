import "../../styles/tailwind.generated.css";
import "@xyflow/react/dist/style.css";
import { lazy } from "react";
import { createRoot } from "react-dom/client";
import { AppShell } from "../../runtime/AppShell";

const LineageApp = lazy(() =>
  import("./LineageApp").then((m) => ({ default: m.LineageApp })),
);

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <AppShell>
      <LineageApp />
    </AppShell>,
  );
}
