import { useEffect, useState } from "react";
import { getRpc } from "../../runtime/rpcClient";
import type {
  InspectorPreviewData,
  InspectorTestsData,
  ModelParam,
  ModelPush,
} from "../../../src/webviews/inspector/contract";
import { ColumnsTab } from "./tabs/ColumnsTab";
import { OverviewTab } from "./tabs/OverviewTab";
import { PreviewTab } from "./tabs/PreviewTab";
import { TestsTab } from "./tabs/TestsTab";

type TabId = "overview" | "columns" | "tests" | "preview";

const TABS: ReadonlyArray<{ id: TabId; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "columns", label: "Columns" },
  { id: "tests", label: "Tests" },
  { id: "preview", label: "Preview" },
];

export function InspectorApp() {
  const [model, setModel] = useState<ModelPush | null>(null);
  const [tab, setTab] = useState<TabId>("overview");
  const [tests, setTests] = useState<InspectorTestsData | null>(null);
  const [preview, setPreview] = useState<InspectorPreviewData | null>(null);

  // The host pushes a fresh model summary on open and on every re-target.
  useEffect(() => {
    return getRpc().onPush<ModelPush>("model", (next) => {
      setModel(next);
      setTests(null);
      setPreview(null);
      setTab("overview");
    });
  }, []);

  const modelName = model?.ok ? model.data.modelName : undefined;

  // Tests execute SQL, so load lazily the first time the tab is opened.
  useEffect(() => {
    if (tab !== "tests" || !modelName || tests) return;
    let active = true;
    void getRpc()
      .request<InspectorTestsData>("tests", { model: modelName } satisfies ModelParam)
      .then((data) => active && setTests(data))
      .catch((err) => active && setTests({ results: [], unavailable: String(err) }));
    return () => {
      active = false;
    };
  }, [tab, modelName, tests]);

  useEffect(() => {
    if (tab !== "preview" || !modelName || preview) return;
    let active = true;
    void getRpc()
      .request<InspectorPreviewData>("preview", { model: modelName } satisfies ModelParam)
      .then((data) => active && setPreview(data))
      .catch((err) => active && setPreview({ unavailable: String(err) }));
    return () => {
      active = false;
    };
  }, [tab, modelName, preview]);

  if (!model) {
    return <div className="p-4 text-vscode-desc">Loading model…</div>;
  }
  if (!model.ok) {
    return (
      <div className="p-4 text-vscode-error">
        <p className="font-semibold">Could not inspect this model.</p>
        <p className="mt-1 text-vscode-desc">{model.error}</p>
      </div>
    );
  }

  const data = model.data;
  return (
    <div className="flex h-full flex-col">
      <header className="flex items-baseline gap-2 border-b border-vscode-border px-4 py-3">
        <span className="text-[11px] uppercase tracking-wide text-vscode-desc">
          {data.kind}
        </span>
        <h1 className="text-base font-semibold text-vscode-fg">{data.modelName}</h1>
        <span className="truncate text-xs text-vscode-desc">{data.fqn}</span>
      </header>
      <nav className="flex gap-1 border-b border-vscode-border px-2">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            onClick={() => setTab(t.id)}
            className={
              "border-b-2 px-3 py-2 text-sm " +
              (tab === t.id
                ? "border-vscode-focus text-vscode-fg"
                : "border-transparent text-vscode-desc hover:text-vscode-fg")
            }
          >
            {t.label}
          </button>
        ))}
      </nav>
      <div className="min-h-0 flex-1 overflow-auto p-4">
        {tab === "overview" && <OverviewTab data={data} />}
        {tab === "columns" && <ColumnsTab data={data} tests={tests} />}
        {tab === "tests" && <TestsTab tests={tests} />}
        {tab === "preview" && <PreviewTab preview={preview} />}
      </div>
    </div>
  );
}
