import { useEffect, useState } from "react";
import { getRpc } from "../../runtime/rpcClient";
import type {
  InspectorModelData,
  InspectorPreviewData,
  InspectorTarget,
  InspectorTestsData,
  ModelParam,
  ModelPush,
} from "../../../src/webviews/inspector/contract";
import type { ProfileOutput } from "../../../src/types/generated/profile";
import { ColumnsTab } from "./tabs/ColumnsTab";
import { LineageTab } from "./tabs/LineageTab";
import { OverviewTab } from "./tabs/OverviewTab";
import { PreviewTab } from "./tabs/PreviewTab";
import { ProfileTab } from "./tabs/ProfileTab";
import { TestsTab } from "./tabs/TestsTab";

type TabId = "overview" | "columns" | "lineage" | "tests" | "preview" | "profile";

const TABS: ReadonlyArray<{ id: TabId; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "columns", label: "Columns" },
  { id: "lineage", label: "Lineage" },
  { id: "tests", label: "Tests" },
  { id: "preview", label: "Preview" },
  { id: "profile", label: "Profile" },
];

export function InspectorApp() {
  const [focusedModel, setFocusedModel] = useState<string | null>(null);
  const [model, setModel] = useState<ModelPush | null>(null);
  const [tab, setTab] = useState<TabId>("overview");
  const [tests, setTests] = useState<InspectorTestsData | null>(null);
  const [preview, setPreview] = useState<InspectorPreviewData | null>(null);
  const [profile, setProfile] = useState<ProfileOutput | null>(null);

  // The host pushes the initial target (which model + tab) when the view opens.
  // Re-targets after that are webview-driven via focusedModel below.
  useEffect(() => {
    return getRpc().onPush<InspectorTarget>("target", (t) => {
      setFocusedModel(t.model);
      if (t.tab) setTab(t.tab as TabId);
    });
  }, []);

  // Load (or reload) the focused model's detail by request, so a re-target —
  // including a click on a node in the Lineage tab's canvas — is race-free; a
  // stale response from a superseded model is ignored.
  useEffect(() => {
    if (!focusedModel) return;
    let ignore = false;
    setModel(null);
    setTests(null);
    setPreview(null);
    setProfile(null);
    void getRpc()
      .request<InspectorModelData>("model", { model: focusedModel } satisfies ModelParam)
      .then((data) => !ignore && setModel({ ok: true, data }))
      .catch((err) => !ignore && setModel({ ok: false, error: String(err) }));
    return () => {
      ignore = true;
    };
  }, [focusedModel]);

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

  // Profiling runs DuckDB aggregates, so load it lazily too.
  useEffect(() => {
    if (tab !== "profile" || !modelName || profile) return;
    let active = true;
    void getRpc()
      .request<ProfileOutput>("profile", { model: modelName } satisfies ModelParam)
      .then((data) => active && setProfile(data))
      .catch(
        (err) =>
          active &&
          setProfile({
            version: "",
            command: "profile",
            model: modelName,
            columns: [],
            unavailable: String(err),
          }),
      );
    return () => {
      active = false;
    };
  }, [tab, modelName, profile]);

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
        {tab === "lineage" && <LineageTab data={data} />}
        {tab === "tests" && <TestsTab tests={tests} />}
        {tab === "preview" && <PreviewTab preview={preview} />}
        {tab === "profile" && <ProfileTab profile={profile} />}
      </div>
    </div>
  );
}
