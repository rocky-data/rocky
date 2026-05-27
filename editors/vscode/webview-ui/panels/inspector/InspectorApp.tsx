import { useEffect, useState } from "react";
import type {
  AiAction,
  AiActionParam,
} from "../../../src/webviews/lineage/contract";
import type {
  InspectorModelData,
  InspectorPreviewData,
  InspectorTarget,
  InspectorTestsData,
  ModelParam,
  ModelPush,
} from "../../../src/webviews/inspector/contract";
import type { ProfileOutput } from "../../../src/types/generated/profile";
import { getRpc } from "../../runtime/rpcClient";
import { LineageCanvasView } from "./LineageCanvasView";
import { ColumnsTab } from "./tabs/ColumnsTab";
import { LineageTab } from "./tabs/LineageTab";
import { OverviewTab } from "./tabs/OverviewTab";
import { PreviewTab } from "./tabs/PreviewTab";
import { ProfileTab } from "./tabs/ProfileTab";
import { TestsTab } from "./tabs/TestsTab";
import { useLineageGraph } from "./useLineageGraph";

type TabId = "overview" | "columns" | "lineage" | "tests" | "preview" | "profile";

const TABS: ReadonlyArray<{ id: TabId; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "columns", label: "Columns" },
  { id: "lineage", label: "Lineage" },
  { id: "tests", label: "Tests" },
  { id: "preview", label: "Preview" },
  { id: "profile", label: "Profile" },
];

/** Pill toggle (graph ⇄ columns) inside the Lineage tab. */
function subTab(on: boolean): string {
  return (
    "rounded px-2 py-0.5 " +
    (on
      ? "bg-vscode-button-bg text-vscode-button-fg"
      : "text-vscode-desc hover:text-vscode-fg")
  );
}

/**
 * The Rocky Inspector: one surface for a model's detail (Overview / Columns /
 * Tests / Preview / Profile) and the project lineage canvas (the Lineage tab).
 * Clicking a node in the canvas re-targets every detail tab to that model.
 */
export function InspectorApp() {
  const lineage = useLineageGraph();
  const [focusedModel, setFocusedModel] = useState<string | null>(null);
  const [model, setModel] = useState<ModelPush | null>(null);
  const [tab, setTab] = useState<TabId>("overview");
  const [lineageView, setLineageView] = useState<"graph" | "columns">("graph");
  // Defer mounting the canvas until its tab is first shown: ReactFlow measures
  // its container on mount, and a hidden (display:none) container is 0×0, which
  // leaves it stuck at a bad zero-size fit. Once mounted at a real size it stays
  // mounted, so later tab switches just toggle visibility and keep the viewport.
  const [seenLineage, setSeenLineage] = useState(false);
  const [tests, setTests] = useState<InspectorTestsData | null>(null);
  const [preview, setPreview] = useState<InspectorPreviewData | null>(null);
  const [profile, setProfile] = useState<ProfileOutput | null>(null);

  // The host pushes the initial target (which model + tab) when the view opens;
  // re-targets after that are webview-driven via focusedModel below.
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

  useEffect(() => {
    if (tab === "lineage") setSeenLineage(true);
  }, [tab]);

  const data = model?.ok ? model.data : null;
  const showCanvas = tab === "lineage" || seenLineage;

  const openFile = (m: string): void => {
    void getRpc().request("openFile", { model: m } satisfies ModelParam);
  };
  const runAi = (action: AiAction, m: string): void => {
    void getRpc().request("ai", { action, model: m } satisfies AiActionParam);
  };

  return (
    <div className="flex h-full flex-col">
      <header className="flex items-baseline gap-2 border-b border-vscode-border px-4 py-3">
        {data ? (
          <>
            <span className="text-[11px] uppercase tracking-wide text-vscode-desc">
              {data.kind}
            </span>
            <h1 className="text-base font-semibold text-vscode-fg">
              {data.modelName}
            </h1>
            <span className="truncate text-xs text-vscode-desc">{data.fqn}</span>
          </>
        ) : (
          <h1 className="text-base font-semibold text-vscode-fg">
            {focusedModel ?? "Rocky Inspector"}
          </h1>
        )}
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

      <div className="min-h-0 flex-1 overflow-hidden">
        {/*
          Lineage tab: the project canvas, kept mounted (hidden off-tab) so it
          never re-runs its layout when the user tabs away and back. A graph ⇄
          columns toggle swaps to the focused model's per-hop column lineage.
        */}
        <div
          className="flex h-full flex-col"
          style={{ display: tab === "lineage" ? undefined : "none" }}
        >
          <div className="flex gap-1 border-b border-vscode-border px-3 py-1 text-xs">
            <button
              type="button"
              onClick={() => setLineageView("graph")}
              className={subTab(lineageView === "graph")}
            >
              Graph
            </button>
            <button
              type="button"
              onClick={() => setLineageView("columns")}
              className={subTab(lineageView === "columns")}
            >
              Columns
            </button>
          </div>
          <div
            className="min-h-0 flex-1"
            style={{ display: lineageView === "graph" ? undefined : "none" }}
          >
            {!showCanvas ? null : lineage.error ? (
              <div className="p-4 text-vscode-error">
                Could not load lineage: {lineage.error}
              </div>
            ) : !lineage.graph ? (
              <div className="p-4 text-vscode-desc">Loading lineage…</div>
            ) : lineage.graph.nodes.length === 0 ? (
              <div className="p-4 text-vscode-desc">
                No models in the project catalog yet. Run a build, then reopen.
              </div>
            ) : (
              <LineageCanvasView
                graph={lineage.graph}
                colorMode={lineage.colorMode}
                onColorMode={lineage.setColorMode}
                search={lineage.search}
                onSearch={lineage.setSearch}
                activeOverlays={lineage.active}
                onToggleOverlay={lineage.toggleOverlay}
                overlays={lineage.overlays}
                focus={focusedModel}
                active={tab === "lineage" && lineageView === "graph"}
                onOpenFile={openFile}
                onSelectModel={setFocusedModel}
                onAi={runAi}
              />
            )}
          </div>
          {lineageView === "columns" && (
            <div className="min-h-0 flex-1 overflow-auto p-4">
              {data ? (
                <LineageTab data={data} />
              ) : (
                <div className="text-vscode-desc">
                  Select a model in the graph to see its column lineage.
                </div>
              )}
            </div>
          )}
        </div>

        {/* Detail tabs — these need the focused model's loaded detail. */}
        {tab !== "lineage" && (
          <div className="h-full overflow-auto p-4">
            {!model ? (
              <div className="text-vscode-desc">Loading model…</div>
            ) : !model.ok ? (
              <div className="text-vscode-error">
                <p className="font-semibold">Could not inspect this model.</p>
                <p className="mt-1 text-vscode-desc">{model.error}</p>
              </div>
            ) : tab === "overview" ? (
              <OverviewTab data={model.data} />
            ) : tab === "columns" ? (
              <ColumnsTab data={model.data} tests={tests} />
            ) : tab === "tests" ? (
              <TestsTab tests={tests} />
            ) : tab === "preview" ? (
              <PreviewTab preview={preview} />
            ) : tab === "profile" ? (
              <ProfileTab profile={profile} />
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
