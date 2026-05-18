import * as path from "path";
import * as vscode from "vscode";
import { resolveProjectRoot } from "../config";
import { getExtensionUri } from "../extensionState";
import { runRocky } from "../rockyCli";
import type { LineageOutput } from "../types/generated/lineage";
import type { ModelHistoryOutput } from "../types/generated/model_history";
import { resolveModelName } from "./ui";

const VIEW_TYPE = "rockyLineage";

interface SerializedState {
  modelName?: string;
  scale?: number;
  panX?: number;
  panY?: number;
  viewMode?: "model" | "column";
  clusterMode?: "none" | "schema" | "source";
  layout?: "LR" | "TB";
  searchQuery?: string;
  focusMode?: "all" | "upstream" | "downstream" | "selected";
  focusNode?: string | null;
}

/** Message types sent from the extension host to the webview. */
type HostToWebviewMessage =
  | { type: "modelDetails"; model: string; history: ModelHistoryOutput | null; errorMsg?: string }
  | { type: "modelDetailsError"; model: string; errorMsg: string };

export async function showLineage(arg?: unknown): Promise<void> {
  // When invoked from the tree-view context menu, `arg` is a ModelTreeItem.
  // When invoked from the command palette, fall back to the active editor.
  let modelName = resolveModelName(arg);

  if (!modelName) {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
      vscode.window.showWarningMessage("Open a Rocky model file first.");
      return;
    }
    modelName = path
      .basename(editor.document.fileName)
      .replace(/\.(rocky|sql)$/, "");
  }
  if (!modelName) return;

  const extensionUri = getExtensionUri();
  const mediaUri = vscode.Uri.joinPath(extensionUri, "media");

  const panel = vscode.window.createWebviewPanel(
    VIEW_TYPE,
    `Lineage: ${modelName}`,
    vscode.ViewColumn.Beside,
    {
      enableScripts: true,
      localResourceRoots: [mediaUri],
      retainContextWhenHidden: true,
    },
  );

  await populatePanel(panel, modelName);
}

/**
 * Registers a webview-panel serializer so lineage panels survive workspace
 * reloads. The webview persists `{ modelName, scale, panX, panY, viewMode }`
 * via `vscode.setState`; on reload we re-run the CLI and restore zoom/pan.
 */
export function registerLineageSerializer(
  context: vscode.ExtensionContext,
): void {
  context.subscriptions.push(
    vscode.window.registerWebviewPanelSerializer(VIEW_TYPE, {
      async deserializeWebviewPanel(
        panel: vscode.WebviewPanel,
        state: SerializedState | undefined,
      ) {
        const modelName = state?.modelName;
        if (!modelName) {
          panel.dispose();
          return;
        }
        const extensionUri = getExtensionUri();
        const mediaUri = vscode.Uri.joinPath(extensionUri, "media");
        panel.webview.options = {
          enableScripts: true,
          localResourceRoots: [mediaUri],
        };
        await populatePanel(panel, modelName);
      },
    }),
  );
}

async function populatePanel(
  panel: vscode.WebviewPanel,
  modelName: string,
): Promise<void> {
  // Resolve the project root by walking up from the active editor's file,
  // so users can open a SQL file outside the workspace root (or in a sibling
  // folder) and lineage still finds the right rocky.toml.
  const projectRoot = resolveProjectRoot();
  const extensionUri = getExtensionUri();
  const mediaUri = vscode.Uri.joinPath(extensionUri, "media");

  panel.webview.html = renderLoadingHtml(panel.webview, modelName);

  // Handle messages from the webview.
  panel.webview.onDidReceiveMessage(
    async (msg: { type?: string; name?: string }) => {
      if (!msg?.type) return;

      // Open a model file in the editor.
      if (msg.type === "openModel" && typeof msg.name === "string") {
        const matches = await vscode.workspace.findFiles(
          `**/models/**/${msg.name}.{rocky,sql}`,
          undefined,
          1,
        );
        if (matches[0]) {
          void vscode.commands.executeCommand("vscode.open", matches[0]);
        }
        return;
      }

      // Fetch model history for the side panel.
      if (msg.type === "loadModelDetails" && typeof msg.name === "string") {
        const clickedModel = msg.name;
        let history: ModelHistoryOutput | null = null;
        let errorMsg: string | undefined;

        try {
          const args = ["history", "--model", clickedModel, "--output", "json"];
          const { stdout } = await runRocky(args, { cwd: projectRoot });
          history = JSON.parse(stdout) as ModelHistoryOutput;
        } catch (err) {
          errorMsg = (err as Error).message;
        }

        const reply: HostToWebviewMessage = {
          type: "modelDetails",
          model: clickedModel,
          history,
          errorMsg,
        };
        void panel.webview.postMessage(reply);
      }
    },
    undefined,
    [],
  );

  // Use the default JSON output (no -o table / --format dot).
  // The engine emits LineageOutput JSON with upstream/downstream/edges.
  // Rocky discovers rocky.toml from cwd, so we don't pass --config.
  const args = ["lineage", modelName];

  try {
    const { stdout } = await runRocky(args, { cwd: projectRoot });
    const lineageData = JSON.parse(stdout) as LineageOutput;
    panel.webview.html = renderLineageHtml(
      panel.webview,
      mediaUri,
      modelName,
      lineageData,
    );
  } catch (err) {
    panel.webview.html = renderErrorHtml(
      panel.webview,
      modelName,
      (err as Error).message,
    );
  }
}

function renderLoadingHtml(
  webview: vscode.Webview,
  modelName: string,
): string {
  const nonce = makeNonce();
  return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="Content-Security-Policy"
        content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline';" />
  <title>Lineage: ${escapeHtml(modelName)}</title>
  <style nonce="${nonce}">
    body {
      display: flex; align-items: center; justify-content: center;
      height: 100vh; margin: 0;
      font-family: var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
    }
  </style>
</head>
<body>
  <p>Loading lineage for <strong>${escapeHtml(modelName)}</strong>…</p>
</body>
</html>`;
}

function renderErrorHtml(
  webview: vscode.Webview,
  modelName: string,
  error: string,
): string {
  const nonce = makeNonce();
  return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="Content-Security-Policy"
        content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline';" />
  <title>Lineage: ${escapeHtml(modelName)}</title>
  <style nonce="${nonce}">
    body {
      padding: 20px; margin: 0;
      font-family: var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
    }
    .error { color: var(--vscode-errorForeground); }
  </style>
</head>
<body>
  <h2>Lineage: ${escapeHtml(modelName)}</h2>
  <p class="error">Failed to generate lineage: ${escapeHtml(error)}</p>
  <p>Make sure the Rocky binary is installed and the project compiles successfully.</p>
</body>
</html>`;
}

function renderLineageHtml(
  webview: vscode.Webview,
  mediaUri: vscode.Uri,
  modelName: string,
  data: LineageOutput,
): string {
  const graphUri = webview.asWebviewUri(
    vscode.Uri.joinPath(mediaUri, "lineage-graph.js"),
  );
  const nonce = makeNonce();
  // Embed the LineageOutput as JSON in a <script type="application/json"> tag
  // so the webview script can read it without string-interpolation issues.
  const dataJson = JSON.stringify(data);

  return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="Content-Security-Policy"
        content="default-src 'none';
                 style-src ${webview.cspSource} 'unsafe-inline';
                 script-src ${webview.cspSource} 'nonce-${nonce}';
                 img-src ${webview.cspSource} data:;" />
  <title>Lineage: ${escapeHtml(modelName)}</title>
  <style nonce="${nonce}">
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--vscode-font-family);
      font-size: var(--vscode-font-size);
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
      display: flex; flex-direction: column;
      height: 100vh; overflow: hidden;
    }
    /* Vertical controls rail (right-docked) */
    #left-rail {
      width: 240px;
      min-width: 240px;
      border-left: 1px solid var(--vscode-panel-border);
      background: var(--vscode-sideBar-background, var(--vscode-editor-background));
      display: flex; flex-direction: column;
      gap: 6px;
      padding: 12px 10px;
      overflow-y: auto;
      overflow-x: hidden;
      flex-shrink: 0;
      transition: width 0.15s ease, min-width 0.15s ease, padding 0.15s ease;
    }
    #left-rail.collapsed {
      width: 28px;
      min-width: 28px;
      padding: 8px 4px;
      gap: 0;
    }
    #left-rail.collapsed > :not(.rail-header) { display: none; }
    .rail-header {
      display: flex; align-items: center; justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    #left-rail.collapsed .rail-header { justify-content: center; margin-bottom: 0; }
    #left-rail.collapsed .rail-title { display: none; }
    #rail-toggle {
      background: transparent;
      color: var(--vscode-descriptionForeground);
      border: 1px solid transparent;
      padding: 2px 4px;
      font-size: 14px;
      line-height: 1;
      cursor: pointer;
      border-radius: 2px;
      flex-shrink: 0;
    }
    #rail-toggle:hover {
      background: var(--vscode-toolbar-hoverBackground, var(--vscode-button-secondaryHoverBackground));
      color: var(--vscode-foreground);
    }
    /* Collapsible section accordion */
    .rail-section {
      display: flex; flex-direction: column;
      border-top: 1px solid var(--vscode-panel-border);
      padding-top: 6px;
    }
    .rail-section:first-of-type { border-top: none; padding-top: 0; }
    .rail-section-toggle {
      display: flex; align-items: center; justify-content: space-between;
      width: 100%;
      background: transparent;
      border: none;
      padding: 4px 0;
      cursor: pointer;
      color: var(--vscode-descriptionForeground);
      font: inherit;
      text-align: left;
    }
    .rail-section-toggle:hover { color: var(--vscode-foreground); }
    .rail-section-toggle:focus { outline: none; }
    .rail-section-label {
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .rail-section-chevron {
      font-size: 10px;
      line-height: 1;
      transition: transform 0.15s ease;
    }
    .rail-section.collapsed .rail-section-chevron { transform: rotate(-90deg); }
    .rail-section-body {
      display: flex; flex-direction: column;
      gap: 4px;
      padding: 4px 0 6px;
    }
    .rail-section.collapsed .rail-section-body { display: none; }
    /* Sub-grouping inside a section (e.g. View / Focus / etc. live under Controls) */
    .rail-subgroup { display: flex; flex-direction: column; gap: 3px; }
    .rail-subgroup + .rail-subgroup { margin-top: 6px; }
    .rail-subgroup-label {
      font-size: 10px;
      color: var(--vscode-descriptionForeground);
    }
    #left-rail .rail-title {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--vscode-descriptionForeground);
      margin: 0;
    }
    #left-rail .rail-model {
      font-size: 13px;
      font-weight: 600;
      color: var(--vscode-foreground);
      word-break: break-word;
      margin: -8px 0 4px;
    }
    #left-rail .btn-group { display: flex; gap: 0; }
    #left-rail .btn-group button { flex: 1; }
    #left-rail .btn-group button:not(:first-child) { border-left-width: 0; }
    #left-rail .btn-group button:first-child { border-radius: 2px 0 0 2px; }
    #left-rail .btn-group button:last-child  { border-radius: 0 2px 2px 0; }
    #left-rail .btn-group button:not(:first-child):not(:last-child) { border-radius: 0; }
    #left-rail button {
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border, transparent);
      padding: 4px 10px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    #left-rail button:hover { background: var(--vscode-button-secondaryHoverBackground); }
    #left-rail button.active {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
    }
    #left-rail select,
    #left-rail input[type="text"] {
      width: 100%;
      background: var(--vscode-input-background, var(--vscode-button-secondaryBackground));
      color: var(--vscode-input-foreground, var(--vscode-button-secondaryForeground));
      border: 1px solid var(--vscode-input-border, var(--vscode-panel-border));
      padding: 4px 7px;
      font-size: 12px;
      border-radius: 2px;
      outline: none;
    }
    #left-rail input[type="text"]:focus,
    #left-rail select:focus { border-color: var(--vscode-focusBorder, #007fd4); }
    #left-rail .zoom-row { display: flex; gap: 4px; }
    #left-rail .zoom-row button { flex: 1; padding: 4px 0; }
    #left-rail .export-btn {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
    }
    #left-rail .export-btn:hover { background: var(--vscode-button-hoverBackground); }
    /* Main content area: graph + side panel */
    #main-content {
      flex: 1; display: flex; overflow: hidden;
    }
    #viewport {
      flex: 1; overflow: hidden; position: relative;
      cursor: grab;
    }
    #viewport.panning { cursor: grabbing; }
    #graph-container {
      width: 100%; height: 100%;
    }
    #graph-container svg {
      display: block; width: 100%; height: 100%;
    }
    /* Node details (now inside the left rail) */
    #side-panel-content {
      display: flex; flex-direction: column;
      gap: 8px;
    }
    .panel-empty {
      color: var(--vscode-descriptionForeground);
      font-size: 12px;
      margin: 0;
    }
    .panel-loading {
      color: var(--vscode-descriptionForeground);
      font-size: 12px;
      margin: 0;
    }
    .panel-section {
      margin: 0;
    }
    .panel-section-title {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--vscode-descriptionForeground);
      margin-bottom: 5px;
    }
    .panel-row {
      display: flex; justify-content: space-between; align-items: baseline;
      font-size: 12px;
      margin-bottom: 3px;
    }
    .panel-row .label { color: var(--vscode-descriptionForeground); }
    .panel-row .value { font-weight: 500; word-break: break-all; }
    .status-ok    { color: var(--vscode-charts-green, #89d185); }
    .status-fail  { color: var(--vscode-errorForeground); }
    .status-other { color: var(--vscode-charts-yellow, #cca700); }
    .column-list {
      font-size: 11px;
      list-style: none;
      margin: 0; padding: 0;
    }
    .column-list li {
      padding: 2px 0;
      border-bottom: 1px solid var(--vscode-panel-border);
      display: flex; justify-content: space-between;
    }
    .column-list li:last-child { border-bottom: none; }
    .col-name { font-family: var(--vscode-editor-font-family, monospace); }
    .col-type { color: var(--vscode-descriptionForeground); }
    .panel-error { color: var(--vscode-errorForeground); font-size: 12px; }
    .open-btn {
      width: 100%; margin-top: 8px;
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border: none;
      padding: 5px 10px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    .open-btn:hover { background: var(--vscode-button-hoverBackground); }
    /* Node styling — use VS Code token colours */
    .node rect {
      fill: var(--vscode-badge-background);
      stroke: var(--vscode-foreground);
      stroke-width: 1.5px;
    }
    .node.focal rect {
      fill: var(--vscode-button-background);
      stroke: var(--vscode-button-foreground);
      stroke-width: 2px;
    }
    .node.source rect {
      fill: var(--vscode-charts-blue, var(--vscode-badge-background));
    }
    .node.leaf rect {
      fill: var(--vscode-charts-green, var(--vscode-badge-background));
    }
    .node.selected rect {
      stroke: var(--vscode-focusBorder, #007fd4);
      stroke-width: 3px;
    }
    .node { cursor: pointer; }
    .node:hover rect {
      stroke-width: 2.5px;
    }
    .node text {
      fill: var(--vscode-foreground);
      font-size: 11px;
      pointer-events: none;
    }
    .node.focal text {
      fill: var(--vscode-button-foreground);
      font-weight: 600;
    }
    /* Edge path + arrow */
    .edgePath path {
      stroke: var(--vscode-descriptionForeground);
      stroke-width: 1.5px;
      fill: none;
      opacity: 0.6;
    }
    .edgeLabel text {
      fill: var(--vscode-descriptionForeground);
      font-size: 10px;
    }
    /* Arrow marker inherits from path */
    marker path {
      fill: var(--vscode-descriptionForeground);
      stroke: none;
    }
    .status {
      padding: 6px 12px;
      color: var(--vscode-descriptionForeground);
      font-size: 11px;
      border-top: 1px solid var(--vscode-panel-border);
      flex-shrink: 0;
    }
    .error { color: var(--vscode-errorForeground); }
    /* Cluster subgraph styles */
    .cluster-bbox {
      fill: transparent;
      stroke: var(--vscode-tab-border, var(--vscode-panel-border));
      stroke-width: 1px;
      stroke-dasharray: 4 3;
      rx: 6;
      ry: 6;
    }
    .cluster-label {
      fill: var(--vscode-descriptionForeground);
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      pointer-events: none;
    }
    .cluster-header-hit {
      fill: transparent;
      cursor: pointer;
    }
    .cluster-header-hit:hover + .cluster-label,
    .cluster-header-hit:hover ~ .cluster-label {
      fill: var(--vscode-foreground);
    }
    /* Dimming: applied to graphGroup when a cluster is focused */
    .dim-mode .node:not(.in-focus) rect,
    .dim-mode .edgePath:not(.in-focus) path {
      opacity: 0.2;
    }
    .dim-mode .node:not(.in-focus) text {
      opacity: 0.2;
    }
    .dim-mode .cluster-bbox:not(.in-focus) {
      opacity: 0.2;
    }
    .dim-mode .cluster-label:not(.in-focus) {
      opacity: 0.2;
    }
    /* Cluster select */
    .cluster-select-label {
      font-size: 12px;
      color: var(--vscode-foreground);
    }
    select.cluster-select {
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border, transparent);
      padding: 3px 6px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    select.cluster-select:hover {
      background: var(--vscode-button-secondaryHoverBackground);
    }
    /* Layout select */
    select.layout-select {
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border, transparent);
      padding: 3px 6px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    select.layout-select:hover {
      background: var(--vscode-button-secondaryHoverBackground);
    }
    /* Focus mode select */
    select.focus-mode-select {
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border, transparent);
      padding: 3px 6px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    select.focus-mode-select:hover {
      background: var(--vscode-button-secondaryHoverBackground);
    }
    select.focus-mode-select.focus-active {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
    }
    /* Search input */
    .search-input {
      background: var(--vscode-input-background);
      color: var(--vscode-input-foreground);
      border: 1px solid var(--vscode-input-border, var(--vscode-panel-border));
      padding: 3px 7px;
      font-size: 12px;
      border-radius: 2px;
      width: 160px;
      outline: none;
    }
    .search-input:focus {
      border-color: var(--vscode-focusBorder, #007fd4);
    }
    .search-input.search-error {
      border-color: var(--vscode-errorForeground);
    }
    /* Dimming for search + focus mode — applied to individual nodes/edges */
    .node.dimmed rect,
    .node.dimmed text {
      opacity: 0.2;
    }
    .edgePath.dimmed path {
      opacity: 0.1;
    }
    /* Matched nodes get a bolder stroke */
    .node.matched rect {
      stroke-width: 2.5px;
      stroke: var(--vscode-focusBorder, #007fd4);
    }
    /* Node-focus dimming (focus mode) — orthogonal to search and cluster dim */
    .node.node-focus-dimmed rect,
    .node.node-focus-dimmed text {
      opacity: 0.15;
    }
    .edgePath.node-focus-dimmed path {
      opacity: 0.08;
    }
  </style>
</head>
<body>
  <div id="main-content">
    <div id="viewport">
      <div id="graph-container"><svg id="graph-svg"></svg></div>
    </div>
    <aside id="left-rail">
      <div class="rail-header">
        <h2 class="rail-title">Lineage</h2>
        <button id="rail-toggle" type="button" title="Collapse panel" aria-label="Collapse panel">›</button>
      </div>
      <div class="rail-model" title="${escapeHtml(modelName)}">${escapeHtml(modelName)}</div>

      <div class="rail-section" data-section="controls">
        <button type="button" class="rail-section-toggle" aria-expanded="true">
          <span class="rail-section-label">Controls</span>
          <span class="rail-section-chevron">▾</span>
        </button>
        <div class="rail-section-body">
          <div class="rail-subgroup">
            <div class="rail-subgroup-label">View</div>
            <div class="btn-group" title="View granularity">
              <button id="mode-model" class="active" title="Model-level view (aggregated)">Model</button>
              <button id="mode-column" title="Column-level view (detailed)">Column</button>
            </div>
          </div>

          <div class="rail-subgroup">
            <div class="rail-subgroup-label">Focus</div>
            <select id="focus-mode" class="focus-mode-select" title="Click a node to set focus. Upstream/Downstream dims unrelated nodes.">
              <option value="all">All</option>
              <option value="upstream">Upstream</option>
              <option value="downstream">Downstream</option>
              <option value="selected">Selected only</option>
            </select>
          </div>

          <div class="rail-subgroup">
            <div class="rail-subgroup-label">Cluster</div>
            <select id="cluster-mode" class="cluster-select" title="Group nodes into clusters">
              <option value="none">None</option>
              <option value="schema">Schema</option>
              <option value="source">Source</option>
            </select>
          </div>

          <div class="rail-subgroup">
            <div class="rail-subgroup-label">Layout</div>
            <select id="layout-mode" class="layout-select" title="Graph layout direction">
              <option value="LR">Horizontal</option>
              <option value="TB">Vertical</option>
            </select>
          </div>

          <div class="rail-subgroup">
            <div class="rail-subgroup-label">Search</div>
            <input id="search-input" class="search-input" type="text" placeholder="filter…" title="Filter nodes by name (case-insensitive). Use /regex/ for pattern matching." />
          </div>

          <div class="rail-subgroup">
            <div class="rail-subgroup-label">Zoom</div>
            <div class="zoom-row">
              <button id="zoom-out" title="Zoom out (-)">−</button>
              <button id="zoom-reset" title="Reset zoom (0)">100%</button>
              <button id="zoom-in" title="Zoom in (+)">+</button>
            </div>
            <button id="zoom-fit" title="Fit to view (F)">Fit</button>
          </div>
        </div>
      </div>

      <div class="rail-section" data-section="details">
        <button type="button" class="rail-section-toggle" aria-expanded="true">
          <span class="rail-section-label">Node Details</span>
          <span class="rail-section-chevron">▾</span>
        </button>
        <div class="rail-section-body">
          <div id="side-panel-content">
            <p class="panel-empty">Click a node for details.</p>
          </div>
        </div>
      </div>

      <div class="rail-section" data-section="export">
        <button type="button" class="rail-section-toggle" aria-expanded="true">
          <span class="rail-section-label">Export</span>
          <span class="rail-section-chevron">▾</span>
        </button>
        <div class="rail-section-body">
          <button id="export-svg" class="export-btn" title="Export as SVG">Export SVG</button>
        </div>
      </div>
    </aside>
  </div>
  <div id="status" class="status">Rendering…</div>

  <script id="lineage-data" type="application/json" nonce="${nonce}">${escapeJsonForScript(dataJson)}</script>
  <script src="${graphUri}" nonce="${nonce}"></script>
  <script nonce="${nonce}">
// Error handler lives in its OWN script tag so a parse error in the main
// script tag below doesn't prevent it from registering. Surfaces any
// uncaught error to the status bar instead of leaving "Rendering…" stuck.
(function () {
  function showError(msg) {
    const el = document.getElementById('status');
    if (el) {
      el.textContent = 'Lineage error: ' + msg;
      el.classList.add('error');
    }
  }
  window.addEventListener('error', (e) => {
    if (e.filename && e.lineno) {
      showError((e.message || 'error') + ' (' + e.filename + ':' + e.lineno + ':' + (e.colno || 0) + ')');
    } else {
      showError((e.error && e.error.message) || e.message || 'unknown error');
    }
  });
  window.addEventListener('unhandledrejection', (e) => {
    const r = e.reason;
    showError((r && r.message) || String(r) || 'unhandled rejection');
  });
})();
  </script>
  <script nonce="${nonce}">
(function () {
  'use strict';

  const vscode = acquireVsCodeApi();
  const rawData = JSON.parse(document.getElementById('lineage-data').textContent);
  const status  = document.getElementById('status');
  const svgEl   = document.getElementById('graph-svg');
  const focalModel = rawData.model;

  // Build a column lookup from LineageOutput.columns (array of {name, data_type?}).
  // NOTE: LineageOutput.columns only contains focal model columns.
  // data_type is omitted by the engine when the compiler can't infer a
  // concrete type (e.g. SELECT * against an uncached upstream). Render the
  // type only when it's present.
  const focalColumns = (rawData.columns || []).map(c => ({
    name: c.name,
    dataType: typeof c.data_type === 'string' ? c.data_type : null,
  }));

  // Restore saved state (zoom/pan/viewMode) from prior session if available.
  const saved = vscode.getState() || {};

  if (!window.dagreD3) {
    status.textContent = 'Renderer failed to load (dagreD3 not available).';
    status.classList.add('error');
    return;
  }

  const { dagre, d3 } = window.dagreD3;

  // ── Current state ─────────────────────────────────────────────────────────────
  let currentViewMode = (saved.viewMode === 'column') ? 'column' : 'model';
  let currentClusterMode = (saved.clusterMode === 'schema' || saved.clusterMode === 'source')
    ? saved.clusterMode : 'none';
  let currentLayout = (saved.layout === 'TB') ? 'TB' : 'LR';
  let currentSearchQuery = (typeof saved.searchQuery === 'string') ? saved.searchQuery : '';
  const validFocusModes = ['all', 'upstream', 'downstream', 'selected'];
  let currentFocusMode = validFocusModes.includes(saved.focusMode) ? saved.focusMode : 'all';
  let currentFocusNode = (typeof saved.focusNode === 'string') ? saved.focusNode : null;
  let railCollapsed = saved.railCollapsed === true;
  // IDs of rail-sections that should be rendered collapsed.
  let collapsedSections = new Set(
    Array.isArray(saved.collapsedSections) ? saved.collapsedSections : []
  );
  let selectedNodeModel = null; // currently selected model name (for side panel)
  let focusedClusterId = null;  // currently focused cluster (null = none)

  // Adjacency maps built once per render() for BFS in focus mode
  // nodeId → Set of upstream nodeIds (predecessors)
  // nodeId → Set of downstream nodeIds (successors)
  let adjUpstream = new Map();
  let adjDownstream = new Map();
  // All rendered node ids (non-cluster) for the current graph
  let renderedNodeIds = new Set();

  let currentState = {
    modelName: focalModel,
    scale: saved.scale,
    panX: saved.panX,
    panY: saved.panY,
    viewMode: currentViewMode,
    clusterMode: currentClusterMode,
    layout: currentLayout,
    searchQuery: currentSearchQuery,
    focusMode: currentFocusMode,
    focusNode: currentFocusNode,
    railCollapsed: railCollapsed,
    collapsedSections: Array.from(collapsedSections),
  };

  function persistState(patch) {
    Object.assign(currentState, patch);
    vscode.setState(currentState);
  }

  // ── Cluster key helpers ───────────────────────────────────────────────────────

  /**
   * Derive a schema cluster key from a qualified model name.
   * "schema.model"          → cluster key "schema"
   * "catalog.schema.model"  → cluster key "catalog.schema"
   * "model"                 → null (no dots → no cluster)
   */
  function schemaClusterKey(modelId) {
    const parts = modelId.split('.');
    if (parts.length < 2) return null;
    return parts.slice(0, -1).join('.');
  }

  /**
   * For Source clustering: only nodes without incoming edges are grouped.
   * They share a single synthetic cluster "sources".
   */
  function sourceClusterKey(nodeId, hasIncoming) {
    return hasIncoming.has(nodeId) ? null : 'sources';
  }

  // ── Graph building ────────────────────────────────────────────────────────────

  function buildModelGraph() {
    const useCompound = currentClusterMode !== 'none';
    const g = new dagre.graphlib.Graph({ multigraph: false, compound: useCompound });
    g.setGraph({
      rankdir: currentLayout,
      nodesep: useCompound ? 40 : 30,
      ranksep: useCompound ? 100 : 80,
      marginx: 20,
      marginy: 20,
    });
    g.setDefaultEdgeLabel(() => ({}));

    const seen = new Set();
    const hasIncoming = new Set();
    const hasOutgoing = new Set();

    // Add focal node even if it has no edges
    if (!seen.has(focalModel)) {
      seen.add(focalModel);
      const parts = focalModel.split('.');
      const label = parts[parts.length - 1];
      g.setNode(focalModel, {
        label,
        model: focalModel,
        focal: true,
        width: 140, height: 36, rx: 4, ry: 4,
      });
    }

    for (const edge of rawData.edges) {
      const src = edge.source.model;
      const tgt = edge.target.model;
      if (src === tgt) continue;

      hasOutgoing.add(src);
      hasIncoming.add(tgt);

      if (!seen.has(src)) {
        seen.add(src);
        const parts = src.split('.');
        g.setNode(src, {
          label: parts[parts.length - 1],
          model: src,
          focal: src === focalModel,
          width: src === focalModel ? 140 : 130,
          height: 36, rx: 4, ry: 4,
        });
      }
      if (!seen.has(tgt)) {
        seen.add(tgt);
        const parts = tgt.split('.');
        g.setNode(tgt, {
          label: parts[parts.length - 1],
          model: tgt,
          focal: tgt === focalModel,
          width: tgt === focalModel ? 140 : 130,
          height: 36, rx: 4, ry: 4,
        });
      }
      if (!g.hasEdge(src, tgt)) {
        g.setEdge(src, tgt, {});
      }
    }

    // Apply cluster parents if compound mode is on
    if (useCompound) {
      const clusterLabels = new Map(); // clusterId → display label
      for (const nodeId of g.nodes()) {
        let cid = null;
        let clabel = null;
        if (currentClusterMode === 'schema') {
          const k = schemaClusterKey(nodeId);
          if (k) { cid = 'cluster:schema:' + k; clabel = k; }
        } else if (currentClusterMode === 'source') {
          const k = sourceClusterKey(nodeId, hasIncoming);
          if (k) { cid = 'cluster:source:sources'; clabel = 'Sources'; }
        }
        if (cid && clabel) {
          if (!g.hasNode(cid)) {
            g.setNode(cid, { label: clabel, isCluster: true, width: 0, height: 0 });
            clusterLabels.set(cid, clabel);
          }
          g.setParent(nodeId, cid);
        }
      }
    }

    return { g, hasIncoming, hasOutgoing };
  }

  function buildColumnGraph() {
    const useCompound = currentClusterMode !== 'none';
    const g = new dagre.graphlib.Graph({ multigraph: false, compound: useCompound });
    g.setGraph({
      rankdir: currentLayout,
      nodesep: useCompound ? 28 : 20,
      ranksep: useCompound ? 80 : 60,
      marginx: 20,
      marginy: 20,
    });
    g.setDefaultEdgeLabel(() => ({}));

    function qualifiedId(qc) { return qc.model + '·' + qc.column; }
    function shortLabel(qc) {
      if (qc.model === focalModel) return qc.column;
      const parts = qc.model.split('.');
      return parts[parts.length - 1] + '\\n' + qc.column;
    }

    const seen = new Set();
    const hasIncoming = new Set();
    const hasOutgoing = new Set();
    // Map from column-node id → its model id (for cluster parenting)
    const nodeToModel = new Map();

    for (const edge of rawData.edges) {
      const srcId = qualifiedId(edge.source);
      const tgtId = qualifiedId(edge.target);
      hasOutgoing.add(srcId);
      hasIncoming.add(tgtId);

      if (!seen.has(srcId)) {
        seen.add(srcId);
        nodeToModel.set(srcId, edge.source.model);
        g.setNode(srcId, {
          label: shortLabel(edge.source),
          model: edge.source.model,
          focal: edge.source.model === focalModel,
          width: edge.source.model === focalModel ? 120 : 140,
          height: 36, rx: 4, ry: 4,
        });
      }
      if (!seen.has(tgtId)) {
        seen.add(tgtId);
        nodeToModel.set(tgtId, edge.target.model);
        g.setNode(tgtId, {
          label: shortLabel(edge.target),
          model: edge.target.model,
          focal: edge.target.model === focalModel,
          width: edge.target.model === focalModel ? 120 : 140,
          height: 36, rx: 4, ry: 4,
        });
      }
      g.setEdge(srcId, tgtId, {
        label: edge.transform !== 'direct' ? edge.transform : '',
      });
    }

    if (g.nodeCount() === 0) {
      const id = focalModel + '·(no edges)';
      g.setNode(id, {
        label: focalModel,
        model: focalModel,
        focal: true,
        width: 160, height: 36, rx: 4, ry: 4,
      });
    }

    // Apply cluster parents if compound mode is on.
    // In column mode, cluster by the owning model's cluster key.
    if (useCompound) {
      // Build per-model hasIncoming from model-level perspective
      const modelHasIncoming = new Set();
      for (const edge of rawData.edges) {
        modelHasIncoming.add(edge.target.model);
      }

      for (const nodeId of g.nodes()) {
        const modelId = nodeToModel.get(nodeId) || nodeId;
        let cid = null;
        let clabel = null;
        if (currentClusterMode === 'schema') {
          const k = schemaClusterKey(modelId);
          if (k) { cid = 'cluster:schema:' + k; clabel = k; }
        } else if (currentClusterMode === 'source') {
          const k = sourceClusterKey(modelId, modelHasIncoming);
          if (k) { cid = 'cluster:source:sources'; clabel = 'Sources'; }
        }
        if (cid && clabel) {
          if (!g.hasNode(cid)) {
            g.setNode(cid, { label: clabel, isCluster: true, width: 0, height: 0 });
          }
          g.setParent(nodeId, cid);
        }
      }
    }

    return { g, hasIncoming, hasOutgoing };
  }

  // ── Side panel ────────────────────────────────────────────────────────────────

  function showPanelLoading(modelName) {
    const content = document.getElementById('side-panel-content');
    content.innerHTML = '<p class="panel-loading">Loading details for <strong>' +
      escHtml(modelName) + '</strong>…</p>';
  }

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatDuration(ms) {
    if (ms < 1000) return ms + 'ms';
    return (ms / 1000).toFixed(1) + 's';
  }

  function formatTimestamp(isoStr) {
    if (!isoStr) return '—';
    try {
      const d = new Date(isoStr);
      const now = new Date();
      const diffMs = now - d;
      const diffSec = Math.floor(diffMs / 1000);
      if (diffSec < 60) return diffSec + 's ago';
      const diffMin = Math.floor(diffSec / 60);
      if (diffMin < 60) return diffMin + 'm ago';
      const diffHr = Math.floor(diffMin / 60);
      if (diffHr < 24) return diffHr + 'h ago';
      const diffDay = Math.floor(diffHr / 24);
      return diffDay + 'd ago';
    } catch {
      return isoStr;
    }
  }

  function statusClass(s) {
    const lower = (s || '').toLowerCase();
    if (lower === 'success' || lower === 'ok') return 'status-ok';
    if (lower === 'failed' || lower === 'failure' || lower === 'error') return 'status-fail';
    return 'status-other';
  }

  function renderPanelDetails(modelName, history, errorMsg) {
    const content = document.getElementById('side-panel-content');
    let html = '';

    // ── Model name ──────────────────────────────────────────────────────────
    html += '<div class="panel-section">';
    html += '<div class="panel-section-title">Model</div>';
    html += '<div class="panel-row"><span class="value">' + escHtml(modelName) + '</span></div>';
    html += '</div>';

    // ── Last run status (from history) ──────────────────────────────────────
    html += '<div class="panel-section">';
    html += '<div class="panel-section-title">Last Run</div>';

    if (errorMsg && !history) {
      html += '<p class="panel-error">History unavailable: ' + escHtml(errorMsg) + '</p>';
    } else if (history && history.executions && history.executions.length > 0) {
      const last = history.executions[0];
      const cls = statusClass(last.status);
      html += '<div class="panel-row"><span class="label">Status</span>' +
        '<span class="value ' + cls + '">' + escHtml(last.status) + '</span></div>';
      html += '<div class="panel-row"><span class="label">When</span>' +
        '<span class="value">' + escHtml(formatTimestamp(last.started_at)) + '</span></div>';
      html += '<div class="panel-row"><span class="label">Duration</span>' +
        '<span class="value">' + escHtml(formatDuration(last.duration_ms)) + '</span></div>';
      if (last.rows_affected != null) {
        html += '<div class="panel-row"><span class="label">Rows</span>' +
          '<span class="value">' + escHtml(String(last.rows_affected)) + '</span></div>';
      }
    } else {
      html += '<div class="panel-row"><span class="label">No runs recorded</span></div>';
    }
    html += '</div>';

    // ── Columns (from LineageOutput — only available for the focal model) ──
    if (modelName === focalModel && focalColumns.length > 0) {
      html += '<div class="panel-section">';
      html += '<div class="panel-section-title">Columns (' + focalColumns.length + ')</div>';
      html += '<ul class="column-list">';
      for (const col of focalColumns) {
        const typePart = col.dataType
          ? '<span class="col-type">' + escHtml(col.dataType) + '</span>'
          : '';
        html += '<li><span class="col-name">' + escHtml(col.name) + '</span>' + typePart + '</li>';
      }
      html += '</ul>';
      html += '</div>';
    }

    // ── Open in editor button ───────────────────────────────────────────────
    // data-model attribute is read by the delegated click listener below —
    // inline onclick is blocked by the CSP (script-src nonce, no unsafe-inline).
    html += '<button class="open-btn" data-model="' + escHtml(modelName) + '">Open in Editor</button>';

    content.innerHTML = html;
  }

  // Delegated click listener for the "Open in Editor" button.
  // Must be attached once after the DOM exists; event delegation handles
  // dynamically-injected button content without inline handlers.
  document.getElementById('side-panel-content').addEventListener('click', (e) => {
    const btn = e.target.closest('.open-btn');
    if (btn) {
      vscode.postMessage({ type: 'openModel', name: btn.dataset.model });
    }
  });

  // Listen for messages from the extension host (model details response).
  window.addEventListener('message', (event) => {
    const msg = event.data;
    if (!msg || msg.type !== 'modelDetails') return;
    renderPanelDetails(msg.model, msg.history, msg.errorMsg);
  });

  // ── Rendering ─────────────────────────────────────────────────────────────────
  const defs = d3.select(svgEl).append('defs');
  defs.append('marker')
    .attr('id', 'arrow')
    .attr('viewBox', '0 0 10 10')
    .attr('refX', 9)
    .attr('refY', 5)
    .attr('markerWidth', 6)
    .attr('markerHeight', 6)
    .attr('orient', 'auto')
    .append('path')
    .attr('d', 'M 0 0 L 10 5 L 0 10 z');

  const graphGroup = d3.select(svgEl).append('g');

  const zoom = d3.zoom()
    .scaleExtent([0.1, 8])
    .on('zoom', (event) => {
      graphGroup.attr('transform', event.transform);
      document.getElementById('zoom-reset').textContent =
        Math.round(event.transform.k * 100) + '%';
      persistState({
        scale: event.transform.k,
        panX: event.transform.x,
        panY: event.transform.y,
      });
    });

  d3.select(svgEl).call(zoom);

  let svgWidth = 0;
  let svgHeight = 0;

  // ── Cluster focus / dim ───────────────────────────────────────────────────────

  function focusCluster(clusterId) {
    focusedClusterId = clusterId;
    // Collect member node ids for this cluster
    const memberNodes = new Set();
    const memberEdges = new Set();
    // graphGroup has nodes with data-cluster-id and data-node-id attributes
    d3.selectAll('.node').each(function() {
      const el = d3.select(this);
      const cid = el.attr('data-cluster-id');
      if (cid === clusterId) {
        el.classed('in-focus', true);
        memberNodes.add(el.attr('data-node-id'));
      } else {
        el.classed('in-focus', false);
      }
    });
    d3.selectAll('.cluster-bbox, .cluster-label, .cluster-header-hit').each(function() {
      const el = d3.select(this);
      el.classed('in-focus', el.attr('data-cluster-id') === clusterId);
    });
    // Edges in-focus if both endpoints belong to this cluster
    d3.selectAll('.edgePath').each(function() {
      const el = d3.select(this);
      const src = el.attr('data-src');
      const tgt = el.attr('data-tgt');
      el.classed('in-focus', memberNodes.has(src) && memberNodes.has(tgt));
    });
    graphGroup.classed('dim-mode', true);

    // Zoom to fit the cluster bbox
    const clusterEl = d3.select('.cluster-bbox[data-cluster-id="' + clusterId + '"]');
    if (!clusterEl.empty()) {
      const cx = parseFloat(clusterEl.attr('data-cx'));
      const cy = parseFloat(clusterEl.attr('data-cy'));
      const cw = parseFloat(clusterEl.attr('data-cw'));
      const ch = parseFloat(clusterEl.attr('data-ch'));
      const viewport = document.getElementById('viewport');
      const vw = viewport.clientWidth;
      const vh = viewport.clientHeight;
      const margin = 0.85;
      const k = Math.min(8, Math.max(0.1, Math.min(vw / cw, vh / ch) * margin));
      const tx = (vw - cw * k) / 2 - cx * k;
      const ty = (vh - ch * k) / 2 - cy * k;
      d3.select(svgEl)
        .transition().duration(300)
        .call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(k));
    }
  }

  function clearClusterFocus() {
    focusedClusterId = null;
    graphGroup.classed('dim-mode', false);
    d3.selectAll('.node, .edgePath, .cluster-bbox, .cluster-label, .cluster-header-hit')
      .classed('in-focus', false);
    fitToView();
  }

  // ── Search / filter ───────────────────────────────────────────────────────────

  /**
   * Build a matcher from the current search query string.
   * If query is empty → match everything.
   * If query starts and ends with '/' → attempt regex (/pattern/flags).
   * Otherwise → case-insensitive substring.
   * Returns { test(s): boolean, isError: boolean }.
   */
  function buildMatcher(query) {
    if (!query) return { test: () => true, isError: false };
    // Double-escaped because this entire script body lives inside a TS
    // template literal; the runtime needs to see \/ in the regex literal.
    const regexMatch = query.match(/^\\/(.*)\\/([gimsuy]*)$/);
    if (regexMatch) {
      try {
        const re = new RegExp(regexMatch[1], regexMatch[2] || 'i');
        return { test: (s) => re.test(s), isError: false };
      } catch (_) {
        // Fallback to plain substring on regex parse error
        return { test: (s) => s.toLowerCase().includes(query.toLowerCase()), isError: true };
      }
    }
    const lower = query.toLowerCase();
    return { test: (s) => s.toLowerCase().includes(lower), isError: false };
  }

  /**
   * Apply search dimming without re-running dagre.
   * Matched nodes get class 'matched'; non-matched get 'dimmed'.
   * Edges are dimmed if both endpoints are dimmed.
   * An empty query clears all search classes.
   */
  function applySearchFilter(query) {
    const matcher = buildMatcher(query);
    const searchInput = document.getElementById('search-input');
    if (matcher.isError) {
      searchInput.classList.add('search-error');
    } else {
      searchInput.classList.remove('search-error');
    }

    if (!query) {
      // Clear all search classes
      d3.selectAll('.node').classed('matched', false).classed('dimmed', false);
      d3.selectAll('.edgePath').classed('dimmed', false);
      return;
    }

    // Determine which node-ids match
    const matchedIds = new Set();
    d3.selectAll('.node').each(function() {
      const el = d3.select(this);
      const nodeId = el.attr('data-node-id');
      if (!nodeId) return;
      // In column mode, nodeId is "model·column"; in model mode it's the model name
      const testStr = nodeId.replace('·', '.');
      const matches = matcher.test(testStr);
      el.classed('matched', matches).classed('dimmed', !matches);
      if (matches) matchedIds.add(nodeId);
    });

    // Dim edges where neither endpoint matches
    d3.selectAll('.edgePath').each(function() {
      const el = d3.select(this);
      const src = el.attr('data-src');
      const tgt = el.attr('data-tgt');
      el.classed('dimmed', !matchedIds.has(src) && !matchedIds.has(tgt));
    });
  }

  // ── Focus mode (upstream / downstream / selected only) ────────────────────────

  /**
   * BFS from startId following adj (a Map of nodeId to Set of neighbors).
   * Returns a Set of all reachable node ids (including startId).
   */
  function bfsReachable(startId, adj) {
    const visited = new Set();
    const queue = [startId];
    while (queue.length > 0) {
      const id = queue.shift();
      if (visited.has(id)) continue;
      visited.add(id);
      const neighbors = adj.get(id);
      if (neighbors) {
        for (const n of neighbors) {
          if (!visited.has(n)) queue.push(n);
        }
      }
    }
    return visited;
  }

  /**
   * Apply node-focus dimming without re-running dagre.
   * Uses the node-focus-dimmed class so it's orthogonal to search's dimmed
   * and cluster's dim-mode.
   */
  function applyFocusMode() {
    if (currentFocusMode === 'all' || !currentFocusNode) {
      d3.selectAll('.node').classed('node-focus-dimmed', false);
      d3.selectAll('.edgePath').classed('node-focus-dimmed', false);
      return;
    }

    // Verify the focus node still exists in the rendered graph
    if (!renderedNodeIds.has(currentFocusNode)) {
      d3.selectAll('.node').classed('node-focus-dimmed', false);
      d3.selectAll('.edgePath').classed('node-focus-dimmed', false);
      return;
    }

    let keepIds;
    if (currentFocusMode === 'selected') {
      keepIds = new Set([currentFocusNode]);
    } else if (currentFocusMode === 'upstream') {
      // Upstream = nodes that can reach the focus node (follow adjUpstream from focus)
      keepIds = bfsReachable(currentFocusNode, adjUpstream);
      keepIds.add(currentFocusNode);
    } else if (currentFocusMode === 'downstream') {
      // Downstream = nodes reachable from the focus node
      keepIds = bfsReachable(currentFocusNode, adjDownstream);
      keepIds.add(currentFocusNode);
    } else {
      keepIds = new Set([currentFocusNode]);
    }

    d3.selectAll('.node').each(function() {
      const el = d3.select(this);
      const nodeId = el.attr('data-node-id');
      el.classed('node-focus-dimmed', nodeId ? !keepIds.has(nodeId) : false);
    });

    d3.selectAll('.edgePath').each(function() {
      const el = d3.select(this);
      const src = el.attr('data-src');
      const tgt = el.attr('data-tgt');
      // Keep edge if at least one endpoint is in the focus set
      el.classed('node-focus-dimmed', !keepIds.has(src) && !keepIds.has(tgt));
    });
  }

  /** Update the focus select visual state (active highlight when not "all"). */
  function updateFocusSelectStyle() {
    const sel = document.getElementById('focus-mode');
    if (sel) sel.classList.toggle('focus-active', currentFocusMode !== 'all');
  }

  function render(viewMode) {
    graphGroup.selectAll('*').remove();
    focusedClusterId = null;
    graphGroup.classed('dim-mode', false);

    const built = viewMode === 'model' ? buildModelGraph() : buildColumnGraph();
    const { g, hasIncoming, hasOutgoing } = built;

    dagre.layout(g);

    const gl = g.graph();
    svgWidth  = (gl.width  || 200) + 40;
    svgHeight = (gl.height || 100) + 40;

    d3.select(svgEl)
      .attr('viewBox', '0 0 ' + svgWidth + ' ' + svgHeight)
      .attr('preserveAspectRatio', 'xMidYMid meet');

    // ── Cluster bounding boxes (render BEFORE nodes so nodes appear on top) ──
    const clusterGroup = graphGroup.append('g').attr('class', 'clusters');
    if (currentClusterMode !== 'none') {
      for (const nodeId of g.nodes()) {
        const n = g.node(nodeId);
        if (!n || !n.isCluster) continue;
        // dagre gives x/y as center, width/height as the full bbox
        const bx = n.x - n.width / 2;
        const by = n.y - n.height / 2;
        const bw = n.width;
        const bh = n.height;
        const cid = nodeId;

        const cg = clusterGroup.append('g').attr('class', 'cluster-group');

        cg.append('rect')
          .attr('class', 'cluster-bbox')
          .attr('x', bx)
          .attr('y', by)
          .attr('width', bw)
          .attr('height', bh)
          .attr('rx', 6)
          .attr('ry', 6)
          .attr('data-cluster-id', cid)
          // Store bbox coords for focusCluster zoom math
          .attr('data-cx', bx)
          .attr('data-cy', by)
          .attr('data-cw', bw)
          .attr('data-ch', bh);

        // Invisible hit region over the label row at the top of the bbox
        const hitH = 18;
        cg.append('rect')
          .attr('class', 'cluster-header-hit')
          .attr('x', bx + 4)
          .attr('y', by + 2)
          .attr('width', Math.max(0, bw - 8))
          .attr('height', hitH)
          .attr('data-cluster-id', cid)
          .on('click', (e) => {
            e.stopPropagation();
            if (focusedClusterId === cid) {
              clearClusterFocus();
            } else {
              focusCluster(cid);
            }
          });

        cg.append('text')
          .attr('class', 'cluster-label')
          .attr('x', bx + 8)
          .attr('y', by + 13)
          .attr('data-cluster-id', cid)
          .text(n.label);
      }
    }

    // Edges
    const edgeGroup = graphGroup.append('g').attr('class', 'edges');
    for (const e of g.edges()) {
      const edgeData = g.edge(e);
      const points = edgeData.points;
      const line = d3.line()
        .x(p => p.x)
        .y(p => p.y)
        .curve(d3.curveCatmullRom.alpha(0.5));

      const ep = edgeGroup.append('g')
        .attr('class', 'edgePath')
        .attr('data-src', e.v)
        .attr('data-tgt', e.w);
      ep.append('path')
        .attr('d', line(points))
        .attr('marker-end', 'url(#arrow)');

      if (edgeData.label) {
        const mid = points[Math.floor(points.length / 2)];
        const el = edgeGroup.append('g').attr('class', 'edgeLabel');
        el.append('text')
          .attr('x', mid.x)
          .attr('y', mid.y - 4)
          .attr('text-anchor', 'middle')
          .text(edgeData.label);
      }
    }

    // ── Build adjacency maps for focus-mode BFS ───────────────────────────────
    adjUpstream = new Map();
    adjDownstream = new Map();
    renderedNodeIds = new Set();
    for (const e of g.edges()) {
      // e.v → e.w means e.v is upstream of e.w
      if (!adjDownstream.has(e.v)) adjDownstream.set(e.v, new Set());
      adjDownstream.get(e.v).add(e.w);
      if (!adjUpstream.has(e.w)) adjUpstream.set(e.w, new Set());
      adjUpstream.get(e.w).add(e.v);
    }
    for (const nodeId of g.nodes()) {
      if (!g.node(nodeId).isCluster) renderedNodeIds.add(nodeId);
    }

    // If the saved focusNode is no longer in the graph, clear it
    if (currentFocusNode && !renderedNodeIds.has(currentFocusNode)) {
      currentFocusNode = null;
      persistState({ focusNode: null });
    }

    // Nodes
    const nodeGroup = graphGroup.append('g').attr('class', 'nodes');
    for (const nodeId of g.nodes()) {
      const n = g.node(nodeId);
      // Skip cluster parent nodes — they only define bboxes, rendered above
      if (n.isCluster) continue;

      const isSource = !hasIncoming.has(nodeId);
      const isLeaf   = !hasOutgoing.has(nodeId);
      // Re-apply selection highlight if this node was selected before re-render
      const isSelected = (n.model === selectedNodeModel);
      let cls = n.focal ? 'node focal' : 'node';
      if (!n.focal && isSource) cls += ' source';
      else if (!n.focal && isLeaf) cls += ' leaf';
      if (isSelected) cls += ' selected';

      // Determine cluster membership for data attribute
      const parentId = currentClusterMode !== 'none' ? (g.parent(nodeId) || '') : '';

      const ng = nodeGroup.append('g')
        .attr('class', cls)
        .attr('data-node-id', nodeId)
        .attr('data-cluster-id', parentId)
        .attr('transform', 'translate(' + (n.x - n.width / 2) + ',' + (n.y - n.height / 2) + ')');

      ng.append('rect')
        .attr('width', n.width)
        .attr('height', n.height)
        .attr('rx', n.rx || 4)
        .attr('ry', n.ry || 4);

      ng.append('title').text('Click to see details for ' + n.model);

      const lines = n.label.split('\\n');
      const lineHeight = 14;
      const totalH = lines.length * lineHeight;
      const startY = (n.height - totalH) / 2 + lineHeight;
      lines.forEach((line, i) => {
        ng.append('text')
          .attr('x', n.width / 2)
          .attr('y', startY + i * lineHeight)
          .attr('text-anchor', 'middle')
          .attr('dominant-baseline', 'auto')
          .text(line);
      });

      // Click: open side panel + update focus node if focus mode is active
      ng.on('click', (e) => {
        e.stopPropagation();
        // Update selected state
        selectedNodeModel = n.model;
        // Update visual selection — toggle class on all nodes
        d3.selectAll('.node').classed('selected', false);
        ng.classed('selected', true);
        // Update focus node when focus mode is active
        if (currentFocusMode !== 'all') {
          currentFocusNode = nodeId;
          persistState({ focusNode: nodeId });
          applyFocusMode();
        }
        // Show loading state in panel
        showPanelLoading(n.model);
        // Request details from extension host
        vscode.postMessage({ type: 'loadModelDetails', name: n.model });
      });
    }

    // Update status bar
    const clusterSuffix = currentClusterMode !== 'none' ? ' · cluster: ' + currentClusterMode : '';
    if (viewMode === 'model') {
      // nodeCount excludes cluster parents
      const leafNodes = g.nodes().filter(id => !g.node(id).isCluster);
      const nodeCount = leafNodes.length;
      const edgeCount = g.edgeCount();
      status.textContent =
        nodeCount + ' model(s) · ' + edgeCount + ' dep(s) · ' +
        rawData.upstream.length + ' upstream · ' + rawData.downstream.length + ' downstream' +
        clusterSuffix + ' · click a node for details · drag to pan · scroll to zoom · F to fit · 0 to reset';
    } else {
      const edgeCount = rawData.edges.length;
      if (edgeCount === 0) {
        status.textContent =
          'No column-level edges found for ' + focalModel +
          '. The model may have no typed columns or no upstream dependencies.';
      } else {
        status.textContent =
          edgeCount + ' column edge(s) · ' +
          rawData.upstream.length + ' upstream · ' + rawData.downstream.length + ' downstream' +
          clusterSuffix + ' · click a node for details · drag to pan · scroll to zoom · F to fit · 0 to reset';
      }
    }

    // Re-apply search filter and focus mode after each render
    applySearchFilter(currentSearchQuery);
    applyFocusMode();
  }

  // ── Initial render ────────────────────────────────────────────────────────────
  render(currentViewMode);

  function updateModeButtons() {
    document.getElementById('mode-model').classList.toggle('active', currentViewMode === 'model');
    document.getElementById('mode-column').classList.toggle('active', currentViewMode === 'column');
  }
  updateModeButtons();

  // Restore cluster mode select to saved value
  document.getElementById('cluster-mode').value = currentClusterMode;

  // Restore layout select to saved value
  document.getElementById('layout-mode').value = currentLayout;

  // Restore search query to saved value
  if (currentSearchQuery) {
    document.getElementById('search-input').value = currentSearchQuery;
    applySearchFilter(currentSearchQuery);
  }

  // ── Search input (debounced 250ms) ────────────────────────────────────────────
  let searchDebounceTimer = null;
  document.getElementById('search-input').addEventListener('input', (e) => {
    const query = e.target.value;
    clearTimeout(searchDebounceTimer);
    searchDebounceTimer = setTimeout(() => {
      currentSearchQuery = query;
      persistState({ searchQuery: query });
      applySearchFilter(query);
    }, 250);
  });

  // Restore focus mode select to saved value
  document.getElementById('focus-mode').value = currentFocusMode;
  updateFocusSelectStyle();

  // ── Focus mode select ─────────────────────────────────────────────────────────
  document.getElementById('focus-mode').addEventListener('change', (e) => {
    const newMode = e.target.value;
    if (newMode === currentFocusMode) return;
    currentFocusMode = newMode;
    // Clear focus node when switching to "all"
    if (newMode === 'all') {
      currentFocusNode = null;
      persistState({ focusMode: newMode, focusNode: null });
      d3.selectAll('.node').classed('node-focus-dimmed', false);
      d3.selectAll('.edgePath').classed('node-focus-dimmed', false);
    } else {
      persistState({ focusMode: newMode });
      // Re-apply with current focus node (may be null — no dimming until click)
      applyFocusMode();
    }
    updateFocusSelectStyle();
  });

  // ── Background click → clear cluster focus + node focus ──────────────────────
  svgEl.addEventListener('click', (e) => {
    // Only clear if the click target is the SVG itself, not a child element
    if (e.target !== svgEl) return;
    let changed = false;
    if (focusedClusterId !== null) {
      clearClusterFocus();
      changed = true;
    }
    if (currentFocusMode !== 'all' && currentFocusNode !== null) {
      currentFocusNode = null;
      persistState({ focusNode: null });
      d3.selectAll('.node').classed('node-focus-dimmed', false);
      d3.selectAll('.edgePath').classed('node-focus-dimmed', false);
      changed = true;
    }
  });

  // ── View mode toggle ──────────────────────────────────────────────────────────
  document.getElementById('mode-model').onclick = () => {
    if (currentViewMode === 'model') return;
    currentViewMode = 'model';
    persistState({ viewMode: 'model' });
    updateModeButtons();
    render('model');
    fitToView();
  };
  document.getElementById('mode-column').onclick = () => {
    if (currentViewMode === 'column') return;
    currentViewMode = 'column';
    persistState({ viewMode: 'column' });
    updateModeButtons();
    render('column');
    fitToView();
  };

  // ── Cluster mode select ───────────────────────────────────────────────────────
  document.getElementById('cluster-mode').addEventListener('change', (e) => {
    const newMode = e.target.value;
    if (newMode === currentClusterMode) return;
    currentClusterMode = newMode;
    persistState({ clusterMode: newMode });
    render(currentViewMode);
    fitToView();
  });

  // ── Layout direction select ───────────────────────────────────────────────────
  document.getElementById('layout-mode').addEventListener('change', (e) => {
    const newLayout = e.target.value;
    if (newLayout === currentLayout) return;
    currentLayout = newLayout;
    persistState({ layout: newLayout });
    render(currentViewMode);
    fitToView();
  });

  // ── Fit to view ──────────────────────────────────────────────────────────────
  function fitToView() {
    const viewport = document.getElementById('viewport');
    const vw = viewport.clientWidth;
    const vh = viewport.clientHeight;
    if (!vw || !vh || !svgWidth || !svgHeight) return;
    const margin = 0.9;
    const k = Math.min(8, Math.max(0.1, Math.min(vw / svgWidth, vh / svgHeight) * margin));
    const tx = (vw - svgWidth * k) / 2;
    const ty = (vh - svgHeight * k) / 2;
    d3.select(svgEl)
      .transition().duration(200)
      .call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(k));
  }

  // ── Zoom buttons ─────────────────────────────────────────────────────────────
  document.getElementById('zoom-in').onclick  = () => {
    d3.select(svgEl).transition().duration(150).call(zoom.scaleBy, 1.2);
  };
  document.getElementById('zoom-out').onclick = () => {
    d3.select(svgEl).transition().duration(150).call(zoom.scaleBy, 1 / 1.2);
  };
  document.getElementById('zoom-reset').onclick = () => {
    d3.select(svgEl).transition().duration(200).call(zoom.transform, d3.zoomIdentity);
  };
  document.getElementById('zoom-fit').onclick = fitToView;

  // ── Keyboard shortcuts ───────────────────────────────────────────────────────
  window.addEventListener('keydown', (e) => {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
    if (e.key === '+' || e.key === '=') {
      d3.select(svgEl).transition().duration(150).call(zoom.scaleBy, 1.2);
      e.preventDefault();
    } else if (e.key === '-') {
      d3.select(svgEl).transition().duration(150).call(zoom.scaleBy, 1 / 1.2);
      e.preventDefault();
    } else if (e.key === '0') {
      d3.select(svgEl).transition().duration(200).call(zoom.transform, d3.zoomIdentity);
      e.preventDefault();
    } else if (e.key === 'f' || e.key === 'F') {
      fitToView();
      e.preventDefault();
    }
  });

  // ── Export SVG ───────────────────────────────────────────────────────────────
  // The live SVG references VS Code CSS variables (e.g. --vscode-foreground)
  // that don't resolve outside the webview, has no xmlns, and lacks a
  // background. Build a standalone-readable clone that inlines a resolved
  // stylesheet, adds the SVG namespace, resets the user's pan/zoom, and paints
  // the editor-background color underneath.
  document.getElementById('export-svg').onclick = () => {
    const clone = svgEl.cloneNode(true);
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
    clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');

    // Reset the d3-zoom transform on the cloned graph group so the export
    // shows the full graph (not the user's current zoom/pan).
    const gg = clone.querySelector('g[transform]');
    if (gg) gg.removeAttribute('transform');

    // Set width/height attrs (some viewers ignore SVGs without them).
    const vb = (clone.getAttribute('viewBox') || '').split(/\\s+/);
    if (vb.length === 4) {
      clone.setAttribute('width', vb[2]);
      clone.setAttribute('height', vb[3]);
    }

    // Resolve VS Code CSS variables to concrete colors.
    const root = getComputedStyle(document.documentElement);
    const v = (name, fallback) => (root.getPropertyValue(name).trim() || fallback);
    const fg = v('--vscode-foreground', '#cccccc');
    const bg = v('--vscode-editor-background', '#1e1e1e');
    const muted = v('--vscode-descriptionForeground', '#888888');
    const focalBg = v('--vscode-button-background', '#0e639c');
    const focalFg = v('--vscode-button-foreground', '#ffffff');
    const badgeBg = v('--vscode-badge-background', '#4d4d4d');
    const sourceFill = v('--vscode-charts-blue', badgeBg);
    const leafFill = v('--vscode-charts-green', badgeBg);
    const panelBorder = v('--vscode-panel-border', '#444444');

    // Inline stylesheet — order matters; .focal overrides .source/.leaf.
    const svgNs = 'http://www.w3.org/2000/svg';
    const styleEl = document.createElementNS(svgNs, 'style');
    styleEl.textContent =
      'text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 11px; }' +
      '.node rect { fill: ' + badgeBg + '; stroke: ' + fg + '; stroke-width: 1.5px; }' +
      '.node text { fill: ' + fg + '; }' +
      '.node.source rect { fill: ' + sourceFill + '; }' +
      '.node.leaf rect { fill: ' + leafFill + '; }' +
      '.node.focal rect { fill: ' + focalBg + '; stroke: ' + focalFg + '; stroke-width: 2px; }' +
      '.node.focal text { fill: ' + focalFg + '; font-weight: 600; }' +
      '.cluster-bbox { fill: transparent; stroke: ' + panelBorder + '; stroke-width: 1px; stroke-dasharray: 4 3; }' +
      '.cluster-label { fill: ' + muted + '; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }' +
      '.cluster-header-hit { display: none; }' +
      '.edgePath path { stroke: ' + muted + '; stroke-width: 1.5px; fill: none; }' +
      '.edgeLabel text { fill: ' + muted + '; font-size: 10px; }' +
      'marker path { fill: ' + muted + '; stroke: none; }';

    // Background rect so the SVG isn't transparent when opened standalone.
    const bgRect = document.createElementNS(svgNs, 'rect');
    bgRect.setAttribute('x', '0');
    bgRect.setAttribute('y', '0');
    bgRect.setAttribute('width', '100%');
    bgRect.setAttribute('height', '100%');
    bgRect.setAttribute('fill', bg);

    clone.insertBefore(styleEl, clone.firstChild);
    clone.insertBefore(bgRect, styleEl.nextSibling);

    const xml = '<?xml version="1.0" encoding="UTF-8"?>\\n' + clone.outerHTML;
    const blob = new Blob([xml], { type: 'image/svg+xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = focalModel + '-lineage.svg';
    a.click();
    URL.revokeObjectURL(url);
  };

  // ── Rail collapse toggle ─────────────────────────────────────────────────────
  const railEl = document.getElementById('left-rail');
  const railToggleBtn = document.getElementById('rail-toggle');
  function applyRailState() {
    railEl.classList.toggle('collapsed', railCollapsed);
    // Rail is right-docked: ‹ expands (push left), › collapses (push right).
    railToggleBtn.textContent = railCollapsed ? '‹' : '›';
    railToggleBtn.title = railCollapsed ? 'Expand panel' : 'Collapse panel';
    railToggleBtn.setAttribute('aria-label', railToggleBtn.title);
  }
  applyRailState();
  railToggleBtn.onclick = () => {
    railCollapsed = !railCollapsed;
    persistState({ railCollapsed: railCollapsed });
    applyRailState();
    // Re-fit after the rail width animation finishes so the graph re-centers
    // into the new viewport.
    setTimeout(() => fitToView(), 180);
  };

  // ── Per-section accordion toggles ────────────────────────────────────────────
  // Each rail-section has a header button that collapses/expands its body.
  // The collapsed set is persisted so reloads remember which sections the user
  // had closed.
  function applySectionState(section) {
    const id = section.getAttribute('data-section');
    const collapsed = collapsedSections.has(id);
    section.classList.toggle('collapsed', collapsed);
    const toggle = section.querySelector('.rail-section-toggle');
    if (toggle) toggle.setAttribute('aria-expanded', String(!collapsed));
  }
  document.querySelectorAll('#left-rail .rail-section').forEach((section) => {
    applySectionState(section);
    const toggle = section.querySelector('.rail-section-toggle');
    if (!toggle) return;
    toggle.addEventListener('click', () => {
      const id = section.getAttribute('data-section');
      if (collapsedSections.has(id)) collapsedSections.delete(id);
      else collapsedSections.add(id);
      applySectionState(section);
      persistState({ collapsedSections: Array.from(collapsedSections) });
    });
  });

  // ── Initial zoom/pan restore or auto-fit ──────────────────────────────────────
  if (typeof saved.scale === 'number' && (saved.panX !== undefined || saved.panY !== undefined)) {
    d3.select(svgEl).call(
      zoom.transform,
      d3.zoomIdentity.translate(saved.panX || 0, saved.panY || 0).scale(saved.scale),
    );
  } else {
    fitToView();
  }
})();
  </script>
</body>
</html>`;
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeJsonForScript(json: string): string {
  // Prevent </script> from terminating the parent script tag.
  return json.replace(/<\/script/gi, "<\\/script");
}

function makeNonce(): string {
  let result = "";
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < 32; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}
