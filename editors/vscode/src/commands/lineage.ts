import * as path from "path";
import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";
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
  const workspaceFolder = getWorkspaceFolder();
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
          const args: string[] = [];
          if (workspaceFolder) {
            args.push("--config", `${workspaceFolder}/rocky.toml`);
          }
          args.push("history", "--model", clickedModel, "--output", "json");
          const { stdout } = await runRocky(args, { cwd: workspaceFolder });
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
  const args = ["lineage", modelName];
  if (workspaceFolder) {
    args.unshift("--config", `${workspaceFolder}/rocky.toml`);
  }

  try {
    const { stdout } = await runRocky(args, { cwd: workspaceFolder });
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
    header {
      display: flex; align-items: center; gap: 8px;
      padding: 8px 12px;
      border-bottom: 1px solid var(--vscode-panel-border);
      background: var(--vscode-editor-background);
      flex-shrink: 0;
    }
    header h2 { margin: 0; font-size: 13px; font-weight: 600; }
    header .spacer { flex: 1; }
    .btn-group {
      display: flex; gap: 0;
    }
    .btn-group button {
      border-radius: 0;
      border-right-width: 0;
    }
    .btn-group button:first-child { border-radius: 2px 0 0 2px; }
    .btn-group button:last-child  { border-radius: 0 2px 2px 0; border-right-width: 1px; }
    button {
      background: var(--vscode-button-secondaryBackground);
      color: var(--vscode-button-secondaryForeground);
      border: 1px solid var(--vscode-button-border, transparent);
      padding: 4px 10px;
      font-size: 12px;
      cursor: pointer;
      border-radius: 2px;
    }
    button:hover { background: var(--vscode-button-secondaryHoverBackground); }
    button.active {
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
    }
    .separator {
      width: 1px;
      background: var(--vscode-panel-border);
      margin: 0 4px;
    }
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
    /* Side panel */
    #side-panel {
      width: 320px;
      min-width: 320px;
      border-left: 1px solid var(--vscode-panel-border);
      background: var(--vscode-sideBar-background, var(--vscode-editor-background));
      display: flex; flex-direction: column;
      overflow: hidden;
    }
    #side-panel-header {
      padding: 10px 12px 6px;
      border-bottom: 1px solid var(--vscode-panel-border);
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--vscode-descriptionForeground);
      flex-shrink: 0;
    }
    #side-panel-content {
      flex: 1; overflow-y: auto; padding: 12px;
    }
    .panel-empty {
      color: var(--vscode-descriptionForeground);
      font-size: 12px;
      text-align: center;
      margin-top: 40px;
    }
    .panel-loading {
      color: var(--vscode-descriptionForeground);
      font-size: 12px;
      text-align: center;
      margin-top: 40px;
    }
    .panel-section {
      margin-bottom: 14px;
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
  </style>
</head>
<body>
  <header>
    <h2>Lineage: ${escapeHtml(modelName)}</h2>
    <span class="spacer"></span>
    <div class="btn-group" title="View granularity">
      <button id="mode-model" class="active" title="Model-level view (aggregated)">Model</button>
      <button id="mode-column" title="Column-level view (detailed)">Column</button>
    </div>
    <div class="separator"></div>
    <span class="cluster-select-label">Cluster:</span>
    <select id="cluster-mode" class="cluster-select" title="Group nodes into clusters">
      <option value="none">None</option>
      <option value="schema">Schema</option>
      <option value="source">Source</option>
    </select>
    <div class="separator"></div>
    <span class="cluster-select-label">Layout:</span>
    <select id="layout-mode" class="layout-select" title="Graph layout direction">
      <option value="LR">Horizontal</option>
      <option value="TB">Vertical</option>
    </select>
    <div class="separator"></div>
    <button id="zoom-out" title="Zoom out (-)">−</button>
    <button id="zoom-reset" title="Reset zoom (0)">100%</button>
    <button id="zoom-in" title="Zoom in (+)">+</button>
    <button id="zoom-fit" title="Fit to view (F)">Fit</button>
    <button id="export-svg" title="Export as SVG">Export SVG</button>
  </header>
  <div id="main-content">
    <div id="viewport">
      <div id="graph-container"><svg id="graph-svg"></svg></div>
    </div>
    <div id="side-panel">
      <div id="side-panel-header">Node Details</div>
      <div id="side-panel-content">
        <p class="panel-empty">Click a node for details.</p>
      </div>
    </div>
  </div>
  <div id="status" class="status">Rendering…</div>

  <script id="lineage-data" type="application/json" nonce="${nonce}">${escapeJsonForScript(dataJson)}</script>
  <script src="${graphUri}" nonce="${nonce}"></script>
  <script nonce="${nonce}">
(function () {
  'use strict';

  const vscode = acquireVsCodeApi();
  const rawData = JSON.parse(document.getElementById('lineage-data').textContent);
  const status  = document.getElementById('status');
  const svgEl   = document.getElementById('graph-svg');
  const focalModel = rawData.model;

  // Build a column lookup from LineageOutput.columns (array of {name}).
  // key: model name (qualified), value: array of column name strings.
  // NOTE: LineageOutput.columns only contains focal model columns.
  const focalColumns = (rawData.columns || []).map(c => c.name);

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
  let selectedNodeModel = null; // currently selected model name (for side panel)
  let focusedClusterId = null;  // currently focused cluster (null = none)

  let currentState = {
    modelName: focalModel,
    scale: saved.scale,
    panX: saved.panX,
    panY: saved.panY,
    viewMode: currentViewMode,
    clusterMode: currentClusterMode,
    layout: currentLayout,
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
        html += '<li><span class="col-name">' + escHtml(col) + '</span></li>';
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

      // Click: open side panel (not the file directly)
      ng.on('click', (e) => {
        e.stopPropagation();
        // Update selected state
        selectedNodeModel = n.model;
        // Update visual selection — toggle class on all nodes
        d3.selectAll('.node').classed('selected', false);
        ng.classed('selected', true);
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

  // ── Background click → clear cluster focus ────────────────────────────────────
  svgEl.addEventListener('click', (e) => {
    // Only clear if the click target is the SVG itself, not a child element
    if (e.target === svgEl && focusedClusterId !== null) {
      clearClusterFocus();
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
  document.getElementById('export-svg').onclick = () => {
    const blob = new Blob([svgEl.outerHTML], { type: 'image/svg+xml' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = focalModel + '-lineage.svg';
    a.click();
    URL.revokeObjectURL(url);
  };

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
