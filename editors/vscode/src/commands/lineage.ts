import * as path from "path";
import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";
import { getExtensionUri } from "../extensionState";
import { runRocky } from "../rockyCli";
import type { LineageOutput } from "../types/generated/lineage";
import { resolveModelName } from "./ui";

const VIEW_TYPE = "rockyLineage";

interface SerializedState {
  modelName?: string;
  scale?: number;
  panX?: number;
  panY?: number;
  viewMode?: "model" | "column";
}

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

  // Resolve clicks from the webview by globbing the workspace for the model file
  // and opening the first match. Disposed when the panel closes.
  panel.webview.onDidReceiveMessage(
    async (msg: { type?: string; name?: string }) => {
      if (msg?.type !== "openModel" || typeof msg.name !== "string") return;
      const matches = await vscode.workspace.findFiles(
        `**/models/**/${msg.name}.{rocky,sql}`,
        undefined,
        1,
      );
      if (matches[0]) {
        void vscode.commands.executeCommand("vscode.open", matches[0]);
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
    <button id="zoom-out" title="Zoom out (-)">−</button>
    <button id="zoom-reset" title="Reset zoom (0)">100%</button>
    <button id="zoom-in" title="Zoom in (+)">+</button>
    <button id="zoom-fit" title="Fit to view (F)">Fit</button>
    <button id="export-svg" title="Export as SVG">Export SVG</button>
  </header>
  <div id="viewport">
    <div id="graph-container"><svg id="graph-svg"></svg></div>
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

  // Restore saved state (zoom/pan/viewMode) from prior session if available.
  const saved = vscode.getState() || {};

  if (!window.dagreD3) {
    status.textContent = 'Renderer failed to load (dagreD3 not available).';
    status.classList.add('error');
    return;
  }

  const { dagre, d3 } = window.dagreD3;

  // ── Current state ─────────────────────────────────────────────────────────────
  // "model" is the default — less noisy, matches typical lineage expectation.
  let currentViewMode = (saved.viewMode === 'column') ? 'column' : 'model';

  // Merged state object — updated incrementally so each setState call
  // preserves all fields.
  let currentState = {
    modelName: focalModel,
    scale: saved.scale,
    panX: saved.panX,
    panY: saved.panY,
    viewMode: currentViewMode,
  };

  function persistState(patch) {
    Object.assign(currentState, patch);
    vscode.setState(currentState);
  }

  // ── Graph building ────────────────────────────────────────────────────────────

  function buildModelGraph() {
    // Aggregate column-level edges into model-level edges (deduplicated).
    const g = new dagre.graphlib.Graph({ multigraph: false });
    g.setGraph({
      rankdir: 'LR',
      nodesep: 30,
      ranksep: 80,
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
      if (src === tgt) continue; // skip self-edges

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
      // dagre deduplicates by (src, tgt) key in a non-multigraph
      if (!g.hasEdge(src, tgt)) {
        g.setEdge(src, tgt, {});
      }
    }

    return { g, hasIncoming, hasOutgoing };
  }

  function buildColumnGraph() {
    const g = new dagre.graphlib.Graph({ multigraph: false });
    g.setGraph({
      rankdir: 'LR',
      nodesep: 20,
      ranksep: 60,
      marginx: 20,
      marginy: 20,
    });
    g.setDefaultEdgeLabel(() => ({}));

    function qualifiedId(qc) { return qc.model + '.' + qc.column; }
    function shortLabel(qc) {
      if (qc.model === focalModel) return qc.column;
      const parts = qc.model.split('.');
      return parts[parts.length - 1] + '\\n' + qc.column;
    }

    const seen = new Set();
    const hasIncoming = new Set();
    const hasOutgoing = new Set();

    for (const edge of rawData.edges) {
      const srcId = qualifiedId(edge.source);
      const tgtId = qualifiedId(edge.target);
      hasOutgoing.add(srcId);
      hasIncoming.add(tgtId);

      if (!seen.has(srcId)) {
        seen.add(srcId);
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
      const id = focalModel + '.(no edges)';
      g.setNode(id, {
        label: focalModel,
        model: focalModel,
        focal: true,
        width: 160, height: 36, rx: 4, ry: 4,
      });
    }

    return { g, hasIncoming, hasOutgoing };
  }

  // ── Rendering ─────────────────────────────────────────────────────────────────
  // Arrow marker + zoom are set up once; render() reuses them.

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

  // d3-zoom — attach once; render() updates the viewBox
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

  function render(viewMode) {
    // Clear previous graph contents (not defs or the root graphGroup)
    graphGroup.selectAll('*').remove();

    const built = viewMode === 'model' ? buildModelGraph() : buildColumnGraph();
    const { g, hasIncoming, hasOutgoing } = built;

    dagre.layout(g);

    const gl = g.graph();
    svgWidth  = (gl.width  || 200) + 40;
    svgHeight = (gl.height || 100) + 40;

    d3.select(svgEl)
      .attr('viewBox', '0 0 ' + svgWidth + ' ' + svgHeight)
      .attr('preserveAspectRatio', 'xMidYMid meet');

    // Edges
    const edgeGroup = graphGroup.append('g').attr('class', 'edges');
    for (const e of g.edges()) {
      const edgeData = g.edge(e);
      const points = edgeData.points;
      const line = d3.line()
        .x(p => p.x)
        .y(p => p.y)
        .curve(d3.curveCatmullRom.alpha(0.5));

      const ep = edgeGroup.append('g').attr('class', 'edgePath');
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
      const isSource = !hasIncoming.has(nodeId);
      const isLeaf   = !hasOutgoing.has(nodeId);
      let cls = n.focal ? 'node focal' : 'node';
      if (!n.focal && isSource) cls += ' source';
      else if (!n.focal && isLeaf) cls += ' leaf';

      const ng = nodeGroup.append('g')
        .attr('class', cls)
        .attr('transform', 'translate(' + (n.x - n.width / 2) + ',' + (n.y - n.height / 2) + ')');

      ng.append('rect')
        .attr('width', n.width)
        .attr('height', n.height)
        .attr('rx', n.rx || 4)
        .attr('ry', n.ry || 4);

      ng.append('title').text('Open ' + n.model);

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

      ng.on('click', (e) => {
        e.stopPropagation();
        vscode.postMessage({ type: 'openModel', name: n.model });
      });
    }

    // Update status bar
    if (viewMode === 'model') {
      const nodeCount = g.nodeCount();
      const edgeCount = g.edgeCount();
      status.textContent =
        nodeCount + ' model(s) · ' + edgeCount + ' dep(s) · ' +
        rawData.upstream.length + ' upstream · ' + rawData.downstream.length + ' downstream · ' +
        'drag to pan · scroll to zoom · click a node to open · F to fit · 0 to reset';
    } else {
      const edgeCount = rawData.edges.length;
      status.textContent =
        edgeCount + ' column edge(s) · ' +
        rawData.upstream.length + ' upstream · ' + rawData.downstream.length + ' downstream · ' +
        'drag to pan · scroll to zoom · click a node to open · F to fit · 0 to reset';
      if (edgeCount === 0) {
        status.textContent =
          'No column-level edges found for ' + focalModel +
          '. The model may have no typed columns or no upstream dependencies.';
      }
    }
  }

  // ── Initial render ────────────────────────────────────────────────────────────
  render(currentViewMode);

  // Update toggle button appearance
  function updateModeButtons() {
    document.getElementById('mode-model').classList.toggle('active', currentViewMode === 'model');
    document.getElementById('mode-column').classList.toggle('active', currentViewMode === 'column');
  }
  updateModeButtons();

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
