import * as path from "path";
import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";
import { getExtensionUri } from "../extensionState";
import { runRocky } from "../rockyCli";
import { resolveModelName } from "./ui";

const VIEW_TYPE = "rockyLineage";

interface SerializedState {
  modelName?: string;
  scale?: number;
  panX?: number;
  panY?: number;
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
 * reloads. The webview persists `{ modelName, scale, panX, panY }` via
 * `vscode.setState`; on reload we re-run the CLI and restore zoom/pan.
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

  // `-o table` opts out of the CLI's default `--output json`, so the lineage
  // handler honours `--format dot`. Without this, stdout is a JSON blob and
  // viz.js rejects the leading `{` as invalid DOT.
  const args = ["-o", "table", "lineage", modelName, "--format", "dot"];
  if (workspaceFolder) {
    args.unshift("--config", `${workspaceFolder}/rocky.toml`);
  }

  try {
    const { stdout } = await runRocky(args, { cwd: workspaceFolder });
    panel.webview.html = renderLineageHtml(
      panel.webview,
      mediaUri,
      modelName,
      stdout.trim(),
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
  dot: string,
): string {
  const vizUri = webview.asWebviewUri(
    vscode.Uri.joinPath(mediaUri, "viz.js"),
  );
  const nonce = makeNonce();
  // Embed DOT as a JSON string so it survives quoting; the script reads it
  // from a hidden <script type="application/json"> tag rather than via string
  // interpolation, which keeps the CSP simple.
  const dotJson = JSON.stringify(dot);
  const modelNameJson = JSON.stringify(modelName);

  return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="Content-Security-Policy"
        content="default-src 'none';
                 style-src ${webview.cspSource} 'unsafe-inline';
                 script-src ${webview.cspSource} 'nonce-${nonce}' 'wasm-unsafe-eval';
                 img-src ${webview.cspSource} data:;" />
  <title>Lineage: ${escapeHtml(modelName)}</title>
  <style nonce="${nonce}">
    body {
      margin: 0;
      font-family: var(--vscode-font-family);
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
    }
    header h2 { margin: 0; font-size: 13px; font-weight: 600; }
    header .spacer { flex: 1; }
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
    #viewport {
      flex: 1; overflow: hidden; position: relative;
      cursor: grab;
    }
    #viewport.panning { cursor: grabbing; }
    #graph {
      transform-origin: 0 0;
      will-change: transform;
    }
    #graph svg { display: block; }
    #graph svg text { fill: var(--vscode-foreground); pointer-events: none; }
    #graph svg g.node { cursor: pointer; }
    #graph svg g.node ellipse,
    #graph svg g.node polygon {
      stroke: var(--vscode-foreground);
      fill: var(--vscode-badge-background);
      transition: fill 80ms ease, stroke-width 80ms ease;
    }
    #graph svg g.node.source ellipse,
    #graph svg g.node.source polygon {
      fill: var(--vscode-charts-blue, var(--vscode-badge-background));
    }
    #graph svg g.node.leaf ellipse,
    #graph svg g.node.leaf polygon {
      fill: var(--vscode-charts-green, var(--vscode-badge-background));
    }
    #graph svg g.node:hover ellipse,
    #graph svg g.node:hover polygon {
      stroke-width: 2px;
    }
    #graph svg g.edge path {
      stroke: var(--vscode-descriptionForeground);
      fill: none;
    }
    #graph svg g.edge polygon {
      fill: var(--vscode-descriptionForeground);
      stroke: var(--vscode-descriptionForeground);
    }
    .status {
      padding: 8px 12px;
      color: var(--vscode-descriptionForeground);
      font-size: 12px;
    }
    .error { color: var(--vscode-errorForeground); }
  </style>
</head>
<body>
  <header>
    <h2>Lineage: ${escapeHtml(modelName)}</h2>
    <span class="spacer"></span>
    <button id="zoom-out" title="Zoom out (-)">−</button>
    <button id="zoom-reset" title="Reset zoom (0)">100%</button>
    <button id="zoom-in" title="Zoom in (+)">+</button>
    <button id="zoom-fit" title="Fit to view (F)">Fit</button>
    <button id="export-svg" title="Export as SVG">Export SVG</button>
  </header>
  <div id="viewport">
    <div id="graph"></div>
  </div>
  <div id="status" class="status">Rendering…</div>

  <script id="dot-source" type="application/json" nonce="${nonce}">${escapeJsonForScript(dotJson)}</script>
  <script id="model-name" type="application/json" nonce="${nonce}">${escapeJsonForScript(modelNameJson)}</script>
  <script src="${vizUri}" nonce="${nonce}"></script>
  <script nonce="${nonce}">
    (function () {
      const vscode = acquireVsCodeApi();
      const dot = JSON.parse(document.getElementById('dot-source').textContent);
      const modelName = JSON.parse(document.getElementById('model-name').textContent);
      const graph = document.getElementById('graph');
      const viewport = document.getElementById('viewport');
      const status = document.getElementById('status');
      const saved = vscode.getState() || {};

      let scale = typeof saved.scale === 'number' ? saved.scale : 1;
      let panX = typeof saved.panX === 'number' ? saved.panX : 0;
      let panY = typeof saved.panY === 'number' ? saved.panY : 0;
      let panning = false;
      let panOriginX = 0;
      let panOriginY = 0;

      function saveState() {
        vscode.setState({ modelName: modelName, scale: scale, panX: panX, panY: panY });
      }

      function applyTransform() {
        graph.style.transform = 'translate(' + panX + 'px,' + panY + 'px) scale(' + scale + ')';
        document.getElementById('zoom-reset').textContent = Math.round(scale * 100) + '%';
      }

      function setZoom(next) {
        scale = Math.min(8, Math.max(0.1, next));
        applyTransform();
        saveState();
      }

      function fitToView() {
        const svg = graph.querySelector('svg');
        if (!svg) return;
        const bbox = svg.getBBox();
        if (!bbox.width || !bbox.height) return;
        const margin = 0.9;
        const sx = (viewport.clientWidth / bbox.width) * margin;
        const sy = (viewport.clientHeight / bbox.height) * margin;
        scale = Math.min(8, Math.max(0.1, Math.min(sx, sy)));
        panX = (viewport.clientWidth - bbox.width * scale) / 2 - bbox.x * scale;
        panY = (viewport.clientHeight - bbox.height * scale) / 2 - bbox.y * scale;
        applyTransform();
        saveState();
      }

      function resetView() {
        scale = 1; panX = 0; panY = 0;
        applyTransform();
        saveState();
      }

      document.getElementById('zoom-in').onclick = () => setZoom(scale * 1.2);
      document.getElementById('zoom-out').onclick = () => setZoom(scale / 1.2);
      document.getElementById('zoom-reset').onclick = resetView;
      document.getElementById('zoom-fit').onclick = fitToView;

      window.addEventListener('keydown', (e) => {
        if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
        if (e.key === '+' || e.key === '=') { setZoom(scale * 1.2); e.preventDefault(); }
        else if (e.key === '-') { setZoom(scale / 1.2); e.preventDefault(); }
        else if (e.key === '0') { resetView(); e.preventDefault(); }
        else if (e.key === 'f' || e.key === 'F') { fitToView(); e.preventDefault(); }
      });

      viewport.addEventListener('wheel', (e) => {
        e.preventDefault();
        const delta = -e.deltaY * 0.001;
        setZoom(scale * (1 + delta));
      }, { passive: false });

      viewport.addEventListener('mousedown', (e) => {
        panning = true;
        viewport.classList.add('panning');
        panOriginX = e.clientX - panX;
        panOriginY = e.clientY - panY;
      });
      window.addEventListener('mousemove', (e) => {
        if (!panning) return;
        panX = e.clientX - panOriginX;
        panY = e.clientY - panOriginY;
        applyTransform();
      });
      window.addEventListener('mouseup', () => {
        if (!panning) return;
        panning = false;
        viewport.classList.remove('panning');
        saveState();
      });

      document.getElementById('export-svg').onclick = () => {
        const svg = graph.querySelector('svg');
        if (!svg) return;
        const blob = new Blob([svg.outerHTML], { type: 'image/svg+xml' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = modelName + '-lineage.svg';
        a.click();
        URL.revokeObjectURL(url);
      };

      // Classify nodes by in/out degree, parsed from DOT before viz.js renders.
      // Rocky's lineage DOT emits edges as \`"model.column" -> "model.column";\`,
      // so we match any quoted string on each side of -> and key the in/out
      // sets by the exact node label.
      function classifyNodes(dotText) {
        const incoming = new Set();
        const outgoing = new Set();
        const edgeRe = /"([^"]+)"\\s*->\\s*"([^"]+)"/g;
        let m;
        while ((m = edgeRe.exec(dotText)) !== null) {
          outgoing.add(m[1]);
          incoming.add(m[2]);
        }
        const all = new Set([...incoming, ...outgoing]);
        const sources = new Set([...all].filter(n => !incoming.has(n)));
        const leaves = new Set([...all].filter(n => !outgoing.has(n)));
        return { sources: sources, leaves: leaves };
      }

      // Node labels are \`model.column\`. The column is always the last
      // dot-separated segment; the rest is the model name we want to open.
      function modelFromLabel(label) {
        const idx = label.lastIndexOf('.');
        return idx > 0 ? label.slice(0, idx) : label;
      }

      function decorateNodes(svgRoot, classes) {
        svgRoot.querySelectorAll('g.node').forEach(nodeEl => {
          const textEl = nodeEl.querySelector('text');
          if (!textEl) return;
          const label = (textEl.textContent || '').trim();
          if (!label) return;
          if (classes.sources.has(label)) nodeEl.classList.add('source');
          else if (classes.leaves.has(label)) nodeEl.classList.add('leaf');

          const model = modelFromLabel(label);

          // Native browser tooltip on hover — no extra DOM, no positioning.
          const titleEl = document.createElementNS('http://www.w3.org/2000/svg', 'title');
          titleEl.textContent = 'Open ' + model;
          nodeEl.appendChild(titleEl);

          nodeEl.addEventListener('click', (e) => {
            e.stopPropagation();
            vscode.postMessage({ type: 'openModel', name: model });
          });
        });
      }

      Viz.instance().then((viz) => {
        try {
          const svg = viz.renderSVGElement(dot);
          graph.innerHTML = '';
          graph.appendChild(svg);
          const classes = classifyNodes(dot);
          decorateNodes(svg, classes);
          status.textContent = 'Drag to pan · scroll to zoom · click a node to open · F to fit · 0 to reset';
          if (saved.scale || saved.panX || saved.panY) {
            applyTransform();
          } else {
            // Auto-fit on first render so wide DAGs are immediately legible.
            fitToView();
          }
        } catch (err) {
          status.textContent = 'Render error: ' + err.message;
          status.classList.add('error');
        }
      }).catch((err) => {
        status.textContent = 'viz.js failed to load: ' + err.message;
        status.classList.add('error');
      });
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
