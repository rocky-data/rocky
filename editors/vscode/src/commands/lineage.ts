import * as path from "path";
import * as vscode from "vscode";
import { getWorkspaceFolder } from "../config";
import { getExtensionUri } from "../extensionState";
import { runRocky } from "../rockyCli";

export async function showLineage(): Promise<void> {
  const editor = vscode.window.activeTextEditor;
  if (!editor) {
    vscode.window.showWarningMessage("Open a Rocky model file first.");
    return;
  }

  const modelName = path
    .basename(editor.document.fileName)
    .replace(/\.(rocky|sql)$/, "");
  if (!modelName) return;

  const workspaceFolder = getWorkspaceFolder();
  const extensionUri = getExtensionUri();
  const mediaUri = vscode.Uri.joinPath(extensionUri, "media");

  const panel = vscode.window.createWebviewPanel(
    "rockyLineage",
    `Lineage: ${modelName}`,
    vscode.ViewColumn.Beside,
    {
      enableScripts: true,
      localResourceRoots: [mediaUri],
      retainContextWhenHidden: true,
    },
  );

  panel.webview.html = renderLoadingHtml(panel.webview, modelName);

  const args = ["lineage", modelName, "--format", "dot"];
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
    #graph svg text { fill: var(--vscode-foreground); }
    #graph svg polygon { fill: var(--vscode-editor-background); stroke: var(--vscode-foreground); }
    #graph svg ellipse { fill: var(--vscode-badge-background); stroke: var(--vscode-foreground); }
    #graph svg path { stroke: var(--vscode-foreground); }
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
    <button id="zoom-out" title="Zoom out">−</button>
    <button id="zoom-reset" title="Reset zoom">100%</button>
    <button id="zoom-in" title="Zoom in">+</button>
    <button id="export-svg" title="Export as SVG">Export SVG</button>
  </header>
  <div id="viewport">
    <div id="graph"></div>
  </div>
  <div id="status" class="status">Rendering…</div>

  <script id="dot-source" type="application/json" nonce="${nonce}">${escapeJsonForScript(dotJson)}</script>
  <script src="${vizUri}" nonce="${nonce}"></script>
  <script nonce="${nonce}">
    (function () {
      const dot = JSON.parse(document.getElementById('dot-source').textContent);
      const graph = document.getElementById('graph');
      const viewport = document.getElementById('viewport');
      const status = document.getElementById('status');

      let scale = 1;
      let panX = 0;
      let panY = 0;
      let panning = false;
      let panOriginX = 0;
      let panOriginY = 0;

      function applyTransform() {
        graph.style.transform = 'translate(' + panX + 'px,' + panY + 'px) scale(' + scale + ')';
      }

      function setZoom(next) {
        scale = Math.min(8, Math.max(0.1, next));
        document.getElementById('zoom-reset').textContent = Math.round(scale * 100) + '%';
        applyTransform();
      }

      document.getElementById('zoom-in').onclick = () => setZoom(scale * 1.2);
      document.getElementById('zoom-out').onclick = () => setZoom(scale / 1.2);
      document.getElementById('zoom-reset').onclick = () => {
        scale = 1; panX = 0; panY = 0;
        setZoom(1);
      };

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
        panning = false;
        viewport.classList.remove('panning');
      });

      document.getElementById('export-svg').onclick = () => {
        const svg = graph.querySelector('svg');
        if (!svg) return;
        const blob = new Blob([svg.outerHTML], { type: 'image/svg+xml' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = '${escapeJsForString(modelName)}-lineage.svg';
        a.click();
        URL.revokeObjectURL(url);
      };

      Viz.instance().then((viz) => {
        try {
          const svg = viz.renderSVGElement(dot);
          graph.innerHTML = '';
          graph.appendChild(svg);
          status.textContent = 'Drag to pan · scroll to zoom';
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

function escapeJsForString(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'")
    .replace(/\n/g, "\\n");
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
