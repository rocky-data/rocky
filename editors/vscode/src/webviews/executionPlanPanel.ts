import * as vscode from "vscode";
import type { DagNodeOutput, DagOutput } from "../types/generated/dag";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/** Widest execution layer — the max number of models that run in parallel. */
export function maxParallelism(layers: string[][]): number {
  return layers.reduce((m, layer) => Math.max(m, layer.length), 0);
}

/** Render `counts_by_kind` as `5 transformation · 2 source`, sorted by kind. */
export function formatKindCounts(counts: Record<string, number>): string {
  return Object.entries(counts)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([kind, n]) => `${n} ${kind}`)
    .join(" · ");
}

/**
 * Human label for a DAG node id. Prefers the node's `label`; falls back to the
 * id with its `{kind}:` prefix stripped (e.g. `transformation:stg` → `stg`).
 */
export function nodeLabel(id: string, byId: Map<string, string>): string {
  return byId.get(id) ?? id.replace(/^[^:]+:/, "");
}

/** Open a webview rendering the execution plan from `rocky dag`. */
export function showExecutionPlan(result: DagOutput): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyExecutionPlan",
    "Rocky execution plan",
    vscode.ViewColumn.Beside,
    { enableScripts: false, retainContextWhenHidden: true },
  );
  panel.webview.html = render(panel.webview, result);
}

function render(webview: vscode.Webview, result: DagOutput): string {
  const nonce = makeNonce();
  const s = result.summary;
  const byId = new Map<string, string>(
    result.nodes.map((n: DagNodeOutput) => [n.id, n.label]),
  );

  const waves = result.execution_layers
    .map((layer, i) => {
      const chips = layer
        .map((id) => `<span class="chip">${escapeHtml(nodeLabel(id, byId))}</span>`)
        .join(" ");
      const parallel = layer.length > 1 ? ` <span class="muted">· ${layer.length} in parallel</span>` : "";
      return /* html */ `
        <div class="wave">
          <div class="wave-label">Wave ${i + 1}${parallel}</div>
          <div class="chips">${chips}</div>
        </div>`;
    })
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky execution plan", waveStyles())}
<body>
  <h1>Execution plan</h1>
  <p class="muted">Topological run schedule — models in the same wave have no mutual dependencies and run in parallel.</p>

  <div class="stat-grid">
    ${stat("Models", s.total_nodes)}
    ${stat("Waves", s.execution_layers)}
    ${stat("Edges", s.total_edges)}
    ${stat("Max parallel", maxParallelism(result.execution_layers))}
  </div>

  <p class="muted">${escapeHtml(formatKindCounts(s.counts_by_kind))}</p>

  ${waves || `<p class="muted">No models to schedule.</p>`}
</body>
</html>`;
}

function stat(label: string, value: number): string {
  return /* html */ `
    <div class="stat">
      <div class="label">${escapeHtml(label)}</div>
      <div class="value">${value}</div>
    </div>`;
}

function waveStyles(): string {
  return `
    .wave { display:flex; gap:12px; align-items:baseline; padding:8px 0; border-bottom:1px solid var(--vscode-panel-border); }
    .wave-label { min-width:140px; font-weight:600; }
    .chips { display:flex; flex-wrap:wrap; gap:6px; }
    .chip { display:inline-block; padding:2px 8px; border-radius:4px; background:var(--vscode-editorWidget-background); border:1px solid var(--vscode-panel-border); }
  `;
}
