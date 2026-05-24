import * as vscode from "vscode";
import type { TraceModelEntry, TraceOutput } from "../types/generated/trace";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/** Human-readable duration: `340ms`, `1.2s`, `1m 5s`. */
export function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.floor((ms % 60_000) / 1000);
  return `${m}m ${s}s`;
}

/**
 * Left offset + width (percent of the run's total duration) for a model's
 * gantt bar. Width is floored at 0.5% so sub-millisecond models stay visible,
 * and clamped so the bar never overflows the track. Pure — unit-tested.
 */
export function barGeometry(
  startOffsetMs: number,
  durationMs: number,
  totalMs: number,
): { leftPct: number; widthPct: number } {
  if (totalMs <= 0) return { leftPct: 0, widthPct: 100 };
  const leftPct = Math.max(0, Math.min(100, (startOffsetMs / totalMs) * 100));
  const widthPct = Math.max(0.5, Math.min(100 - leftPct, (durationMs / totalMs) * 100));
  return { leftPct, widthPct };
}

/** `failed`/`error` statuses render in the fail color; everything else ok. */
function statusClass(status: string): "ok" | "fail" {
  return /fail|error/i.test(status) ? "fail" : "ok";
}

/** Open a webview rendering the run timeline from `rocky trace`. */
export function showRunTrace(result: TraceOutput): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyRunTrace",
    `Rocky run trace`,
    vscode.ViewColumn.Beside,
    { enableScripts: false, retainContextWhenHidden: true },
  );
  panel.webview.html = render(panel.webview, result);
}

function render(webview: vscode.Webview, result: TraceOutput): string {
  const nonce = makeNonce();
  const total = result.run_duration_ms;
  const verdict = statusClass(result.status);

  const rows = [...result.models]
    .sort((a, b) => a.start_offset_ms - b.start_offset_ms)
    .map((m: TraceModelEntry) => {
      const { leftPct, widthPct } = barGeometry(m.start_offset_ms, m.duration_ms, total);
      const cls = statusClass(m.status);
      const lane = typeof m.lane === "number" ? `L${m.lane}` : "";
      return /* html */ `
      <tr>
        <td>${escapeHtml(m.model_name)}</td>
        <td class="muted">${escapeHtml(lane)}</td>
        <td>${escapeHtml(formatMs(m.duration_ms))}</td>
        <td class="track">
          <div class="bar ${cls}" style="margin-left:${leftPct.toFixed(2)}%;width:${widthPct.toFixed(2)}%" title="${escapeHtml(m.status)} · ${escapeHtml(formatMs(m.duration_ms))}"></div>
        </td>
      </tr>`;
    })
    .join("");

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky run trace", traceStyles())}
<body>
  <h1>Run trace <span class="badge ${verdict}">${escapeHtml(result.status)}</span></h1>
  <p class="muted">Run <code>${escapeHtml(result.run_id)}</code> · ${escapeHtml(result.trigger)} · started ${escapeHtml(result.started_at)}</p>

  <div class="stat-grid">
    ${stat("Duration", formatMs(total))}
    ${stat("Models", result.models.length)}
    ${stat("Lanes", result.lane_count)}
  </div>

  ${
    rows
      ? `<table>
      <thead><tr><th>Model</th><th>Lane</th><th>Duration</th><th>Timeline</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`
      : `<p class="muted">No model executions recorded for this run.</p>`
  }
</body>
</html>`;
}

function stat(label: string, value: string | number): string {
  return /* html */ `
    <div class="stat">
      <div class="label">${escapeHtml(label)}</div>
      <div class="value">${escapeHtml(String(value))}</div>
    </div>`;
}

function traceStyles(): string {
  return `
    td.track { width: 50%; min-width: 200px; }
    td.track .bar { height: 14px; border-radius: 3px; min-width: 2px; }
    .bar.ok { background: var(--vscode-testing-iconPassed); }
    .bar.fail { background: var(--vscode-testing-iconFailed); }
  `;
}
