import * as vscode from "vscode";
import { resolveProjectRoot } from "../config";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { ApplyOutput } from "../types/generated/apply";
import type {
  BreakingChange,
  BreakingFinding,
  ReviewOutput,
} from "../types/generated/review";
import { buildHead, escapeHtml, makeNonce } from "./htmlUtil";

/** One-line description of a breaking change, keyed off its `kind`. Pure. */
export function formatBreakingChange(c: BreakingChange): string {
  const at = `${c.model}${"column" in c && c.column ? `.${String(c.column)}` : ""}`;
  switch (c.kind) {
    case "model_removed":
      return `model removed: ${c.model}`;
    case "model_added":
      return `model added: ${c.model}`;
    case "column_dropped":
      return `column dropped: ${at} (${String((c as { data_type?: string }).data_type ?? "?")})`;
    case "column_added":
      return `column added: ${at}`;
    case "column_type_changed":
      return `type changed: ${at} ${String((c as { old_type?: string }).old_type)} → ${String((c as { new_type?: string }).new_type)}${(c as { narrowing?: boolean }).narrowing ? " (narrowing)" : ""}`;
    case "column_nullability_changed":
      return `nullability changed: ${at}`;
    case "column_reordered":
      return `column reordered: ${at}`;
    default:
      return `${String(c.kind)}: ${at}`;
  }
}

/**
 * Verdict for the review header. `approved` once the human marker is written;
 * otherwise `blocked` when breaking changes are present, or `clean` when none.
 * Pure — unit-tested.
 */
export function reviewVerdict(r: ReviewOutput): "approved" | "blocked" | "clean" {
  if (r.approved) return "approved";
  return (r.breaking_changes?.length ?? 0) > 0 ? "blocked" : "clean";
}

/** Apply is only allowed once the plan has been reviewed + approved. */
export function canApply(r: ReviewOutput): boolean {
  return r.approved;
}

/**
 * Open an interactive review panel for `planId`. Renders the breaking-change
 * report and wires the Approve (`rocky review --approve`) and Apply
 * (`rocky apply`) buttons — Apply stays disabled until the plan is approved
 * (the engine refuses an unreviewed apply anyway; this is the matching UI).
 */
export function openReviewPanel(planId: string, review: ReviewOutput): void {
  const panel = vscode.window.createWebviewPanel(
    "rockyReview",
    `Review ${planId.slice(0, 8)}`,
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: true },
  );

  let current = review;
  const rerender = (): void => {
    panel.webview.html = render(panel.webview, planId, current);
  };
  rerender();

  panel.webview.onDidReceiveMessage(async (msg: { type?: string }) => {
    const cwd = resolveProjectRoot();
    if (msg.type === "approve") {
      try {
        current = await runRockyJsonWithProgress<ReviewOutput>(
          `Approving plan ${planId.slice(0, 8)}…`,
          ["review", planId, "--approve", "--output", "json"],
          { cwd },
        );
        rerender();
        void vscode.window.showInformationMessage(
          `Approved plan ${planId.slice(0, 8)} — Apply is now enabled.`,
        );
      } catch (err) {
        showRockyError("Approve failed", err);
      }
    } else if (msg.type === "apply") {
      try {
        const result = await runRockyJsonWithProgress<ApplyOutput>(
          `Applying plan ${planId.slice(0, 8)}…`,
          ["apply", planId, "--output", "json"],
          { cwd },
        );
        if (result.success) {
          void vscode.window.showInformationMessage(
            `Applied plan ${planId.slice(0, 8)} (${result.plan_kind}).`,
          );
        } else {
          void vscode.window.showErrorMessage(
            `Apply failed for plan ${planId.slice(0, 8)}.`,
          );
        }
      } catch (err) {
        showRockyError("Apply failed", err);
      }
    }
  });
}

function render(
  webview: vscode.Webview,
  planId: string,
  r: ReviewOutput,
): string {
  const nonce = makeNonce();
  const verdict = reviewVerdict(r);
  const findings = r.breaking_changes ?? [];

  const rows = findings
    .map(
      (f: BreakingFinding) => `
      <tr>
        <td><code>${escapeHtml(String(f.severity))}</code></td>
        <td>${escapeHtml(formatBreakingChange(f.change))}</td>
      </tr>`,
    )
    .join("");

  const applyAttrs = canApply(r) ? "" : "disabled";
  const approveHidden = r.approved ? "hidden" : "";

  return /* html */ `<!DOCTYPE html>
<html lang="en">
${buildHead(webview, nonce, "Rocky review")}
<body>
  <h1>Plan review <span class="badge ${verdict === "approved" ? "ok" : verdict === "blocked" ? "warn" : "ok"}">${verdict}</span></h1>
  <p class="muted">Plan <code>${escapeHtml(planId)}</code> · diffed vs <code>${escapeHtml(r.base_ref)}</code></p>

  <div id="toolbar" style="margin: 12px 0; display:flex; gap:8px;">
    <button id="approve" ${approveHidden}>Approve</button>
    <button id="apply" ${applyAttrs}>Apply</button>
  </div>
  <p class="muted">${r.approved ? "Approved — Apply will execute the plan." : "Approve records your sign-off (writes the review marker) and unblocks Apply."}</p>

  ${
    findings.length > 0
      ? `<h2>Breaking changes (${findings.length})</h2>
    <table>
      <thead><tr><th>Severity</th><th>Change</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`
      : `<p class="badge ok">No breaking changes detected vs ${escapeHtml(r.base_ref)}.</p>`
  }

  <script nonce="${nonce}">
    const vscode = acquireVsCodeApi();
    const approve = document.getElementById("approve");
    const apply = document.getElementById("apply");
    if (approve) approve.addEventListener("click", () => vscode.postMessage({ type: "approve" }));
    if (apply) apply.addEventListener("click", () => vscode.postMessage({ type: "apply" }));
  </script>
</body>
</html>`;
}
