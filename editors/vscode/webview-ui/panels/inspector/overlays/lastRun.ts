import type { ReplayData } from "../../../../src/webviews/lineage/contract";
import type { LineageOverlay } from "./types";

function compactRows(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
  return `${(n / 1_000_000).toFixed(1)}M`;
}

/**
 * Build a last-run overlay: badge each model with the rows it wrote and its
 * status in the most recent run, matched by name or fqn.
 */
export function makeLastRunOverlay(data: ReplayData): LineageOverlay {
  const byModel = new Map(data.models.map((m) => [m.model, m]));
  return {
    id: "lastRun",
    decorate(node) {
      const run = byModel.get(node.id) ?? byModel.get(node.data.fqn);
      if (!run) return undefined;
      const failed = /fail|error/i.test(run.status);
      const text =
        run.rows != null ? `${compactRows(run.rows)} rows` : run.status;
      return [
        {
          text,
          color: failed
            ? "var(--vscode-charts-red)"
            : "var(--vscode-charts-green)",
          title: `last run: ${run.status}`,
        },
      ];
    },
    legend() {
      return [
        { text: "last run ok", color: "var(--vscode-charts-green)" },
        { text: "last run failed", color: "var(--vscode-charts-red)" },
      ];
    },
  };
}
