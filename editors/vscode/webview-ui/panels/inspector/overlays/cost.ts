import type { LineageOverlay } from "./types";

/** Badges each node with its heuristic compile-time cost estimate. */
export const costOverlay: LineageOverlay = {
  id: "cost",
  decorate(node) {
    const cost = node.data.costHint;
    if (!cost) return undefined;
    return [
      {
        text: `~$${cost.estimated_cost_usd.toFixed(3)}`,
        color: "var(--vscode-charts-yellow)",
        title: `≈ ${cost.estimated_rows.toLocaleString()} rows · ${cost.confidence} confidence (heuristic)`,
      },
    ];
  },
  legend() {
    return [
      { text: "est. cost / run", color: "var(--vscode-charts-yellow)" },
    ];
  },
};
