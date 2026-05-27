import type { LineageOverlay } from "./types";

function compactLag(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

/** Badges models that declare a freshness policy with their max lag. */
export const freshnessOverlay: LineageOverlay = {
  id: "freshness",
  decorate(node) {
    const freshness = node.data.freshness;
    if (!freshness) return undefined;
    return [
      {
        text: `≤ ${compactLag(freshness.max_lag_seconds)}`,
        color: "var(--vscode-charts-blue)",
        title: `freshness policy${freshness.severity ? ` (${freshness.severity})` : ""}`,
      },
    ];
  },
  legend() {
    return [{ text: "freshness policy", color: "var(--vscode-charts-blue)" }];
  },
};
