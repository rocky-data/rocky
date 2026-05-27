import type { DriftData } from "../../../../src/webviews/lineage/contract";
import type { LineageOverlay } from "./types";

/**
 * Build a drift overlay from fetched drift actions. Matches an action to a node
 * by fully-qualified name, falling back to the bare model name.
 */
export function makeDriftOverlay(drift: DriftData): LineageOverlay {
  const byTable = new Map(drift.actions.map((a) => [a.table, a]));
  return {
    id: "drift",
    decorate(node) {
      const action = byTable.get(node.data.fqn) ?? byTable.get(node.id);
      if (!action) return undefined;
      const fullRefresh = /full|refresh/i.test(action.action);
      return [
        {
          text: action.action,
          color: fullRefresh
            ? "var(--vscode-charts-red)"
            : "var(--vscode-charts-orange)",
          title: action.reason,
        },
      ];
    },
    legend() {
      return [
        { text: "schema ALTER", color: "var(--vscode-charts-orange)" },
        { text: "full refresh", color: "var(--vscode-charts-red)" },
      ];
    },
  };
}
