import type {
  BreakingData,
  GraphData,
} from "../../../../src/webviews/lineage/contract";
import { downstreamOf } from "../graph";
import type { LineageOverlay } from "./types";

const SEVERITY_RANK: Record<string, number> = {
  info: 1,
  warning: 2,
  breaking: 3,
};
const SEVERITY_COLOR: Record<string, string> = {
  breaking: "var(--vscode-charts-red)",
  warning: "var(--vscode-charts-orange)",
  info: "var(--vscode-charts-blue)",
};

/**
 * Build a breaking-change overlay: each changed model gets a severity badge,
 * and its downstream consumers get an "impacted" badge. A finding's model (bare
 * name or fqn) is resolved against the graph's node set before flood-filling.
 */
export function makeBreakingOverlay(
  data: BreakingData,
  graph: GraphData,
): LineageOverlay {
  const ids = new Set(graph.nodes.map((n) => n.id));
  const idByFqn = new Map(graph.nodes.map((n) => [n.fqn, n.id]));
  const resolve = (model: string): string | undefined =>
    ids.has(model) ? model : idByFqn.get(model);

  const changed = new Map<string, string>();
  for (const finding of data.findings) {
    const id = resolve(finding.model);
    if (!id) continue;
    const current = changed.get(id);
    if (
      !current ||
      (SEVERITY_RANK[finding.severity] ?? 0) > (SEVERITY_RANK[current] ?? 0)
    ) {
      changed.set(id, finding.severity);
    }
  }

  const impacted = new Set<string>();
  for (const id of changed.keys()) {
    for (const down of downstreamOf(graph.edges, id)) {
      if (!changed.has(down)) impacted.add(down);
    }
  }

  return {
    id: "breaking",
    decorate(node) {
      const severity = changed.get(node.id);
      if (severity) {
        return [
          {
            text: severity,
            color: SEVERITY_COLOR[severity] ?? "var(--vscode-charts-red)",
            title: `breaking-change finding (${severity})`,
          },
        ];
      }
      if (impacted.has(node.id)) {
        return [
          {
            text: "impacted",
            color: "var(--vscode-charts-purple)",
            title: "downstream of a breaking change",
          },
        ];
      }
      return undefined;
    },
    legend() {
      return [
        { text: "breaking", color: SEVERITY_COLOR.breaking },
        { text: "warning", color: SEVERITY_COLOR.warning },
        { text: "impacted", color: "var(--vscode-charts-purple)" },
      ];
    },
  };
}
