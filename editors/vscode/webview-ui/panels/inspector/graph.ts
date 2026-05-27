import type {
  GraphEdge,
  GraphNode,
} from "../../../src/webviews/lineage/contract";

/** Models reachable upstream of `model` (its transitive producers). */
export function upstreamOf(edges: GraphEdge[], model: string): Set<string> {
  return walk(edges, model, "up");
}

/** Models reachable downstream of `model` (its transitive consumers). */
export function downstreamOf(edges: GraphEdge[], model: string): Set<string> {
  return walk(edges, model, "down");
}

/** `model` plus everything upstream and downstream of it. */
export function neighborhood(edges: GraphEdge[], model: string): Set<string> {
  const set = new Set<string>([model]);
  for (const up of upstreamOf(edges, model)) set.add(up);
  for (const down of downstreamOf(edges, model)) set.add(down);
  return set;
}

function walk(edges: GraphEdge[], start: string, dir: "up" | "down"): Set<string> {
  const seen = new Set<string>();
  const queue = [start];
  while (queue.length > 0) {
    const current = queue.shift() as string;
    for (const edge of edges) {
      const from = dir === "up" ? edge.target : edge.source;
      const to = dir === "up" ? edge.source : edge.target;
      if (from === current && !seen.has(to)) {
        seen.add(to);
        queue.push(to);
      }
    }
  }
  return seen;
}

/**
 * Whether `label` matches a search query. A query wrapped in slashes (`/foo/`)
 * is treated as a case-insensitive regex; otherwise it's a substring match.
 * An empty query matches everything.
 */
export function labelMatches(label: string, query: string): boolean {
  const q = query.trim();
  if (!q) return true;
  if (q.length > 2 && q.startsWith("/") && q.endsWith("/")) {
    try {
      return new RegExp(q.slice(1, -1), "i").test(label);
    } catch {
      return false;
    }
  }
  return label.toLowerCase().includes(q.toLowerCase());
}

/** {@link labelMatches} against a node's label. */
export function matchesSearch(node: GraphNode, query: string): boolean {
  return labelMatches(node.label, query);
}
