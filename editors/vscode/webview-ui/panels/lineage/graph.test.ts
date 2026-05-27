import { describe, expect, it } from "vitest";
import type {
  GraphEdge,
  GraphNode,
} from "../../../src/webviews/lineage/contract";
import { downstreamOf, matchesSearch, neighborhood, upstreamOf } from "./graph";

// raw → stg → mart → report
const edges: GraphEdge[] = [
  { source: "raw", target: "stg" },
  { source: "stg", target: "mart" },
  { source: "mart", target: "report" },
];

describe("graph traversal", () => {
  it("walks upstream transitively", () => {
    expect(upstreamOf(edges, "mart")).toEqual(new Set(["stg", "raw"]));
  });

  it("walks downstream transitively", () => {
    expect(downstreamOf(edges, "stg")).toEqual(new Set(["mart", "report"]));
  });

  it("includes the focal model in its neighborhood", () => {
    expect(neighborhood(edges, "stg")).toEqual(
      new Set(["stg", "raw", "mart", "report"]),
    );
  });

  it("terminates on cycles", () => {
    const cyclic: GraphEdge[] = [
      { source: "a", target: "b" },
      { source: "b", target: "a" },
    ];
    expect(upstreamOf(cyclic, "a")).toEqual(new Set(["b", "a"]));
  });
});

describe("search matching", () => {
  const node = (label: string): GraphNode => ({
    id: label,
    label,
    kind: "Model",
    materialization: null,
    fqn: label,
    costHint: null,
    freshness: null,
  });

  it("matches substrings case-insensitively", () => {
    expect(matchesSearch(node("stg_orders"), "ORD")).toBe(true);
    expect(matchesSearch(node("stg_orders"), "xyz")).toBe(false);
  });

  it("matches a /regex/ query", () => {
    expect(matchesSearch(node("stg_orders"), "/^stg_/")).toBe(true);
    expect(matchesSearch(node("mart_x"), "/^stg_/")).toBe(false);
  });

  it("treats a blank query as match-all", () => {
    expect(matchesSearch(node("anything"), "  ")).toBe(true);
  });
});
