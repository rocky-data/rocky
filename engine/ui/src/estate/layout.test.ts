import { describe, expect, it } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import dagFixture from "@rocky-fixtures/dag.json";
import { NODE_HEIGHT, NODE_WIDTH, layeredFlow } from "./layout";

function dag(overrides: Partial<DagOutput>): DagOutput {
  return {
    version: "1.74.0",
    command: "dag",
    nodes: [],
    edges: [],
    execution_layers: [],
    summary: { counts_by_kind: {}, execution_layers: 0, total_edges: 0, total_nodes: 0 },
    ...overrides,
  } as DagOutput;
}

const node = (id: string, kind = "Model") => ({ id, label: id, kind });

describe("layeredFlow", () => {
  it("places every node in its layer's column and its row within the layer", () => {
    const flow = layeredFlow(
      dag({
        nodes: [node("a"), node("b"), node("c")],
        edges: [
          { from: "a", to: "b", edge_type: "depends_on" },
          { from: "a", to: "c", edge_type: "depends_on" },
        ],
        execution_layers: [["a"], ["b", "c"]],
      }),
    );
    const byId = new Map(flow.nodes.map((n) => [n.id, n]));
    expect(byId.get("a")?.position.x).toBe(0);
    expect(byId.get("b")?.position.x).toBeGreaterThan(NODE_WIDTH);
    expect(byId.get("c")?.position.x).toBe(byId.get("b")?.position.x);
    expect((byId.get("c")?.position.y ?? 0) - (byId.get("b")?.position.y ?? 0)).toBeGreaterThan(
      NODE_HEIGHT,
    );
    expect(byId.get("a")?.data.layer).toBe(0);
    expect(byId.get("c")?.data.layer).toBe(1);
    expect(flow.edges.map((e) => e.id)).toEqual(["a->b", "a->c"]);
    expect(flow.dropped).toBe(0);
  });

  it("drops an edge whose endpoint is not a node, and counts it", () => {
    const flow = layeredFlow(
      dag({
        nodes: [node("a")],
        edges: [{ from: "a", to: "ghost", edge_type: "depends_on" }],
        execution_layers: [["a"]],
      }),
    );
    expect(flow.edges).toEqual([]);
    expect(flow.dropped).toBe(1);
  });

  it("shows a node the layers never mention, in a trailing column", () => {
    const flow = layeredFlow(
      dag({
        nodes: [node("a"), node("stray")],
        edges: [],
        execution_layers: [["a"]],
      }),
    );
    const stray = flow.nodes.find((n) => n.id === "stray");
    expect(stray?.data.layer).toBe(1);
    expect(stray?.position.x).toBeGreaterThan(0);
  });

  it("carries the strategy, target and pipeline as text, or null when absent", () => {
    const flow = layeredFlow(
      dag({
        nodes: [
          {
            ...node("orders"),
            strategy: { type: "merge", unique_key: ["id"] },
            target: { catalog: "c", schema: "s", table: "orders" },
            pipeline: "core",
          } as DagOutput["nodes"][number],
          node("bare"),
        ],
        execution_layers: [["orders", "bare"]],
      }),
    );
    const orders = flow.nodes.find((n) => n.id === "orders")?.data;
    expect(orders?.strategy).toBe("merge");
    expect(orders?.target).toBe("c.s.orders");
    expect(orders?.pipeline).toBe("core");
    const bare = flow.nodes.find((n) => n.id === "bare")?.data;
    expect(bare?.strategy).toBeNull();
    expect(bare?.target).toBeNull();
  });

  it("lays out the playground's captured DAG: one node per model, one edge per dependency", () => {
    const captured = dagFixture as unknown as DagOutput;
    const flow = layeredFlow(captured);
    expect(flow.nodes.map((n) => n.id).sort()).toEqual(captured.nodes.map((n) => n.id).sort());
    expect(flow.edges).toHaveLength(captured.edges.length);
    expect(flow.dropped).toBe(0);
    expect(new Set(flow.nodes.map((n) => n.data.layer)).size).toBe(
      captured.execution_layers.length,
    );
  });
});
