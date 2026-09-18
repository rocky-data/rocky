import { describe, expect, it } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import type { ModelListOutput } from "@rocky-types/model_list";
import dagFixture from "@rocky-fixtures/dag.json";
import mixedDag from "../test/fixtures/dag-mixed-kinds.json";
import twoPipelinesDag from "../test/fixtures/dag-two-pipelines.json";
import twoPipelinesModels from "../test/fixtures/model-list-two-pipelines.json";
import { NODE_HEIGHT, NODE_WIDTH, layeredFlow } from "./layout";
import { compiledModels } from "./nodeRoute";

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
      "unknown",
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
      "unknown",
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
      "unknown",
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
      "unknown",
    );
    const orders = flow.nodes.find((n) => n.id === "orders")?.data;
    expect(orders?.strategy).toBe("merge");
    expect(orders?.target).toBe("c.s.orders");
    expect(orders?.pipeline).toBe("core");
    const bare = flow.nodes.find((n) => n.id === "bare")?.data;
    expect(bare?.strategy).toBeNull();
    expect(bare?.target).toBeNull();
  });

  it("makes only a servable node focusable and selectable", () => {
    const flow = layeredFlow(
      dag({
        nodes: [
          { id: "transformation:orders", label: "orders", kind: "transformation" },
          { id: "source:ecommerce", label: "ecommerce (source)", kind: "source" },
        ] as DagOutput["nodes"],
        execution_layers: [["transformation:orders", "source:ecommerce"]],
      }),
      "unknown",
    );
    const model = flow.nodes.find((n) => n.id === "transformation:orders");
    expect(model?.focusable).toBe(true);
    expect(model?.selectable).toBe(true);
    expect(model?.ariaRole).toBe("button");
    expect(model?.domAttributes).toBeUndefined();

    const source = flow.nodes.find((n) => n.id === "source:ecommerce");
    expect(source?.focusable).toBe(false);
    expect(source?.selectable).toBe(false);
    expect(source?.ariaRole).toBeUndefined();
    expect(source?.domAttributes).toEqual({ "aria-disabled": true });
  });

  it("takes every kind but transformation out of the captured DAG's tab order", () => {
    const flow = layeredFlow(mixedDag as unknown as DagOutput, "unknown");
    const focusable = flow.nodes.filter((n) => n.focusable);
    expect(focusable.map((n) => n.id)).toEqual([
      "transformation:raw_orders",
      "transformation:customer_orders",
      "transformation:revenue_summary",
    ]);
    // Seven kinds in the fixture; six of them take no tab stop at all.
    expect(flow.nodes.filter((n) => !n.focusable)).toHaveLength(6);
    expect(flow.nodes.filter((n) => !n.focusable).every((n) => n.selectable === false)).toBe(true);
  });

  it("states each node's size, which is what the minimap needs", () => {
    // React Flow skips a node whose dimensions it does not know
    // (`nodeHasDimensions`), and it never learns them here: the graph is
    // controlled with no `onNodesChange`, so a measurement has nowhere to be
    // written back to. Measured before this: the minimap drew 0 of 3 nodes
    // while its viewBox spanned the correct bounds.
    const flow = layeredFlow(mixedDag as unknown as DagOutput, "unknown");
    expect(flow.nodes.length).toBeGreaterThan(0);
    for (const node of flow.nodes) {
      expect(node.width).toBe(NODE_WIDTH);
      expect(node.height).toBe(NODE_HEIGHT);
    }
  });

  it("takes a model the server did not compile out of the tab order", () => {
    const flow = layeredFlow(
      twoPipelinesDag as unknown as DagOutput,
      compiledModels(twoPipelinesModels as unknown as ModelListOutput),
    );
    const byId = new Map(flow.nodes.map((n) => [n.id, n]));

    const outside = byId.get("transformation:weekly_revenue");
    expect(outside?.focusable).toBe(false);
    expect(outside?.selectable).toBe(false);
    expect(outside?.ariaRole).toBeUndefined();
    expect(outside?.domAttributes).toEqual({ "aria-disabled": true });
    expect(outside?.data.route).toEqual({ state: "not-compiled", model: "weekly_revenue" });

    const inside = byId.get("transformation:raw_orders");
    expect(inside?.focusable).toBe(true);
    expect(inside?.ariaRole).toBe("button");
    expect(inside?.data.route).toEqual({ state: "servable", model: "raw_orders" });
  });

  it("keeps every model in the tab order while the compiled set is unknown", () => {
    const flow = layeredFlow(twoPipelinesDag as unknown as DagOutput, "unknown");
    expect(flow.nodes.every((n) => n.focusable)).toBe(true);
  });

  it("does not move a node when the compiled set changes", () => {
    // The panel refits on layout identity. A set that arrives after the DAG
    // changes which nodes open, and must not reshape the graph under a hand.
    const dagValue = twoPipelinesDag as unknown as DagOutput;
    const before = layeredFlow(dagValue, "unknown").nodes.map((n) => [n.id, n.position]);
    const after = layeredFlow(dagValue, new Set<string>()).nodes.map((n) => [n.id, n.position]);
    expect(after).toEqual(before);
  });

  it("refuses a tab stop to a kind it cannot classify", () => {
    const flow = layeredFlow(
      dag({
        nodes: [{ id: "x:a", label: "a", kind: "materialized_view" }] as DagOutput["nodes"],
        execution_layers: [["x:a"]],
      }),
      "unknown",
    );
    expect(flow.nodes[0]?.focusable).toBe(false);
  });

  it("lays out the playground's captured DAG: one node per model, one edge per dependency", () => {
    const captured = dagFixture as unknown as DagOutput;
    const flow = layeredFlow(captured, "unknown");
    expect(flow.nodes.map((n) => n.id).sort()).toEqual(captured.nodes.map((n) => n.id).sort());
    expect(flow.edges).toHaveLength(captured.edges.length);
    expect(flow.dropped).toBe(0);
    expect(new Set(flow.nodes.map((n) => n.data.layer)).size).toBe(
      captured.execution_layers.length,
    );
  });
});
