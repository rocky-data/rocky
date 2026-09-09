import { describe, expect, it } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import mixedDag from "../test/fixtures/dag-mixed-kinds.json";
import { CLASSIFIED_KINDS, nodeRoute } from "./nodeRoute";

const captured = mixedDag as unknown as DagOutput;

describe("nodeRoute", () => {
  it("serves a transformation node under its bare label, never its id", () => {
    const route = nodeRoute({ kind: "transformation", label: "customer_orders" });
    expect(route).toEqual({ state: "servable", model: "customer_orders" });
  });

  it.each([
    ["source", "ecommerce (source)"],
    ["replication", "ecommerce"],
    ["quality", "nightly_dq"],
    ["snapshot", "customer_history"],
    ["load", "ecommerce (load)"],
    ["seed", "country_codes"],
    ["test", "revenue_summary::not_null_customer_id"],
  ])("does not serve a %s node", (kind, label) => {
    expect(nodeRoute({ kind, label })).toEqual({ state: "not-servable" });
  });

  it("names every kind the engine declares, and nothing else", () => {
    // `NodeKind` in `unified_dag.rs`, in declaration order.
    expect(CLASSIFIED_KINDS).toEqual([
      "source",
      "replication",
      "transformation",
      "quality",
      "snapshot",
      "load",
      "seed",
      "test",
    ]);
  });

  it("refuses to classify a kind the table does not name", () => {
    // Not "not-servable": a kind added in Rust reaches this package as a
    // bare string, so the third state is the only thing a test can see.
    expect(nodeRoute({ kind: "materialized_view", label: "x" })).toEqual({
      state: "unknown-kind",
      kind: "materialized_view",
    });
  });

  it("does not mistake an inherited object property for a kind", () => {
    expect(nodeRoute({ kind: "constructor", label: "x" })).toEqual({
      state: "unknown-kind",
      kind: "constructor",
    });
  });

  describe("against the captured DAG", () => {
    it("classifies every node in it", () => {
      const unknown = captured.nodes
        .map((node) => ({ node, route: nodeRoute(node) }))
        .filter(({ route }) => route.state === "unknown-kind");
      expect(unknown.map(({ node }) => node.kind)).toEqual([]);
    });

    it("covers seven of the engine's eight kinds, so the branches are real", () => {
      expect(new Set(captured.nodes.map((n) => n.kind))).toEqual(
        new Set(["seed", "snapshot", "source", "load", "quality", "transformation", "test"]),
      );
    });

    it("serves exactly its transformation nodes, each under its bare label", () => {
      const servable = captured.nodes
        .map((node) => [node, nodeRoute(node)] as const)
        .filter(([, route]) => route.state === "servable");
      expect(servable.map(([node]) => node.kind)).toEqual([
        "transformation",
        "transformation",
        "transformation",
      ]);
      expect(
        servable.map(([, route]) => (route.state === "servable" ? route.model : null)),
      ).toEqual(["customer_orders", "raw_orders", "revenue_summary"]);
      // The bare label, not the id the panel used to pass.
      expect(servable.map(([node]) => node.id)).toEqual([
        "transformation:customer_orders",
        "transformation:raw_orders",
        "transformation:revenue_summary",
      ]);
    });
  });
});
