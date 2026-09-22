import { describe, expect, it } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import type { ModelListOutput } from "@rocky-types/model_list";
import mixedDag from "../test/fixtures/dag-mixed-kinds.json";
import twoPipelinesDag from "../test/fixtures/dag-two-pipelines.json";
import twoPipelinesModels from "../test/fixtures/model-list-two-pipelines.json";
import { CLASSIFIED_KINDS, compiledModels, nodeRoute } from "./nodeRoute";

const captured = mixedDag as unknown as DagOutput;

const list = (names: string[], count = names.length): ModelListOutput => ({
  count,
  models: names.map((name) => ({ name, columns: 1, has_star: false, upstream: [], downstream: [] })),
});

describe("nodeRoute", () => {
  it("serves a transformation node under its bare label, never its id", () => {
    const route = nodeRoute({ kind: "transformation", label: "customer_orders" }, "unknown");
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
    expect(nodeRoute({ kind, label }, "unknown")).toEqual({ state: "not-servable" });
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
    expect(nodeRoute({ kind: "materialized_view", label: "x" }, "unknown")).toEqual({
      state: "unknown-kind",
      kind: "materialized_view",
    });
  });

  it("does not mistake an inherited object property for a kind", () => {
    expect(nodeRoute({ kind: "constructor", label: "x" }, "unknown")).toEqual({
      state: "unknown-kind",
      kind: "constructor",
    });
  });

  describe("against the compiled model set", () => {
    const compiled = new Set(["customer_orders", "raw_orders"]);

    it("serves a transformation node the server compiled", () => {
      expect(nodeRoute({ kind: "transformation", label: "raw_orders" }, compiled)).toEqual({
        state: "servable",
        model: "raw_orders",
      });
    });

    it("does not serve a transformation node the server did not compile", () => {
      expect(nodeRoute({ kind: "transformation", label: "weekly_revenue" }, compiled)).toEqual({
        state: "not-compiled",
        model: "weekly_revenue",
      });
    });

    it("keeps a transformation node servable while the set is unknown", () => {
      // Not knowing is not evidence of absence. Gating on nothing would
      // disable every model whenever the list failed to load.
      expect(nodeRoute({ kind: "transformation", label: "weekly_revenue" }, "unknown")).toEqual({
        state: "servable",
        model: "weekly_revenue",
      });
    });

    it("treats an empty set as known: no model is compiled", () => {
      expect(nodeRoute({ kind: "transformation", label: "raw_orders" }, new Set())).toEqual({
        state: "not-compiled",
        model: "raw_orders",
      });
    });

    it("matches the name exactly, as the route looks it up", () => {
      expect(nodeRoute({ kind: "transformation", label: "Raw_Orders" }, compiled).state).toBe(
        "not-compiled",
      );
    });

    it("never makes another kind servable, even when its label is in the set", () => {
      expect(
        nodeRoute({ kind: "seed", label: "country_codes" }, new Set(["country_codes"])),
      ).toEqual({ state: "not-servable" });
    });
  });

  describe("compiledModels", () => {
    it("reads a whole list as the set of its names", () => {
      expect(compiledModels(list(["a", "b"]))).toEqual(new Set(["a", "b"]));
    });

    it("reads a cut list as unknown, so no real model is marked absent", () => {
      // `count` repeats `models.len()`; a list that carries fewer than it
      // says is not a list of what was compiled.
      expect(compiledModels(list(["a"], 2))).toBe("unknown");
    });

    it("reads a whole empty list as a known empty set", () => {
      expect(compiledModels(list([]))).toEqual(new Set());
    });
  });

  describe("against the captured DAG", () => {
    it("classifies every node in it", () => {
      const unknown = captured.nodes
        .map((node) => ({ node, route: nodeRoute(node, "unknown") }))
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
        .map((node) => [node, nodeRoute(node, "unknown")] as const)
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

  describe("against the captured two-pipeline project", () => {
    // Recorded from one `rocky serve`: a second transformation pipeline keeps
    // its model in `reporting/`, which the DAG reads and the compile does not.
    const dag = twoPipelinesDag as unknown as DagOutput;
    const models = twoPipelinesModels as unknown as ModelListOutput;

    it("draws a model the compile does not have", () => {
      const labels = dag.nodes.filter((n) => n.kind === "transformation").map((n) => n.label);
      expect(labels).toHaveLength(4);
      expect(models.count).toBe(3);
    });

    it("serves the three compiled models and marks the fourth not compiled", () => {
      const compiled = compiledModels(models);
      const routes = Object.fromEntries(dag.nodes.map((n) => [n.label, nodeRoute(n, compiled).state]));
      expect(routes).toEqual({
        customer_orders: "servable",
        raw_orders: "servable",
        revenue_summary: "servable",
        weekly_revenue: "not-compiled",
      });
    });
  });
});
