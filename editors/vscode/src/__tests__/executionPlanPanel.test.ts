import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import {
  formatKindCounts,
  maxParallelism,
  nodeLabel,
} from "../webviews/executionPlanPanel";

describe("maxParallelism", () => {
  it("is the widest layer", () => {
    expect(maxParallelism([["a"], ["b", "c"], ["d"]])).toBe(2);
  });
  it("is 0 for an empty plan", () => {
    expect(maxParallelism([])).toBe(0);
  });
});

describe("formatKindCounts", () => {
  it("renders sorted kind counts", () => {
    expect(formatKindCounts({ transformation: 5, source: 2 })).toBe(
      "2 source · 5 transformation",
    );
  });
  it("handles a single kind", () => {
    expect(formatKindCounts({ transformation: 1 })).toBe("1 transformation");
  });
});

describe("nodeLabel", () => {
  const byId = new Map([["transformation:stg_orders", "stg_orders"]]);
  it("prefers the node label", () => {
    expect(nodeLabel("transformation:stg_orders", byId)).toBe("stg_orders");
  });
  it("falls back to the id with the kind prefix stripped", () => {
    expect(nodeLabel("source:raw.orders", byId)).toBe("raw.orders");
  });
});
