import { describe, expect, it } from "vitest";
import { composeBadges } from "./compose";
import type { LineageOverlay, NodeView } from "./types";

const node: NodeView = {
  id: "m",
  data: {
    label: "m",
    kind: "Model",
    materialization: null,
    fqn: "c.s.m",
    costHint: null,
    freshness: null,
  },
};

describe("composeBadges", () => {
  it("concatenates badges from all overlays, in order", () => {
    const a: LineageOverlay = { id: "a", decorate: () => [{ text: "A" }] };
    const b: LineageOverlay = {
      id: "b",
      decorate: () => [{ text: "B1" }, { text: "B2" }],
    };
    expect(composeBadges(node, [a, b]).map((x) => x.text)).toEqual([
      "A",
      "B1",
      "B2",
    ]);
  });

  it("skips overlays that decline to decorate", () => {
    const a: LineageOverlay = { id: "a", decorate: () => undefined };
    const b: LineageOverlay = { id: "b", decorate: () => [{ text: "B" }] };
    expect(composeBadges(node, [a, b]).map((x) => x.text)).toEqual(["B"]);
  });

  it("returns nothing when no overlays are active", () => {
    expect(composeBadges(node, [])).toEqual([]);
  });
});
