import { describe, expect, it } from "vitest";
import type { PlanCostData } from "../../../../src/webviews/lineage/contract";
import { makeCostOverlay, relativeTime } from "./cost";
import type { NodeView } from "./types";

const NOW = Date.parse("2026-10-16T12:00:00Z");

function node(id: string, hint = true): NodeView {
  return {
    id,
    data: {
      label: id,
      kind: "Model",
      materialization: null,
      fqn: `c.s.${id}`,
      costHint: hint
        ? { estimated_cost_usd: 0.002, estimated_rows: 1000, confidence: "low" }
        : null,
      freshness: null,
    } as NodeView["data"],
  };
}

const plan = (over: Partial<PlanCostData> = {}): PlanCostData => ({
  recordedAt: "2026-10-16T11:30:00Z",
  filter: "client=acme",
  source: "adapter",
  modelsChangedSince: false,
  models: [
    { model: "a", estimatedCostUsd: 0.05, estimatedRows: 10, source: "adapter" },
    { model: "unpriced", estimatedCostUsd: null, estimatedRows: null, source: "adapter" },
  ],
  ...over,
});

describe("cost overlay", () => {
  it("falls back to the labelled heuristic when no plan exists", () => {
    const [b] = makeCostOverlay(null, NOW).decorate(node("a"))!;
    expect(b.text).toBe("~$0.002 (heuristic)");
    expect(makeCostOverlay(null, NOW).legend!()[0].text).toContain("heuristic");
  });

  it("shows the plan figure labelled 'from last plan' with its time", () => {
    const [b] = makeCostOverlay(plan(), NOW).decorate(node("a"))!;
    expect(b.text).toBe("~$0.050 (plan, adapter)");
    expect(b.title).toContain("from last plan");
    expect(b.title).toContain("30 min ago");
    expect(b.title).not.toContain("stale");
  });

  it("names the planned filter in the tooltip and legend, only when set", () => {
    const o = makeCostOverlay(plan(), NOW);
    expect(o.decorate(node("a"))![0].title).toContain("30 min ago · filter client=acme");
    expect(o.legend!()[0].text).toContain("filter client=acme");
    const none = makeCostOverlay(plan({ filter: "" }), NOW);
    expect(none.decorate(node("a"))![0].title).not.toContain("filter");
    expect(none.legend!()[0].text).not.toContain("filter");
  });

  it("labels a plan stale when a model changed since", () => {
    const [b] = makeCostOverlay(plan({ modelsChangedSince: true }), NOW).decorate(node("a"))!;
    expect(b.text).toContain("stale");
    expect(b.title).toContain("models changed since this plan");
  });

  it("labels a plan older than a day stale", () => {
    const o = makeCostOverlay(plan({ recordedAt: "2026-10-14T12:00:00Z" }), NOW);
    expect(o.decorate(node("a"))![0].text).toContain("stale");
    expect(o.legend!()[0].text).toContain("stale");
  });

  it("keeps the labelled heuristic for models outside the plan", () => {
    const [b] = makeCostOverlay(plan(), NOW).decorate(node("other"))!;
    expect(b.text).toContain("heuristic");
  });

  it("never shows $0 for a model the plan could not price", () => {
    const [b] = makeCostOverlay(plan(), NOW).decorate(node("unpriced"))!;
    expect(b.text).toContain("heuristic");
    expect(b.title).toContain("could not price");
    expect(makeCostOverlay(plan(), NOW).decorate(node("unpriced", false))).toBeUndefined();
  });

  it("formats relative time", () => {
    expect(relativeTime("2026-10-16T11:59:50Z", NOW)).toBe("just now");
    expect(relativeTime("2026-10-16T09:00:00Z", NOW)).toBe("3 h ago");
    expect(relativeTime("2026-10-10T12:00:00Z", NOW)).toBe("6 d ago");
  });
});
