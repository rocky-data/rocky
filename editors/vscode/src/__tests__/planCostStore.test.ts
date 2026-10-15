import { beforeEach, describe, expect, it } from "vitest";
import {
  changedSince,
  initPlanCostStore,
  readPlanCost,
  recordPlanCost,
} from "../planCostStore";
import type { PlanOutput } from "../types/generated/plan";

function memento(): Parameters<typeof initPlanCostStore>[0] {
  const map = new Map<string, unknown>();
  return {
    get: <T>(k: string) => map.get(k) as T | undefined,
    update: (k, v) => {
      map.set(k, v);
      return Promise.resolve();
    },
  };
}

const withCost = {
  command: "plan",
  filter: "x=1",
  cost_preview: {
    is_estimate: true,
    models_to_rebuild: 2,
    source: "mixed",
    models: [
      { model: "a", source: "adapter", estimated_cost_usd: 0.4, estimated_rows: 9 },
      { model: "b", source: "heuristic" },
    ],
  },
} as unknown as PlanOutput;

describe("planCostStore", () => {
  beforeEach(() => initPlanCostStore(memento()));

  it("records the preview; an unpriced model stays null, not 0", () => {
    recordPlanCost(withCost, "x=1", new Date("2026-10-16T10:00:00Z"));
    const got = readPlanCost()!;
    expect(got.recordedAt).toBe("2026-10-16T10:00:00.000Z");
    expect(got.filter).toBe("x=1");
    expect(got.source).toBe("mixed");
    expect(got.models).toEqual([
      { model: "a", estimatedCostUsd: 0.4, estimatedRows: 9, source: "adapter" },
      { model: "b", estimatedCostUsd: null, estimatedRows: null, source: "heuristic" },
    ]);
  });

  it("prefers the plan's own created_at", () => {
    recordPlanCost({ ...withCost, created_at: "2026-10-15T00:00:00Z" } as PlanOutput, "x=1");
    expect(readPlanCost()!.recordedAt).toBe("2026-10-15T00:00:00Z");
  });

  it("a plan without cost_preview keeps the earlier estimate", () => {
    recordPlanCost(withCost, "x=1");
    recordPlanCost({ command: "plan", filter: "y=2" } as PlanOutput, "y=2");
    expect(readPlanCost()!.filter).toBe("x=1");
  });

  it("is empty before any plan", () => {
    expect(readPlanCost()).toBeNull();
  });

  it("changedSince flags files newer than the plan, and bad dates", () => {
    const t = Date.parse("2026-10-16T10:00:00Z");
    expect(changedSince("2026-10-16T10:00:00Z", [t - 1, t])).toBe(false);
    expect(changedSince("2026-10-16T10:00:00Z", [t + 1])).toBe(true);
    expect(changedSince("garbage", [])).toBe(true);
  });
});
