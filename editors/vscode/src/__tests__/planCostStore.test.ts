import { beforeEach, describe, expect, it, vi } from "vitest";

let projectRoot: string | undefined = "/proj/a";
let found: Array<{ fsPath: string; mtime: number }> = [];

vi.mock("vscode", () => ({
  RelativePattern: class {
    constructor(
      public base: string,
      public pattern: string,
    ) {}
  },
  workspace: {
    // Honour the `**/*.{a,b}` extension list, as the real glob would.
    findFiles: vi.fn(async (p: { pattern: string }) => {
      const exts = (/\{(.+)\}/.exec(p.pattern)?.[1] ?? "").split(",");
      return found
        .filter((f) => exts.some((e) => f.fsPath.endsWith(`.${e}`)))
        .map((f) => ({ fsPath: f.fsPath }));
    }),
    fs: {
      stat: vi.fn(async (u: { fsPath: string }) => ({
        mtime: found.find((f) => f.fsPath === u.fsPath)!.mtime,
      })),
    },
  },
}));
vi.mock("../config", () => ({ resolveProjectRoot: () => projectRoot }));

import {
  changedSince,
  initPlanCostStore,
  loadPlanCost,
  onPlanCostRecorded,
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

const T0 = "2026-10-16T10:00:00.000Z";
const t0 = Date.parse(T0);

describe("planCostStore", () => {
  beforeEach(() => {
    initPlanCostStore(memento());
    projectRoot = "/proj/a";
    found = [];
  });

  it("records the preview; an unpriced model stays null, not 0", () => {
    recordPlanCost(withCost, "x=1", "/proj/a", new Date(T0));
    const got = readPlanCost("/proj/a")!;
    expect(got.recordedAt).toBe(T0);
    expect(got.filter).toBe("x=1");
    expect(got.source).toBe("mixed");
    expect(got.models).toEqual([
      { model: "a", estimatedCostUsd: 0.4, estimatedRows: 9, source: "adapter" },
      { model: "b", estimatedCostUsd: null, estimatedRows: null, source: "heuristic" },
    ]);
  });

  it("prefers the plan's own created_at", () => {
    recordPlanCost({ ...withCost, created_at: "2026-10-15T00:00:00Z" } as PlanOutput, "x=1", "/proj/a");
    expect(readPlanCost("/proj/a")!.recordedAt).toBe("2026-10-15T00:00:00Z");
  });

  it("a plan without cost_preview keeps the earlier estimate", () => {
    recordPlanCost(withCost, "x=1", "/proj/a");
    recordPlanCost({ command: "plan", filter: "y=2" } as PlanOutput, "y=2", "/proj/a");
    expect(readPlanCost("/proj/a")!.filter).toBe("x=1");
  });

  it("is empty before any plan", () => {
    expect(readPlanCost("/proj/a")).toBeNull();
  });

  it("returns null for a different project root", () => {
    recordPlanCost(withCost, "x=1", "/proj/a");
    expect(readPlanCost("/proj/b")).toBeNull();
    projectRoot = "/proj/b";
    return expect(loadPlanCost()).resolves.toBeNull();
  });

  it("notifies listeners when a plan is recorded", () => {
    const seen = vi.fn();
    const off = onPlanCostRecorded(seen);
    recordPlanCost(withCost, "x=1", "/proj/a");
    off();
    recordPlanCost(withCost, "x=1", "/proj/a");
    expect(seen).toHaveBeenCalledTimes(1);
  });

  it("changedSince flags files newer than the plan, and bad dates", () => {
    expect(changedSince(T0, [t0 - 1, t0])).toBe(false);
    expect(changedSince(T0, [t0 + 1])).toBe(true);
    expect(changedSince("garbage", [])).toBe(true);
  });

  describe("loadPlanCost staleness", () => {
    beforeEach(() => recordPlanCost(withCost, "x=1", "/proj/a", new Date(T0)));

    it("is fresh when nothing changed after the plan", async () => {
      found = [{ fsPath: "/proj/a/models/m.sql", mtime: t0 - 1000 }];
      expect((await loadPlanCost())!.modelsChangedSince).toBe(false);
    });

    it("is stale when a sidecar .toml is newer", async () => {
      found = [
        { fsPath: "/proj/a/models/m.sql", mtime: t0 - 1000 },
        { fsPath: "/proj/a/models/m.toml", mtime: t0 + 1000 },
      ];
      expect((await loadPlanCost())!.modelsChangedSince).toBe(true);
    });

    it("is stale when rocky.toml is newer", async () => {
      found = [{ fsPath: "/proj/a/rocky.toml", mtime: t0 + 1000 }];
      expect((await loadPlanCost())!.modelsChangedSince).toBe(true);
    });

    it("scans the project root for rocky, sql and toml files", async () => {
      const vscode = await import("vscode");
      await loadPlanCost();
      const call = vi.mocked(vscode.workspace.findFiles).mock.calls.at(-1)!;
      const pattern = call[0] as unknown as { base: string; pattern: string };
      expect(pattern.base).toBe("/proj/a");
      expect(pattern.pattern).toContain("toml");
      expect(call[1]).toBeUndefined();
    });

    it("checks the root the graph was built from, not the active editor's", async () => {
      // Active editor is in project B; the Inspector graph is project A's.
      projectRoot = "/proj/b";
      expect(await loadPlanCost("/proj/a")).not.toBeNull();
      // Graph from project B while the plan is project A's: no cost.
      projectRoot = "/proj/a";
      expect(await loadPlanCost("/proj/b")).toBeNull();
    });

    it("reads stale when the scan hit its file cap", async () => {
      found = Array.from({ length: 20000 }, (_, i) => ({
        fsPath: `/proj/a/m${i}.sql`,
        mtime: t0 - 1000,
      }));
      expect((await loadPlanCost("/proj/a"))!.modelsChangedSince).toBe(true);
    });

    it("does not leak the storage-only fields", async () => {
      const got = (await loadPlanCost()) as unknown as Record<string, unknown>;
      expect(got.projectRoot).toBeUndefined();
      expect(got.version).toBeUndefined();
    });
  });
});
