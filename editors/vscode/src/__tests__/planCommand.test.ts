import { beforeEach, describe, expect, it, vi } from "vitest";

const run = vi.fn();

vi.mock("vscode", () => ({
  window: {
    showInformationMessage: vi.fn(),
    showErrorMessage: vi.fn(),
  },
}));
vi.mock("../config", () => ({ resolveProjectRoot: () => "/proj/a" }));
vi.mock("../rockyCli", () => ({
  runRockyJsonWithProgress: (...a: unknown[]) => run(...a),
  showRockyError: vi.fn(),
}));
vi.mock("../webviews/runSummary", () => ({ showRunSummary: vi.fn() }));
vi.mock("../commands/ui", () => ({
  confirmAction: vi.fn(),
  ensureWorkspace: () => true,
  promptForInput: async () => "client=acme",
  showJsonInEditor: vi.fn(),
  showSqlInEditor: vi.fn(),
}));

import { plan } from "../commands/run";
import { initPlanCostStore, readPlanCost } from "../planCostStore";

describe("plan command records the cost preview", () => {
  beforeEach(() => {
    const map = new Map<string, unknown>();
    initPlanCostStore({
      get: <T>(k: string) => map.get(k) as T | undefined,
      update: (k, v) => {
        map.set(k, v);
        return Promise.resolve();
      },
    });
    run.mockReset();
  });

  it("stores what it parsed, scoped to the project root it ran in", async () => {
    run.mockResolvedValue({
      command: "plan",
      filter: "client=acme",
      statements: [],
      cost_preview: {
        is_estimate: true,
        models_to_rebuild: 1,
        source: "adapter",
        models: [{ model: "a", source: "adapter", estimated_cost_usd: 0.1 }],
      },
    });
    await plan();
    expect(run.mock.calls[0][2]).toMatchObject({ cwd: "/proj/a" });
    const got = readPlanCost("/proj/a")!;
    expect(got.filter).toBe("client=acme");
    expect(got.models[0]).toMatchObject({ model: "a", estimatedCostUsd: 0.1 });
    expect(readPlanCost("/proj/b")).toBeNull();
  });

  it("records nothing when the CLI fails", async () => {
    run.mockRejectedValue(new Error("boom"));
    await plan();
    expect(readPlanCost("/proj/a")).toBeNull();
  });
});
