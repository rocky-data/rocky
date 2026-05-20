import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// VS Code mock — must precede all imports that touch vscode
// ---------------------------------------------------------------------------

let costAnnotationsEnabled = true;

vi.mock("vscode", () => {
  class Range {
    constructor(
      public startLine: number,
      public startChar: number,
      public endLine: number,
      public endChar: number,
    ) {}
  }

  class CodeLens {
    constructor(
      public range: Range,
      public command?: { title: string; command: string; arguments?: unknown[] },
    ) {}
  }

  class EventEmitter {
    private listeners: (() => void)[] = [];
    event = (listener: () => void): { dispose(): void } => {
      this.listeners.push(listener);
      return { dispose: (): void => undefined };
    };
    fire(): void {
      for (const l of this.listeners) l();
    }
    dispose(): void {}
  }

  return {
    Range,
    CodeLens,
    EventEmitter,
    workspace: {
      getConfiguration: (_section: string) => ({
        get: <T,>(key: string, fallback: T): T => {
          if (key === "costAnnotations.enabled")
            return costAnnotationsEnabled as unknown as T;
          return fallback;
        },
      }),
      onDidChangeConfiguration: () => ({ dispose: () => undefined }),
    },
    languages: {
      registerCodeLensProvider: () => ({ dispose: () => undefined }),
    },
    commands: {
      registerCommand: () => ({ dispose: () => undefined }),
    },
    window: {
      showInformationMessage: vi.fn(() => Promise.resolve(undefined)),
    },
  };
});

// rockyCli mock — use vi.fn() with no factory so hoisting works safely.
// We'll set the implementation in beforeEach via mockImplementation.
vi.mock("../rockyCli", () => ({
  runRockyJson: vi.fn(),
}));

// output channel mock
vi.mock("../output", () => ({
  getOutputChannel: () => ({ appendLine: vi.fn() }),
}));

// `views/getStartedView` exposes the hasRockyProject() / onDidChangeRockyProject
// signal used to gate CLI invocations. Tests assume a Rocky project is
// present so the provider actually fetches data; the onDidChange event is a
// no-op subscription.
vi.mock("../views/getStartedView", () => ({
  hasRockyProject: (): boolean => true,
  onDidChangeRockyProject: () => ({ dispose: (): void => undefined }),
}));

// ---------------------------------------------------------------------------
// Imports — must follow vi.mock calls (hoisting works, but named bindings need
// to come AFTER the mock declarations so the mock factory runs first)
// ---------------------------------------------------------------------------

import * as rockyCliModule from "../rockyCli";
import type { OptimizeOutput, OptimizeRecommendation } from "../types/generated/optimize";
import {
  CostCodeLensProvider,
  invalidateCostCache,
} from "../costCodeLens";

// Typed handle on the mock so we can configure per-test responses.
const runRockyJsonMock = rockyCliModule.runRockyJson as ReturnType<typeof vi.fn>;

// ---------------------------------------------------------------------------
// Test data helpers
// ---------------------------------------------------------------------------

function makeRecommendation(
  modelName: string,
  overrides: Partial<OptimizeRecommendation> = {},
): OptimizeRecommendation {
  return {
    model_name: modelName,
    compute_cost_per_run: 0.12,
    storage_cost_per_month: 3.5,
    estimated_monthly_savings: 1.25,
    current_strategy: "view",
    recommended_strategy: "table",
    reasoning: "High downstream fan-out",
    downstream_references: 5,
    ...overrides,
  };
}

function makeOptimizeOutput(recs: OptimizeRecommendation[]): OptimizeOutput {
  return {
    command: "optimize",
    version: "1.31.0",
    total_models_analyzed: recs.length,
    recommendations: recs,
  };
}

/** Fake VS Code TextDocument for a model file. */
function makeDocument(filePath: string): import("vscode").TextDocument {
  return {
    fileName: filePath,
    lineCount: 1,
    lineAt: () => ({ text: "" }),
  } as unknown as import("vscode").TextDocument;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CostCodeLensProvider", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    costAnnotationsEnabled = true;
    invalidateCostCache();
  });

  afterEach(() => {
    invalidateCostCache();
  });

  describe("cache hit → CodeLens is emitted", () => {
    it("returns a CodeLens with cost label for a known model after refresh", async () => {
      const rec = makeRecommendation("orders");
      runRockyJsonMock.mockResolvedValueOnce(makeOptimizeOutput([rec]));

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      const doc = makeDocument("/workspace/models/orders.sql");
      const lenses = provider.provideCodeLenses(doc);

      expect(lenses).toHaveLength(1);
      expect(lenses[0].command?.title).toContain("$0.1200/run");
      expect(lenses[0].command?.title).toContain("$3.50/mo storage");
      expect(lenses[0].command?.title).toContain("save $1.25/mo");
      expect(lenses[0].command?.command).toBe("rocky.showCostDetail");
    });

    it("works for .rocky files under models/", async () => {
      const rec = makeRecommendation("customers");
      runRockyJsonMock.mockResolvedValueOnce(makeOptimizeOutput([rec]));

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      const doc = makeDocument("/workspace/models/customers.rocky");
      const lenses = provider.provideCodeLenses(doc);

      expect(lenses).toHaveLength(1);
      expect(lenses[0].command?.arguments?.[0]).toMatchObject({
        model_name: "customers",
      });
    });
  });

  describe("cache miss → no CodeLens", () => {
    it("returns empty array before any refresh", () => {
      const provider = new CostCodeLensProvider();
      const doc = makeDocument("/workspace/models/orders.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });

    it("returns empty array when model is not in the optimize output", async () => {
      runRockyJsonMock.mockResolvedValueOnce(
        makeOptimizeOutput([makeRecommendation("other_model")]),
      );

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      const doc = makeDocument("/workspace/models/unknown.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });

    it("returns empty array for files outside models/", async () => {
      runRockyJsonMock.mockResolvedValueOnce(
        makeOptimizeOutput([makeRecommendation("orders")]),
      );

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      const doc = makeDocument("/workspace/seeds/orders.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });
  });

  describe("disabled setting → CodeLens is cleared", () => {
    it("returns empty array when costAnnotations.enabled is false", async () => {
      runRockyJsonMock.mockResolvedValueOnce(
        makeOptimizeOutput([makeRecommendation("orders")]),
      );

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      costAnnotationsEnabled = false;

      const doc = makeDocument("/workspace/models/orders.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });
  });

  describe("malformed / failed JSON → graceful no-op", () => {
    it("silently no-ops when rocky optimize fails, leaving cache empty", async () => {
      runRockyJsonMock.mockRejectedValueOnce(new Error("spawn rocky ENOENT"));

      const provider = new CostCodeLensProvider();
      await provider.refresh(); // must not throw

      const doc = makeDocument("/workspace/models/orders.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });

    it("does not throw when recommendations is empty", async () => {
      runRockyJsonMock.mockResolvedValueOnce(makeOptimizeOutput([]));

      const provider = new CostCodeLensProvider();
      await provider.refresh();

      const doc = makeDocument("/workspace/models/orders.sql");
      const lenses = provider.provideCodeLenses(doc);
      expect(lenses).toHaveLength(0);
    });
  });

  describe("fireChange", () => {
    it("fires the onDidChangeCodeLenses event", () => {
      const provider = new CostCodeLensProvider();
      const listener = vi.fn();
      provider.onDidChangeCodeLenses(listener);
      provider.fireChange();
      expect(listener).toHaveBeenCalledTimes(1);
    });
  });
});
