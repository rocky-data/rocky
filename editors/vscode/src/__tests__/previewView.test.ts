import { beforeEach, describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// vscode mock
// ---------------------------------------------------------------------------
vi.mock("vscode", () => {
  class TreeItem {
    public label: string;
    public collapsibleState: number;
    public iconPath: unknown;
    public tooltip: string | undefined;
    public command: unknown;
    public description: string | undefined;
    public contextValue: string | undefined;
    constructor(label: string, collapsibleState: number) {
      this.label = label;
      this.collapsibleState = collapsibleState;
    }
  }

  class ThemeIcon {
    constructor(public id: string, public color?: unknown) {}
  }

  class ThemeColor {
    constructor(public id: string) {}
  }

  class EventEmitter<T> {
    private listeners: ((value: T) => void)[] = [];
    event = (listener: (value: T) => void): { dispose(): void } => {
      this.listeners.push(listener);
      return { dispose: (): void => undefined };
    };
    fire(value: T): void {
      for (const l of this.listeners) l(value);
    }
  }

  class Position {
    constructor(public line: number, public character: number) {}
  }

  return {
    TreeItem,
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    ThemeIcon,
    ThemeColor,
    EventEmitter,
    Position,
    Uri: {
      parse: (s: string) => ({ toString: () => s }),
      file: (p: string) => ({ fsPath: p, toString: () => p }),
    },
    workspace: {
      findFiles: vi.fn(async () => []),
      workspaceFolders: undefined,
      asRelativePath: (uri: { fsPath: string }) => uri.fsPath,
      getConfiguration: () => ({
        get: <T,>(_key: string, fallback: T): T => fallback,
      }),
      onDidChangeConfiguration: () => ({ dispose: () => undefined }),
      onDidChangeWorkspaceFolders: () => ({ dispose: () => undefined }),
      openTextDocument: vi.fn(async () => ({
        getText: () => "",
      })),
    },
    commands: {
      registerCommand: () => ({ dispose: () => undefined }),
    },
    window: {
      createTreeView: () => ({ dispose: () => undefined }),
      createTextEditorDecorationType: () => ({ dispose: () => undefined }),
      visibleTextEditors: [],
      showInformationMessage: vi.fn(async () => undefined),
      showTextDocument: vi.fn(async () => ({
        edit: vi.fn(async (cb: (e: { insert: () => void }) => void) => {
          cb({ insert: () => undefined });
          return true;
        }),
      })),
    },
    env: {
      clipboard: { writeText: vi.fn(async () => undefined) },
    },
  };
});

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

let hasProject = true;
const projectChangeListeners: ((v: boolean) => void)[] = [];

vi.mock("../views/getStartedView", () => ({
  hasRockyProject: () => hasProject,
  onDidChangeRockyProject: (cb: (v: boolean) => void) => {
    projectChangeListeners.push(cb);
    return { dispose: () => undefined };
  },
}));

const mockRunRockyJson = vi.fn<() => Promise<unknown>>();
vi.mock("../rockyCli", () => ({
  runRockyJson: (...args: unknown[]) => mockRunRockyJson(...args),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

import type { PreviewCostOutput, PreviewDiffOutput } from "../types/generated";
import {
  PreviewStateManager,
  PreviewTreeProvider,
  PreviewBranchNode,
  PreviewCategoryNode,
  PreviewCostModelNode,
  PreviewDiffModelNode,
  type ActivePreview,
} from "../views/previewView";

/** A minimal Memento backed by a plain object (mirrors vscode.Memento shape). */
function makeMemento(): {
  store: Record<string, unknown>;
  get<T>(key: string, defaultValue: T): T;
  update(key: string, value: unknown): Promise<void>;
} {
  const store: Record<string, unknown> = {};
  return {
    store,
    get<T>(key: string, defaultValue: T): T {
      return (store[key] as T) ?? defaultValue;
    },
    async update(key: string, value: unknown): Promise<void> {
      store[key] = value;
    },
  };
}

function makeCostOutput(branchName: string): PreviewCostOutput {
  return {
    branch_name: branchName,
    branch_run_id: "run-1",
    command: "preview cost",
    markdown: "",
    per_model: [
      {
        model_name: "orders",
        base_duration_ms: 100,
        branch_duration_ms: 120,
        delta_usd: 0.002,
        skipped_via_copy: false,
      },
    ],
    summary: {
      delta_usd: 0.002,
      models_skipped_via_copy: 0,
      total_branch_duration_ms: 120,
    },
    version: "1.0.0",
  };
}

function makeDiffOutput(branchName: string): PreviewDiffOutput {
  return {
    base_ref: "main",
    branch_name: branchName,
    command: "preview diff",
    markdown: "# Diff\nsome changes",
    models: [
      {
        model_name: "orders",
        structural: {
          added_columns: ["new_col"],
          removed_columns: [],
          type_changes: [],
        },
        algorithm: {
          kind: "sampled",
          sampled: {
            rows_added: 5,
            rows_changed: 2,
            rows_removed: 0,
            samples: [],
          },
          sampling_window: {
            coverage: "first_n_by_order",
            coverage_warning: false,
            limit: 1000,
            ordered_by: "id",
          },
        },
      },
    ],
    summary: {
      any_coverage_warning: false,
      models_with_changes: 1,
      models_unchanged: 0,
      total_rows_added: 5,
      total_rows_changed: 2,
      total_rows_removed: 0,
    },
    version: "1.0.0",
  };
}

// ---------------------------------------------------------------------------
// PreviewStateManager tests
// ---------------------------------------------------------------------------

describe("PreviewStateManager", () => {
  it("getAll returns [] when empty", () => {
    const mgr = new PreviewStateManager(makeMemento());
    expect(mgr.getAll()).toEqual([]);
  });

  it("add stores a preview and getAll returns it", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    const preview: ActivePreview = {
      branchName: "preview-fix-price",
      createdAt: "2026-05-14T00:00:00Z",
    };
    await mgr.add(preview);
    expect(mgr.getAll()).toHaveLength(1);
    expect(mgr.getAll()[0].branchName).toBe("preview-fix-price");
  });

  it("add overwrites an existing entry with the same branchName", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "br", createdAt: "t1" });
    await mgr.add({ branchName: "br", createdAt: "t2" });
    const all = mgr.getAll();
    expect(all).toHaveLength(1);
    expect(all[0].createdAt).toBe("t2");
  });

  it("remove deletes the matching entry", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "a", createdAt: "t" });
    await mgr.add({ branchName: "b", createdAt: "t" });
    await mgr.remove("a");
    const all = mgr.getAll();
    expect(all).toHaveLength(1);
    expect(all[0].branchName).toBe("b");
  });

  it("updateBranch merges patch fields", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "br", createdAt: "t" });
    await mgr.updateBranch("br", {
      lastCostSummary: { delta_usd: 0.5, models_changed: 3 },
    });
    const updated = mgr.getAll()[0];
    expect(updated.lastCostSummary?.delta_usd).toBe(0.5);
    expect(updated.createdAt).toBe("t");
  });
});

// ---------------------------------------------------------------------------
// PreviewTreeProvider tests
// ---------------------------------------------------------------------------

describe("PreviewTreeProvider", () => {
  beforeEach(() => {
    hasProject = true;
    mockRunRockyJson.mockReset();
    projectChangeListeners.length = 0;
  });

  it("returns [] when no Rocky project is detected", async () => {
    hasProject = false;
    const mgr = new PreviewStateManager(makeMemento());
    const provider = new PreviewTreeProvider(mgr);
    expect(await provider.getChildren()).toEqual([]);
  });

  it("returns [] when no previews are tracked (triggers viewsWelcome)", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    const provider = new PreviewTreeProvider(mgr);
    expect(await provider.getChildren()).toEqual([]);
  });

  it("returns one PreviewBranchNode per tracked preview", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "preview-a", createdAt: "t" });
    await mgr.add({ branchName: "preview-b", createdAt: "t" });
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    expect(roots).toHaveLength(2);
    expect(roots[0]).toBeInstanceOf(PreviewBranchNode);
    expect(roots[0].label).toBe("preview-a");
  });

  it("PreviewBranchNode description shows cost + model count when data present", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({
      branchName: "pr",
      createdAt: "t",
      lastCostSummary: { delta_usd: 0.0042, models_changed: 1 },
      lastDiffSummary: {
        models_with_changes: 1,
        models_unchanged: 0,
        total_rows_added: 5,
        total_rows_removed: 0,
        total_rows_changed: 2,
        any_coverage_warning: false,
      },
    });
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    const node = roots[0] as PreviewBranchNode;
    expect(node.description).toContain("Δ$");
    expect(node.description).toContain("1 model");
  });

  it("branch node children are Cost / Diff / Models Changed categories", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "pr", createdAt: "t" });
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    const branchNode = roots[0] as PreviewBranchNode;
    const children = await provider.getChildren(branchNode);
    expect(children).toHaveLength(3);
    expect(children[0]).toBeInstanceOf(PreviewCategoryNode);
    expect(children[0].label).toBe("Cost");
    expect(children[1].label).toBe("Diff");
    expect(children[2].label).toBe("Models Changed");
  });

  it("Cost category children are PreviewCostModelNode instances", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({
      branchName: "pr",
      createdAt: "t",
      perModelCost: [
        {
          model_name: "orders",
          base_duration_ms: 100,
          branch_duration_ms: 120,
          delta_usd: 0.002,
          skipped_via_copy: false,
        },
      ],
    });
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    const branchNode = roots[0] as PreviewBranchNode;
    const cats = await provider.getChildren(branchNode);
    const costCat = cats[0] as PreviewCategoryNode;
    const costChildren = await provider.getChildren(costCat);
    expect(costChildren).toHaveLength(1);
    expect(costChildren[0]).toBeInstanceOf(PreviewCostModelNode);
    expect(costChildren[0].label).toBe("orders");
  });

  it("Diff category children are PreviewDiffModelNode instances", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({
      branchName: "pr",
      createdAt: "t",
      perModelDiff: [
        {
          model_name: "orders",
          structural: {
            added_columns: ["x"],
            removed_columns: [],
            type_changes: [],
          },
          algorithm: {
            kind: "sampled",
            sampled: {
              rows_added: 1,
              rows_changed: 0,
              rows_removed: 0,
              samples: [],
            },
            sampling_window: {
              coverage: "first_n_by_order",
              coverage_warning: false,
              limit: 1000,
              ordered_by: "id",
            },
          },
        },
      ],
    });
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    const branchNode = roots[0] as PreviewBranchNode;
    const cats = await provider.getChildren(branchNode);
    const diffCat = cats[1] as PreviewCategoryNode;
    const diffChildren = await provider.getChildren(diffCat);
    expect(diffChildren).toHaveLength(1);
    expect(diffChildren[0]).toBeInstanceOf(PreviewDiffModelNode);
    expect(diffChildren[0].label).toBe("orders");
    // "added" heuristic: added_columns present, nothing removed/type-changed
    expect((diffChildren[0] as PreviewDiffModelNode).diffStatus).toBe("added");
  });

  it("refresh fires onDidChangeTreeData", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    const provider = new PreviewTreeProvider(mgr);
    let fired = false;
    provider.onDidChangeTreeData(() => { fired = true; });
    provider.refresh();
    expect(fired).toBe(true);
  });

  it("fetchAllBranches calls runRockyJson for cost and diff per branch", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "pr", createdAt: "t" });
    const provider = new PreviewTreeProvider(mgr);

    mockRunRockyJson
      .mockResolvedValueOnce(makeCostOutput("pr"))
      .mockResolvedValueOnce(makeDiffOutput("pr"));

    await provider.fetchAllBranches();

    // 2 calls: cost + diff
    expect(mockRunRockyJson).toHaveBeenCalledTimes(2);
    const updated = mgr.getAll()[0];
    expect(updated.lastCostSummary?.delta_usd).toBe(0.002);
    expect(updated.lastDiffSummary?.models_with_changes).toBe(1);
    expect(updated.perModelCost).toHaveLength(1);
    expect(updated.perModelDiff).toHaveLength(1);
  });

  it("fetchAllBranches handles a failure on one branch gracefully", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "pr", createdAt: "t" });
    const provider = new PreviewTreeProvider(mgr);

    mockRunRockyJson.mockRejectedValue(new Error("CLI error"));

    // Should not throw
    await expect(provider.fetchAllBranches()).resolves.toBeUndefined();
  });

  it("remove command updates state and refreshes provider", async () => {
    const mgr = new PreviewStateManager(makeMemento());
    await mgr.add({ branchName: "pr", createdAt: "t" });
    const provider = new PreviewTreeProvider(mgr);

    let fired = false;
    provider.onDidChangeTreeData(() => { fired = true; });

    await mgr.remove("pr");
    provider.refresh();

    expect(fired).toBe(true);
    const roots = await provider.getChildren();
    expect(roots).toHaveLength(0);
  });

  it("empty state shows [] even with project (viewsWelcome takes over)", async () => {
    hasProject = true;
    const mgr = new PreviewStateManager(makeMemento());
    const provider = new PreviewTreeProvider(mgr);
    const roots = await provider.getChildren();
    expect(roots).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// PreviewDiffModelNode status heuristic tests
// ---------------------------------------------------------------------------

describe("PreviewDiffModelNode diffStatus heuristic", () => {
  function makeModel(
    added: string[],
    removed: string[],
    typeChanges: { name: string; from: string; to: string }[],
  ): import("../types/generated").PreviewModelDiff {
    return {
      model_name: "test",
      structural: {
        added_columns: added,
        removed_columns: removed,
        type_changes: typeChanges,
      },
      algorithm: {
        kind: "sampled",
        sampled: { rows_added: 0, rows_changed: 0, rows_removed: 0, samples: [] },
        sampling_window: {
          coverage: "first_n_by_order",
          coverage_warning: false,
          limit: 100,
          ordered_by: "id",
        },
      },
    };
  }

  it("pure added columns → diffStatus = added", () => {
    const node = new PreviewDiffModelNode("br", makeModel(["x"], [], []), "");
    expect(node.diffStatus).toBe("added");
  });

  it("pure removed columns → diffStatus = removed", () => {
    const node = new PreviewDiffModelNode("br", makeModel([], ["x"], []), "");
    expect(node.diffStatus).toBe("removed");
  });

  it("type change only → diffStatus = modified", () => {
    const node = new PreviewDiffModelNode(
      "br",
      makeModel([], [], [{ name: "col", from: "INT", to: "BIGINT" }]),
      "",
    );
    expect(node.diffStatus).toBe("modified");
  });

  it("mixed add + remove → diffStatus = modified", () => {
    const node = new PreviewDiffModelNode("br", makeModel(["a"], ["b"], []), "");
    expect(node.diffStatus).toBe("modified");
  });

  it("no structural changes → diffStatus = unchanged", () => {
    const node = new PreviewDiffModelNode("br", makeModel([], [], []), "");
    expect(node.diffStatus).toBe("unchanged");
  });
});
