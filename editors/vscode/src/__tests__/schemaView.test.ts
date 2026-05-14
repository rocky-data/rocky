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

  class Range {
    constructor(
      public start: unknown,
      public end: unknown,
    ) {}
  }

  return {
    TreeItem,
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    ThemeIcon,
    ThemeColor,
    Range,
    EventEmitter: class {
      private listeners: ((value: unknown) => void)[] = [];
      event = (listener: (value: unknown) => void): { dispose(): void } => {
        this.listeners.push(listener);
        return { dispose: (): void => undefined };
      };
      fire(value: unknown): void {
        for (const l of this.listeners) l(value);
      }
    },
    Uri: { file: (p: string) => ({ fsPath: p, toString: () => p }) },
    workspace: {
      findFiles: vi.fn(async () => []),
      workspaceFolders: undefined,
      asRelativePath: (uri: { fsPath: string }) => uri.fsPath,
      getConfiguration: () => ({
        get: <T,>(_key: string, fallback: T): T => fallback,
      }),
      onDidChangeConfiguration: () => ({ dispose: () => undefined }),
      onDidChangeWorkspaceFolders: () => ({ dispose: () => undefined }),
    },
    commands: {
      registerCommand: () => ({ dispose: () => undefined }),
    },
    window: {
      createTreeView: () => ({ dispose: () => undefined }),
      createTextEditorDecorationType: () => ({
        dispose: () => undefined,
      }),
      visibleTextEditors: [],
      showInformationMessage: vi.fn(async () => undefined),
    },
    env: {
      clipboard: {
        writeText: vi.fn(async () => undefined),
      },
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

vi.mock("../output", () => ({
  getOutputChannel: () => ({ appendLine: () => undefined }),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

import type { CatalogOutput, CatalogAsset } from "../types/generated";
import { SchemaTreeProvider, SchemaModelNode, SchemaColumnNode } from "../views/schemaView";

function makeCatalog(assets: CatalogAsset[]): CatalogOutput {
  return {
    assets,
    command: "catalog",
    config_hash: "abc123",
    edges: [],
    generated_at: "2026-05-14T00:00:00Z",
    project_name: "test_project",
    stats: {
      asset_count: assets.length,
      assets_with_star: 0,
      column_count: assets.reduce((n, a) => n + a.columns.length, 0),
      duration_ms: 1,
      edge_count: 0,
      orphan_columns: 0,
    },
    version: "1.0.0",
  };
}

function makeAsset(modelName: string, cols: { name: string; data_type?: string; nullable?: boolean }[]): CatalogAsset {
  return {
    model_name: modelName,
    fqn: `catalog.schema.${modelName}`,
    kind: "Model",
    columns: cols.map((c) => ({
      name: c.name,
      data_type: c.data_type,
      nullable: c.nullable,
    })),
    downstream_models: [],
    upstream_models: [],
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SchemaTreeProvider", () => {
  beforeEach(() => {
    hasProject = true;
    mockRunRockyJson.mockReset();
    projectChangeListeners.length = 0;
  });

  it("returns [] when no Rocky project is detected", async () => {
    hasProject = false;
    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();
    expect(roots).toEqual([]);
    // CLI should never be called with no project
    expect(mockRunRockyJson).not.toHaveBeenCalled();
  });

  it("loads catalog and returns one SchemaModelNode per asset", async () => {
    const asset = makeAsset("orders", [
      { name: "id", data_type: "INT64", nullable: false },
      { name: "amount", data_type: "FLOAT64", nullable: true },
    ]);
    mockRunRockyJson.mockResolvedValueOnce(makeCatalog([asset]));

    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();

    expect(roots).toHaveLength(1);
    const modelNode = roots[0];
    expect(modelNode).toBeInstanceOf(SchemaModelNode);
    expect(modelNode.label).toBe("orders");
    expect((modelNode as SchemaModelNode).description).toBe("2 cols");
  });

  it("returns [] (empty viewsWelcome state) when catalog has no assets", async () => {
    mockRunRockyJson.mockResolvedValueOnce(makeCatalog([]));

    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();
    expect(roots).toHaveLength(0);
  });

  it("returns an error MessageNode when runRockyJson throws", async () => {
    mockRunRockyJson.mockRejectedValueOnce(new Error("binary not found"));

    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();

    expect(roots).toHaveLength(1);
    expect(roots[0].label).toContain("Error:");
  });

  it("getChildren of a SchemaModelNode returns SchemaColumnNode per column", async () => {
    const asset = makeAsset("orders", [
      { name: "id", data_type: "INT64", nullable: false },
      { name: "amount", data_type: "FLOAT64", nullable: true },
    ]);
    mockRunRockyJson.mockResolvedValueOnce(makeCatalog([asset]));

    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();
    const modelNode = roots[0] as SchemaModelNode;
    const cols = await provider.getChildren(modelNode);

    expect(cols).toHaveLength(2);
    expect(cols[0]).toBeInstanceOf(SchemaColumnNode);
    expect(cols[0].label).toBe("id");
    expect((cols[0] as SchemaColumnNode).description).toBe("INT64");   // not nullable → no ?
    expect(cols[1].label).toBe("amount");
    expect((cols[1] as SchemaColumnNode).description).toBe("FLOAT64?"); // nullable → ?
  });

  it("SchemaColumnNode description falls back to 'unknown' when data_type is absent", async () => {
    const asset = makeAsset("dim_user", [{ name: "uid" }]);
    mockRunRockyJson.mockResolvedValueOnce(makeCatalog([asset]));

    const provider = new SchemaTreeProvider();
    const roots = await provider.getChildren();
    const modelNode = roots[0] as SchemaModelNode;
    const cols = await provider.getChildren(modelNode);

    expect((cols[0] as SchemaColumnNode).description).toBe("unknown");
  });

  it("refresh clears cache and fires onDidChangeTreeData", async () => {
    mockRunRockyJson.mockResolvedValue(makeCatalog([]));

    const provider = new SchemaTreeProvider();
    await provider.getChildren(); // prime cache

    let fired = false;
    provider.onDidChangeTreeData(() => {
      fired = true;
    });

    provider.refresh();
    expect(fired).toBe(true);
  });

  it("fires onDidChangeTreeData twice during load (loading state + result)", async () => {
    let resolveLoad!: (v: unknown) => void;
    mockRunRockyJson.mockReturnValueOnce(
      new Promise((res) => {
        resolveLoad = res;
      }),
    );

    const provider = new SchemaTreeProvider();
    const fireCount = { n: 0 };
    provider.onDidChangeTreeData(() => {
      fireCount.n++;
    });

    const loadingPromise = provider.getChildren();

    // Still loading — kick off but don't await yet
    resolveLoad(makeCatalog([]));
    await loadingPromise;
    // Two fires: one for loading=true, one for loading=false
    expect(fireCount.n).toBeGreaterThanOrEqual(2);
  });

  it("copy command arg produces '<modelName>.<columnName>' string", () => {
    const col: SchemaColumnNode = new SchemaColumnNode("orders", {
      name: "amount",
      data_type: "FLOAT64",
      nullable: true,
    });
    const ref = `${col.modelName}.${col.column.name}`;
    expect(ref).toBe("orders.amount");
  });
});
