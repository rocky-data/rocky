import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class TreeItem {
    public label: string;
    public collapsibleState: number;
    public iconPath: unknown;
    public tooltip: string | undefined;
    public command: unknown;
    constructor(label: string, collapsibleState: number) {
      this.label = label;
      this.collapsibleState = collapsibleState;
    }
  }

  class ThemeIcon {
    constructor(public id: string) {}
  }

  return {
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
    TreeItem,
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    ThemeIcon,
    Uri: { parse: (s: string) => ({ toString: () => s, scheme: "https" }) },
    window: {
      createTreeView: () => ({ dispose: () => undefined }),
    },
  };
});

// Import must follow vi.mock so the mocked module is bound.
import { HelpTreeProvider } from "../views/helpView";

describe("HelpTreeProvider", () => {
  it("returns five leaf links with stable labels", () => {
    const provider = new HelpTreeProvider();
    const items = provider.getChildren();
    expect(items).toHaveLength(5);

    const labels = items.map((it) => it.label);
    expect(labels).toEqual([
      "Documentation",
      "Report a Bug",
      "View on Marketplace",
      "Releases & Changelog",
      "Source Code",
    ]);
  });

  it("attaches a vscode.open command to every leaf", () => {
    const provider = new HelpTreeProvider();
    for (const item of provider.getChildren()) {
      const cmd = (item as {
        command?: { command: string; arguments: unknown[] };
      }).command;
      expect(cmd?.command).toBe("vscode.open");
      expect(cmd?.arguments).toBeDefined();
      expect((cmd?.arguments as unknown[])?.length).toBe(1);
    }
  });

  it("passes through getTreeItem unchanged", () => {
    const provider = new HelpTreeProvider();
    const items = provider.getChildren();
    expect(provider.getTreeItem(items[0])).toBe(items[0]);
  });
});
