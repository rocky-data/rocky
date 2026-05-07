import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => {
  class TreeItem {
    public label: string;
    public collapsibleState: number;
    public iconPath: unknown;
    public tooltip: string | undefined;
    public command: unknown;
    public description: string | undefined;
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

  return {
    TreeItem,
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    ThemeIcon,
    ThemeColor,
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
    },
  };
});

vi.mock("../config", () => ({
  getConfig: () => ({
    serverPath: "/usr/local/bin/rocky",
    extraArgs: ["--verbose"],
    inlayHintsEnabled: true,
  }),
  getWorkspaceFolder: () => undefined,
}));

const lspState = { current: { status: "Ready" as const } };
vi.mock("../lspClient", () => ({
  getLspState: () => lspState.current,
  onDidChangeLspState: () => ({ dispose: () => undefined }),
}));

const cliVersion = { value: "1.26.0" as string | undefined };
vi.mock("../rockyCli", () => ({
  clearCliVersionCache: () => undefined,
  getCliVersion: async () => cliVersion.value,
}));

import * as vscode from "vscode";
import { ExtensionInfoTreeProvider } from "../views/extensionInfoView";

function makeContext(): vscode.ExtensionContext {
  return {
    extension: { packageJSON: { version: "1.15.0" } },
    subscriptions: [],
  } as unknown as vscode.ExtensionContext;
}

describe("ExtensionInfoTreeProvider", () => {
  beforeEach(() => {
    cliVersion.value = "1.26.0";
    lspState.current = { status: "Ready" };
  });

  it("returns four root sections in About → Configuration → Project → Logs order", async () => {
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const sections = await provider.getChildren();
    expect(sections.map((s) => s.label)).toEqual([
      "About",
      "Configuration",
      "Project",
      "Logs & Diagnostics",
    ]);
  });

  it("About section includes extension version, CLI version, and LSP status", async () => {
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const [aboutSection] = await provider.getChildren();
    const leaves = await provider.getChildren(aboutSection);

    const labels = leaves.map((l) => l.label);
    expect(labels).toEqual(["Extension", "Rocky CLI", "Language Server"]);

    expect(leaves[0].description).toBe("1.15.0");
    expect(leaves[1].description).toBe("1.26.0");
    expect(leaves[2].description).toBe("Ready");
  });

  it("About surfaces 'not detected' when the CLI version lookup fails", async () => {
    cliVersion.value = undefined;
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const [aboutSection] = await provider.getChildren();
    const leaves = await provider.getChildren(aboutSection);
    expect(leaves[1].description).toBe("not detected");
  });

  it("About reflects the latest LSP state on each refresh", async () => {
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const [aboutSection] = await provider.getChildren();

    lspState.current = { status: "Failed", error: "ENOENT" };
    const leaves = await provider.getChildren(aboutSection);
    expect(leaves[2].description).toBe("Failed");
    expect(leaves[2].tooltip).toBe("ENOENT");
  });

  it("Configuration section reads server path, inlay hints, extra args", async () => {
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const sections = await provider.getChildren();
    const configSection = sections[1];
    const leaves = await provider.getChildren(configSection);

    expect(leaves.map((l) => l.label)).toEqual([
      "Server Path",
      "Inlay Hints",
      "Extra Args",
    ]);
    expect(leaves[0].description).toBe("/usr/local/bin/rocky");
    expect(leaves[1].description).toBe("enabled");
    expect(leaves[2].description).toBe("--verbose");
  });

  it("Logs section exposes commands for output channel, doctor, restart", async () => {
    const provider = new ExtensionInfoTreeProvider(makeContext());
    const sections = await provider.getChildren();
    const logs = sections[3];
    const leaves = await provider.getChildren(logs);

    const commands = leaves.map(
      (l) => (l as { command?: { command: string } }).command?.command,
    );
    expect(commands).toEqual([
      "rocky.openOutputChannel",
      "rocky.doctor",
      "rocky.restartServer",
    ]);
  });
});
