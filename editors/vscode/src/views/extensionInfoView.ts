import * as vscode from "vscode";
import { getConfig } from "../config";
import { getLspState, onDidChangeLspState } from "../lspClient";
import type { LspStatus } from "../lspClient";
import { clearCliVersionCache, getCliVersion } from "../rockyCli";

type Section = "about" | "config" | "project" | "logs";

class SectionItem extends vscode.TreeItem {
  override readonly contextValue = "rockyInfoSection";

  constructor(
    public readonly section: Section,
    label: string,
    icon: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.Expanded);
    this.iconPath = new vscode.ThemeIcon(icon);
  }
}

class LeafItem extends vscode.TreeItem {
  override readonly contextValue = "rockyInfoLeaf";

  constructor(
    label: string,
    description: string | undefined,
    icon: string | vscode.ThemeIcon,
    command?: vscode.Command,
    tooltip?: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    if (description !== undefined) this.description = description;
    this.iconPath = typeof icon === "string" ? new vscode.ThemeIcon(icon) : icon;
    if (command) this.command = command;
    if (tooltip) this.tooltip = tooltip;
  }
}

type Node = SectionItem | LeafItem;

/**
 * Tree view backing the "Extension Info" sidebar section. Surfaces extension
 * version, Rocky CLI version, LSP state, configuration, detected project
 * paths, and quick links to logs and diagnostics.
 *
 * Refreshes on:
 *  - LSP state transitions (via {@link onDidChangeLspState})
 *  - Configuration changes under `rocky.*`
 *  - Workspace folder changes
 *  - User invoking `rocky.refreshInfo`
 */
export class ExtensionInfoTreeProvider implements vscode.TreeDataProvider<Node> {
  private readonly emitter = new vscode.EventEmitter<Node | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;

  constructor(private readonly context: vscode.ExtensionContext) {}

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(item: Node): vscode.TreeItem {
    return item;
  }

  async getChildren(element?: Node): Promise<Node[]> {
    if (!element) {
      return [
        new SectionItem("about", "About", "info"),
        new SectionItem("config", "Configuration", "settings-gear"),
        new SectionItem("project", "Project", "folder"),
        new SectionItem("logs", "Logs & Diagnostics", "output"),
      ];
    }
    if (element instanceof SectionItem) {
      switch (element.section) {
        case "about":
          return await this.aboutLeaves();
        case "config":
          return this.configLeaves();
        case "project":
          return await this.projectLeaves();
        case "logs":
          return this.logsLeaves();
      }
    }
    return [];
  }

  private async aboutLeaves(): Promise<LeafItem[]> {
    const extVersion = String(
      this.context.extension.packageJSON.version ?? "unknown",
    );
    const cliVersion = await getCliVersion();
    const lsp = getLspState();
    return [
      new LeafItem("Extension", extVersion, "tag"),
      new LeafItem(
        "Rocky CLI",
        cliVersion ?? "not detected",
        cliVersion ? "package" : "warning",
        undefined,
        cliVersion ? undefined : "Rocky CLI was not found on PATH or at rocky.server.path.",
      ),
      new LeafItem(
        "Language Server",
        lspStatusLabel(lsp.status),
        lspIcon(lsp.status),
        { command: "rocky.restartServer", title: "Restart Language Server" },
        lsp.error,
      ),
    ];
  }

  private configLeaves(): LeafItem[] {
    const cfg = getConfig();
    return [
      new LeafItem(
        "Server Path",
        cfg.serverPath,
        "terminal",
        openSettingCommand("rocky.server.path"),
      ),
      new LeafItem(
        "Inlay Hints",
        cfg.inlayHintsEnabled ? "enabled" : "disabled",
        "symbol-parameter",
        openSettingCommand("rocky.inlayHints.enabled"),
      ),
      new LeafItem(
        "Extra Args",
        cfg.extraArgs.length > 0 ? cfg.extraArgs.join(" ") : "(none)",
        "symbol-array",
        openSettingCommand("rocky.server.extraArgs"),
      ),
    ];
  }

  private async projectLeaves(): Promise<LeafItem[]> {
    const tomls = await vscode.workspace.findFiles(
      "**/rocky.toml",
      "**/node_modules/**",
      5,
    );
    const folder =
      vscode.workspace.workspaceFolders?.[0]?.name ?? "(no folder open)";
    const items: LeafItem[] = [];
    if (tomls.length === 0) {
      items.push(new LeafItem("rocky.toml", "not detected", "warning"));
    } else {
      for (const uri of tomls) {
        const rel = vscode.workspace.asRelativePath(uri);
        items.push(
          new LeafItem(
            "rocky.toml",
            rel,
            "file-code",
            {
              command: "vscode.open",
              title: "Open rocky.toml",
              arguments: [uri],
            },
            uri.fsPath,
          ),
        );
      }
    }
    items.push(new LeafItem("Workspace", folder, "folder"));
    return items;
  }

  private logsLeaves(): LeafItem[] {
    return [
      new LeafItem(
        "Show Output Channel",
        undefined,
        "output",
        { command: "rocky.openOutputChannel", title: "Show Output Channel" },
      ),
      new LeafItem(
        "Run Health Check",
        undefined,
        "pulse",
        { command: "rocky.doctor", title: "Run Doctor" },
      ),
      new LeafItem(
        "Restart Language Server",
        undefined,
        "debug-restart",
        { command: "rocky.restartServer", title: "Restart Language Server" },
      ),
    ];
  }
}

function openSettingCommand(setting: string): vscode.Command {
  return {
    command: "workbench.action.openSettings",
    title: "Open Setting",
    arguments: [setting],
  };
}

function lspStatusLabel(status: LspStatus): string {
  return status;
}

function lspIcon(status: LspStatus): vscode.ThemeIcon {
  switch (status) {
    case "Ready":
      return new vscode.ThemeIcon(
        "check",
        new vscode.ThemeColor("testing.iconPassed"),
      );
    case "Starting":
    case "Restarting":
      return new vscode.ThemeIcon("loading~spin");
    case "Stopped":
      return new vscode.ThemeIcon("circle-slash");
    case "Failed":
      return new vscode.ThemeIcon(
        "error",
        new vscode.ThemeColor("testing.iconFailed"),
      );
    default:
      return new vscode.ThemeIcon("question");
  }
}

export function registerExtensionInfoView(
  context: vscode.ExtensionContext,
): ExtensionInfoTreeProvider {
  const provider = new ExtensionInfoTreeProvider(context);
  const view = vscode.window.createTreeView("rocky.info", {
    treeDataProvider: provider,
    showCollapseAll: false,
  });
  context.subscriptions.push(view);

  context.subscriptions.push(onDidChangeLspState(() => provider.refresh()));

  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("rocky")) provider.refresh();
    }),
  );

  context.subscriptions.push(
    vscode.workspace.onDidChangeWorkspaceFolders(() => provider.refresh()),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshInfo", () => {
      clearCliVersionCache();
      provider.refresh();
    }),
  );

  return provider;
}
