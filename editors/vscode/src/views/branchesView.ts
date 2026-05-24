import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import type { BranchEntry, BranchListOutput } from "../types/generated/branch_list";
import { hasRockyProject, onDidChangeRockyProject } from "./getStartedView";

/** One-line secondary label for a branch row (its creator). */
export function branchDescription(entry: BranchEntry): string {
  return entry.created_by;
}

/** Hover tooltip: schema prefix, creator, timestamp, and description. */
export function branchTooltip(entry: BranchEntry): string {
  const lines = [
    `Branch: ${entry.name}`,
    `Schema prefix: ${entry.schema_prefix}`,
    `Created by: ${entry.created_by}`,
    `Created at: ${entry.created_at}`,
  ];
  if (entry.description) lines.push(`\n${entry.description}`);
  return lines.join("\n");
}

class BranchTreeItem extends vscode.TreeItem {
  readonly branchName: string;
  constructor(entry: BranchEntry) {
    super(entry.name, vscode.TreeItemCollapsibleState.None);
    this.branchName = entry.name;
    this.description = branchDescription(entry);
    this.tooltip = branchTooltip(entry);
    this.contextValue = "rockyBranch";
    this.iconPath = new vscode.ThemeIcon("git-branch");
  }
}

class MessageItem extends vscode.TreeItem {
  constructor(message: string) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.contextValue = "rockyBranchMessage";
  }
}

type Node = BranchTreeItem | MessageItem;

/**
 * Tree view of registered Rocky branches, backed by `rocky branch list
 * --output json`. Read-only; per-branch actions (approve / promote) dispatch to
 * the existing branch commands from the item context menu.
 */
export class BranchesTreeProvider implements vscode.TreeDataProvider<Node> {
  private readonly emitter = new vscode.EventEmitter<Node | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;

  private cache: BranchEntry[] | undefined;
  private loadError: string | undefined;

  refresh(): void {
    this.cache = undefined;
    this.loadError = undefined;
    this.emitter.fire();
  }

  getTreeItem(element: Node): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: Node): Promise<Node[]> {
    if (element) return [];
    if (!hasRockyProject()) return [];

    if (this.cache === undefined && this.loadError === undefined) {
      try {
        const result = await runRockyJson<BranchListOutput>([
          "branch",
          "list",
          "--output",
          "json",
        ]);
        this.cache = result.branches ?? [];
      } catch (err) {
        this.loadError = err instanceof Error ? err.message : String(err);
      }
    }

    if (this.loadError) return [new MessageItem("Failed to list branches")];
    const branches = this.cache ?? [];
    if (branches.length === 0) {
      return [new MessageItem("No branches — create one with rocky branch create")];
    }
    return branches.map((b) => new BranchTreeItem(b));
  }
}

export function registerBranchesView(
  context: vscode.ExtensionContext,
): BranchesTreeProvider {
  const provider = new BranchesTreeProvider();
  const view = vscode.window.createTreeView("rocky.branches", {
    treeDataProvider: provider,
  });
  context.subscriptions.push(
    view,
    vscode.commands.registerCommand("rocky.refreshBranches", () =>
      provider.refresh(),
    ),
    onDidChangeRockyProject(() => provider.refresh()),
  );
  return provider;
}
