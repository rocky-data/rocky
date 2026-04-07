import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import type { DiscoverResult, DiscoverSource } from "../types/rockyJson";

type Table = NonNullable<DiscoverSource["tables"]>[number];

type Node = SourceNode | TableNode | MessageNode;

/**
 * Tree view for sources discovered by `rocky discover --output json`. Loading
 * is lazy: the CLI is only invoked the first time the user expands the view
 * (or hits refresh) because discovery hits external APIs and can take 10-60s.
 */
export class SourcesTreeProvider implements vscode.TreeDataProvider<Node> {
  private readonly emitter = new vscode.EventEmitter<Node | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;

  private cache: DiscoverSource[] | undefined;
  private loadError: string | undefined;
  private loading = false;

  refresh(): void {
    this.cache = undefined;
    this.loadError = undefined;
    this.loading = false;
    this.emitter.fire();
  }

  getTreeItem(node: Node): vscode.TreeItem {
    return node;
  }

  async getChildren(node?: Node): Promise<Node[]> {
    if (!node) {
      if (this.loading) {
        return [makeMessage("Discovering sources…", "loading~spin")];
      }
      if (this.cache === undefined) {
        await this.loadSources();
      }
      if (this.loadError) {
        return [makeMessage(`Error: ${this.loadError}`, "error")];
      }
      if (!this.cache || this.cache.length === 0) {
        return [makeMessage("No sources discovered", "info")];
      }
      return this.cache.map((s) => new SourceNode(s));
    }

    if (node instanceof SourceNode) {
      return (node.source.tables ?? []).map((t) => new TableNode(t));
    }

    return [];
  }

  private async loadSources(): Promise<void> {
    this.loading = true;
    this.emitter.fire();
    try {
      const result = await runRockyJson<DiscoverResult>([
        "discover",
        "--output",
        "json",
      ]);
      this.cache = result.sources ?? [];
    } catch (err) {
      this.cache = [];
      this.loadError = (err as Error).message;
    } finally {
      this.loading = false;
      this.emitter.fire();
    }
  }
}

class SourceNode extends vscode.TreeItem {
  override readonly contextValue = "rockySource";

  constructor(public readonly source: DiscoverSource) {
    const label = describeSource(source);
    super(label, vscode.TreeItemCollapsibleState.Collapsed);
    const tableCount = source.tables?.length ?? 0;
    this.description = `${tableCount} table${tableCount === 1 ? "" : "s"}`;
    this.tooltip = source.last_sync_at
      ? `Last synced: ${source.last_sync_at}`
      : undefined;
    this.iconPath = new vscode.ThemeIcon("database");
  }
}

class TableNode extends vscode.TreeItem {
  override readonly contextValue = "rockyTable";

  constructor(table: Table) {
    super(table.name, vscode.TreeItemCollapsibleState.None);
    if (typeof table.row_count === "number") {
      this.description = `${table.row_count.toLocaleString()} rows`;
    }
    this.iconPath = new vscode.ThemeIcon("table");
  }
}

class MessageNode extends vscode.TreeItem {
  override readonly contextValue = "rockyMessage";

  constructor(label: string, icon: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
  }
}

function makeMessage(label: string, icon: string): MessageNode {
  return new MessageNode(label, icon);
}

function describeSource(source: DiscoverSource): string {
  if (source.components) {
    const parts = Object.values(source.components).filter(Boolean);
    if (parts.length > 0) return parts.join(" / ");
  }
  return source.id ?? source.source_type ?? "(unknown source)";
}

export function registerSourcesView(
  context: vscode.ExtensionContext,
): SourcesTreeProvider {
  const provider = new SourcesTreeProvider();
  const view = vscode.window.createTreeView("rocky.sources", {
    treeDataProvider: provider,
  });
  context.subscriptions.push(view);

  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshSources", () =>
      provider.refresh(),
    ),
  );

  return provider;
}
