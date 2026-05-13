import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import type { DiscoverResult, DiscoverSource } from "../types/rockyJson";
import { hasRockyProject, onDidChangeRockyProject } from "./getStartedView";

type Table = NonNullable<DiscoverSource["tables"]>[number];

type Node = SourceTypeGroupNode | SourceNode | TableNode | MessageNode;

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
      // Skip discovery when there's no Rocky project — the viewsWelcome
      // already prompts the user to open one, and we don't want to flash an
      // error from the CLI complaining about a missing rocky.toml.
      if (!hasRockyProject()) {
        return [];
      }
      if (this.loading) {
        return [makeMessage("Discovering sources…", "loading~spin")];
      }
      if (this.cache === undefined) {
        await this.loadSources();
      }
      if (this.loadError) {
        return [makeMessage(`Error: ${this.loadError}`, "error")];
      }
      const sources = this.cache ?? [];
      // Empty cache: return [] so the viewsWelcome content for rocky.sources
      // (with its "Discover Sources" CTA) takes over.
      if (sources.length === 0) return [];
      return groupBySourceType(sources);
    }

    if (node instanceof SourceTypeGroupNode) {
      // If only one source in the group, show its tables directly.
      if (node.sources.length === 1) {
        return (node.sources[0].tables ?? []).map((t) => new TableNode(t));
      }
      return node.sources.map((s) => new SourceNode(s));
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

/**
 * Groups sources by `source_type` and returns collapsible parent nodes.
 * Source types with a single member collapse directly to their table children.
 */
function groupBySourceType(sources: DiscoverSource[]): SourceTypeGroupNode[] {
  const groups = new Map<string, DiscoverSource[]>();
  for (const s of sources) {
    const key = s.source_type ?? "(unknown)";
    const existing = groups.get(key);
    if (existing) {
      existing.push(s);
    } else {
      groups.set(key, [s]);
    }
  }
  return Array.from(groups.entries()).map(
    ([type, srcs]) => new SourceTypeGroupNode(type, srcs),
  );
}

/**
 * Top-level group node for a `source_type` (e.g. "fivetran", "airbyte").
 * Shows total table count and the most-recent last_sync_at across members.
 */
class SourceTypeGroupNode extends vscode.TreeItem {
  override readonly contextValue = "rockySourceGroup";

  constructor(
    public readonly sourceType: string,
    public readonly sources: DiscoverSource[],
  ) {
    super(sourceType, vscode.TreeItemCollapsibleState.Collapsed);

    const totalTables = sources.reduce(
      (n, s) => n + (s.tables?.length ?? 0),
      0,
    );
    const connectorCount = sources.length;
    this.description =
      connectorCount === 1
        ? `${totalTables} table${totalTables === 1 ? "" : "s"}`
        : `${connectorCount} connectors · ${totalTables} table${totalTables === 1 ? "" : "s"}`;

    // Surface the most-recent sync time across all sources in this group.
    const syncTimes = sources
      .map((s) => s.last_sync_at)
      .filter((t): t is string => typeof t === "string" && t.length > 0)
      .sort()
      .reverse();
    if (syncTimes.length > 0) {
      this.tooltip = `Last synced: ${syncTimes[0]}`;
    }

    this.iconPath = new vscode.ThemeIcon("layers");
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

  context.subscriptions.push(
    onDidChangeRockyProject(() => provider.refresh()),
  );

  return provider;
}
