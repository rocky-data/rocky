import * as path from "path";
import * as vscode from "vscode";

const MODELS_GLOB = "**/models/**/*.rocky";

/**
 * Tree view for the Rocky models in the workspace. Items are discovered by
 * file glob (cheap, no compile required) and clicking an item opens the
 * source file. Right-click context menu (declared in package.json) routes to
 * the existing rocky.* commands using the model name.
 */
export class ModelsTreeProvider
  implements vscode.TreeDataProvider<ModelTreeItem>
{
  private readonly emitter = new vscode.EventEmitter<
    ModelTreeItem | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(element: ModelTreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(
    element?: ModelTreeItem,
  ): Promise<ModelTreeItem[]> {
    if (element) return [];
    const files = await vscode.workspace.findFiles(MODELS_GLOB);
    return files
      .map((uri) => new ModelTreeItem(uri))
      .sort((a, b) => a.label.localeCompare(b.label));
  }
}

export class ModelTreeItem extends vscode.TreeItem {
  override readonly label: string;
  override readonly contextValue = "rockyModel";

  constructor(public readonly resourceUri: vscode.Uri) {
    const label = path.basename(resourceUri.fsPath, ".rocky");
    super(label, vscode.TreeItemCollapsibleState.None);
    this.label = label;
    this.id = label;
    this.tooltip = resourceUri.fsPath;
    this.description = vscode.workspace.asRelativePath(resourceUri);
    this.command = {
      command: "vscode.open",
      title: "Open Model",
      arguments: [resourceUri],
    };
    this.iconPath = new vscode.ThemeIcon("symbol-class");
  }
}

export function registerModelsView(
  context: vscode.ExtensionContext,
): ModelsTreeProvider {
  const provider = new ModelsTreeProvider();
  const view = vscode.window.createTreeView("rocky.models", {
    treeDataProvider: provider,
    showCollapseAll: false,
  });
  context.subscriptions.push(view);

  const watcher = vscode.workspace.createFileSystemWatcher(MODELS_GLOB);
  watcher.onDidCreate(() => provider.refresh());
  watcher.onDidDelete(() => provider.refresh());
  watcher.onDidChange(() => provider.refresh());
  context.subscriptions.push(watcher);

  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshModels", () =>
      provider.refresh(),
    ),
  );

  return provider;
}
