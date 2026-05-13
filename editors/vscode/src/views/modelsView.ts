import * as path from "path";
import * as vscode from "vscode";

const MODELS_GLOB = "**/models/**/*.{rocky,sql}";

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
    const label = path.basename(
      resourceUri.fsPath,
      path.extname(resourceUri.fsPath),
    );
    super(label, vscode.TreeItemCollapsibleState.None);
    this.label = label;
    this.id = vscode.workspace.asRelativePath(resourceUri);
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

  // Debounce so rapid create/delete bursts (e.g. branch switches) don't
  // trigger a refresh per file. `onDidChange` is intentionally skipped — the
  // file list doesn't change on saves and the existing tree items don't
  // display body contents.
  let pending: NodeJS.Timeout | undefined;
  const scheduleRefresh = (): void => {
    if (pending) clearTimeout(pending);
    pending = setTimeout(() => {
      pending = undefined;
      provider.refresh();
    }, 500);
  };

  const watcher = vscode.workspace.createFileSystemWatcher(MODELS_GLOB);
  watcher.onDidCreate(scheduleRefresh);
  watcher.onDidDelete(scheduleRefresh);
  context.subscriptions.push(watcher);
  context.subscriptions.push({
    dispose() {
      if (pending) clearTimeout(pending);
    },
  });

  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshModels", () =>
      provider.refresh(),
    ),
  );

  return provider;
}
