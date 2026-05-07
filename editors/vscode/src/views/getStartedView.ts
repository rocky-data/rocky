import * as vscode from "vscode";

/**
 * The Get Started panel itself is rendered via `contributes.viewsWelcome` in
 * package.json — this provider only owns the empty tree (so the welcome
 * markdown shows) and the `rocky.hasProject` context that gates which welcome
 * variant is visible.
 *
 * `rocky.hasProject` is also consumed by Models / Runs / Sources viewsWelcome
 * entries so all five sections stay in sync.
 */
export class GetStartedTreeProvider
  implements vscode.TreeDataProvider<vscode.TreeItem>
{
  private readonly emitter = new vscode.EventEmitter<
    vscode.TreeItem | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(item: vscode.TreeItem): vscode.TreeItem {
    return item;
  }

  getChildren(): vscode.TreeItem[] {
    return [];
  }
}

async function detectProject(): Promise<boolean> {
  const found = await vscode.workspace.findFiles(
    "**/rocky.toml",
    "**/node_modules/**",
    1,
  );
  return found.length > 0;
}

let cachedHasProject = false;

/**
 * Synchronous read of whether a Rocky project (`rocky.toml`) was detected in
 * the workspace. Updated by the file watcher in {@link registerGetStartedView}.
 *
 * Other views (Runs, Sources) use this to skip CLI invocations on workspaces
 * with no Rocky project.
 */
export function hasRockyProject(): boolean {
  return cachedHasProject;
}

const projectChangeEmitter = new vscode.EventEmitter<boolean>();

/** Fires whenever {@link hasRockyProject} flips. */
export const onDidChangeRockyProject: vscode.Event<boolean> =
  projectChangeEmitter.event;

export function registerGetStartedView(
  context: vscode.ExtensionContext,
): GetStartedTreeProvider {
  const provider = new GetStartedTreeProvider();
  const view = vscode.window.createTreeView("rocky.getStarted", {
    treeDataProvider: provider,
    showCollapseAll: false,
  });
  context.subscriptions.push(view);

  const refreshContext = async (): Promise<void> => {
    const has = await detectProject();
    const changed = has !== cachedHasProject;
    cachedHasProject = has;
    await vscode.commands.executeCommand(
      "setContext",
      "rocky.hasProject",
      has,
    );
    provider.refresh();
    if (changed) projectChangeEmitter.fire(has);
  };

  void refreshContext();

  const watcher = vscode.workspace.createFileSystemWatcher("**/rocky.toml");
  watcher.onDidCreate(() => void refreshContext());
  watcher.onDidDelete(() => void refreshContext());
  context.subscriptions.push(watcher);

  context.subscriptions.push(
    vscode.workspace.onDidChangeWorkspaceFolders(
      () => void refreshContext(),
    ),
  );

  return provider;
}
