import * as vscode from "vscode";

interface HelpLink {
  label: string;
  url: string;
  icon: string;
}

const HELP_LINKS: readonly HelpLink[] = [
  {
    label: "Documentation",
    url: "https://github.com/rocky-data/rocky#readme",
    icon: "book",
  },
  {
    label: "Report a Bug",
    url: "https://github.com/rocky-data/rocky/issues/new",
    icon: "bug",
  },
  {
    label: "View on Marketplace",
    url: "https://marketplace.visualstudio.com/items?itemName=rocky-data.rocky",
    icon: "extensions",
  },
  {
    label: "Releases & Changelog",
    url: "https://github.com/rocky-data/rocky/releases?q=engine",
    icon: "tag",
  },
  {
    label: "Source Code",
    url: "https://github.com/rocky-data/rocky",
    icon: "github",
  },
];

class HelpItem extends vscode.TreeItem {
  override readonly contextValue = "rockyHelpItem";

  constructor(link: HelpLink) {
    super(link.label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(link.icon);
    this.tooltip = link.url;
    this.command = {
      command: "vscode.open",
      title: "Open",
      arguments: [vscode.Uri.parse(link.url)],
    };
  }
}

/**
 * Static tree of external help links — docs, issue tracker, marketplace,
 * releases, source code. Each leaf opens a URL via the built-in `vscode.open`
 * command.
 */
export class HelpTreeProvider
  implements vscode.TreeDataProvider<vscode.TreeItem>
{
  private readonly emitter = new vscode.EventEmitter<
    vscode.TreeItem | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  getTreeItem(item: vscode.TreeItem): vscode.TreeItem {
    return item;
  }

  getChildren(): vscode.TreeItem[] {
    return HELP_LINKS.map((link) => new HelpItem(link));
  }
}

export function registerHelpView(
  context: vscode.ExtensionContext,
): HelpTreeProvider {
  const provider = new HelpTreeProvider();
  const view = vscode.window.createTreeView("rocky.help", {
    treeDataProvider: provider,
    showCollapseAll: false,
  });
  context.subscriptions.push(view);
  return provider;
}
