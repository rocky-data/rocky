import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import { getOutputChannel } from "../output";
import type { CatalogResult } from "../types/rockyJson";
import type { CatalogAsset, CatalogColumn } from "../types/generated";
import { hasRockyProject, onDidChangeRockyProject } from "./getStartedView";

// Single module-scoped decoration type so that re-highlighting replaces the
// previous highlight rather than stacking decorations.
const columnHighlightDecoration = vscode.window.createTextEditorDecorationType(
  {
    backgroundColor: new vscode.ThemeColor(
      "editor.findMatchHighlightBackground",
    ),
    borderRadius: "2px",
  },
);

type Node = SchemaModelNode | SchemaColumnNode | MessageNode;

/**
 * Tree provider for the Schema sidebar view.
 *
 * Backs the `rocky.schema` tree view with a two-level tree:
 *   Model (with column count) → Column (with data_type + nullability).
 *
 * Data is fetched from `rocky catalog --output json` on first open or manual
 * refresh and held in memory until the next refresh. Errors are surfaced in
 * the output channel; the empty state (no data) falls through to the
 * `viewsWelcome` content defined in `package.json`.
 */
export class SchemaTreeProvider implements vscode.TreeDataProvider<Node> {
  private readonly emitter = new vscode.EventEmitter<Node | undefined | void>();
  readonly onDidChangeTreeData = this.emitter.event;

  private assets: CatalogAsset[] | undefined;
  private loadError: string | undefined;
  private loading = false;

  refresh(): void {
    this.assets = undefined;
    this.loadError = undefined;
    this.loading = false;
    this.emitter.fire();
  }

  getTreeItem(node: Node): vscode.TreeItem {
    return node;
  }

  async getChildren(node?: Node): Promise<Node[]> {
    if (!node) {
      // No project → viewsWelcome takes over.
      if (!hasRockyProject()) return [];

      if (this.loading) {
        return [new MessageNode("Loading catalog…", "loading~spin")];
      }

      if (this.assets === undefined) {
        await this.loadCatalog();
      }

      if (this.loadError) {
        return [new MessageNode(`Error: ${this.loadError}`, "error")];
      }

      const assets = this.assets ?? [];
      // Empty: fall through to viewsWelcome (with "Run rocky catalog" CTA).
      if (assets.length === 0) return [];

      return assets.map((a) => new SchemaModelNode(a));
    }

    if (node instanceof SchemaModelNode) {
      return node.asset.columns.map(
        (col) => new SchemaColumnNode(node.asset.model_name, col),
      );
    }

    return [];
  }

  private async loadCatalog(): Promise<void> {
    this.loading = true;
    this.emitter.fire();
    try {
      const result = await runRockyJson<CatalogResult>([
        "catalog",
        "--output",
        "json",
      ]);
      this.assets = result.assets ?? [];
    } catch (err) {
      this.assets = [];
      this.loadError = (err as Error).message;
      getOutputChannel().appendLine(
        `[Schema] rocky catalog failed: ${this.loadError}`,
      );
    } finally {
      this.loading = false;
      this.emitter.fire();
    }
  }
}

// ---------------------------------------------------------------------------
// Tree item classes
// ---------------------------------------------------------------------------

export class SchemaModelNode extends vscode.TreeItem {
  override readonly contextValue = "rockySchemaModel";

  constructor(public readonly asset: CatalogAsset) {
    super(asset.model_name, vscode.TreeItemCollapsibleState.Collapsed);
    const count = asset.columns.length;
    this.description = `${count} col${count === 1 ? "" : "s"}`;
    this.tooltip = asset.fqn;
    this.iconPath = new vscode.ThemeIcon("database");
  }
}

export class SchemaColumnNode extends vscode.TreeItem {
  override readonly contextValue = "rockySchemaColumn";

  constructor(
    public readonly modelName: string,
    public readonly column: CatalogColumn,
  ) {
    super(column.name, vscode.TreeItemCollapsibleState.None);

    const typePart = column.data_type ?? "unknown";
    const nullPart = column.nullable === true ? "?" : "";
    this.description = `${typePart}${nullPart}`;
    this.tooltip = column.data_type ?? "type unknown";

    this.iconPath = new vscode.ThemeIcon("symbol-field");

    // Clicking a column node reveals it in the editor.
    this.command = {
      title: "Reveal column in editor",
      command: "rocky.revealColumnInEditor",
      arguments: [modelName, column.name],
    };
  }
}

class MessageNode extends vscode.TreeItem {
  override readonly contextValue = "rockySchemaMessage";

  constructor(label: string, icon: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
  }
}

// ---------------------------------------------------------------------------
// Column-highlight helper
// ---------------------------------------------------------------------------

/**
 * Opens the model file matching the glob pattern for a given model name in the
 * active editor and highlights every occurrence of columnName using a
 * word-boundary regex. Clears the previous decoration before applying the new
 * one.
 *
 * Silently no-ops if the file cannot be found (e.g. for source assets).
 */
async function revealColumnInEditor(
  modelName: string,
  columnName: string,
): Promise<void> {
  const files = await vscode.workspace.findFiles(
    `**/models/**/${modelName}.{rocky,sql}`,
    "**/node_modules/**",
    1,
  );

  // Clear any previous highlight regardless of whether we find a new file.
  for (const editor of vscode.window.visibleTextEditors) {
    editor.setDecorations(columnHighlightDecoration, []);
  }

  if (files.length === 0) return;

  const doc = await vscode.workspace.openTextDocument(files[0]);
  const editor = await vscode.window.showTextDocument(doc);

  const text = doc.getText();
  const ranges: vscode.Range[] = [];
  // Word-boundary match so `id` doesn't highlight inside `order_id`.
  const pattern = new RegExp(`\\b${escapeRegExp(columnName)}\\b`, "g");
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(text)) !== null) {
    const start = doc.positionAt(match.index);
    const end = doc.positionAt(match.index + match[0].length);
    ranges.push(new vscode.Range(start, end));
  }

  editor.setDecorations(columnHighlightDecoration, ranges);
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

export function registerSchemaView(
  context: vscode.ExtensionContext,
): SchemaTreeProvider {
  const provider = new SchemaTreeProvider();

  const view = vscode.window.createTreeView("rocky.schema", {
    treeDataProvider: provider,
  });
  context.subscriptions.push(view);

  // Refresh toolbar button.
  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshSchema", () =>
      provider.refresh(),
    ),
  );

  // Copy <modelName>.<columnName> to clipboard (right-click context menu).
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "rocky.copyColumnReference",
      (node: SchemaColumnNode) => {
        const ref = `${node.modelName}.${node.column.name}`;
        void vscode.env.clipboard.writeText(ref).then(() => {
          void vscode.window.showInformationMessage(
            `Copied: ${ref}`,
          );
        });
      },
    ),
  );

  // Internal command: click column → reveal in editor.
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "rocky.revealColumnInEditor",
      (modelName: string, columnName: string) => {
        void revealColumnInEditor(modelName, columnName);
      },
    ),
  );

  // Refresh when a rocky.toml appears or disappears.
  context.subscriptions.push(onDidChangeRockyProject(() => provider.refresh()));

  return provider;
}
