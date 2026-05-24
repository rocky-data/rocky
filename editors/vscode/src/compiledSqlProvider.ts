import * as path from "path";
import * as vscode from "vscode";
import { resolveProjectRoot } from "./config";
import { resolveModelName } from "./commands/ui";
import { RockyCliError, runRockyJson } from "./rockyCli";
import type { CompileOutput } from "./types/generated/compile";

/**
 * Virtual-document scheme for the live compiled-SQL panel. A read-only
 * `rocky-compiled:/<model>.sql` document renders the model's macro-expanded
 * SQL beside the source and refreshes on save. Using a `.sql` URI suffix means
 * VS Code applies SQL syntax highlighting for free — no webview needed.
 */
export const COMPILED_SCHEME = "rocky-compiled";

/** Derive the model name from a `rocky-compiled:/<model>.sql` URI. */
function modelFromUri(uri: vscode.Uri): string {
  return path.basename(uri.path).replace(/\.sql$/, "");
}

/** Build the virtual URI for a model's compiled SQL. */
function compiledUri(model: string): vscode.Uri {
  return vscode.Uri.parse(`${COMPILED_SCHEME}:/${encodeURIComponent(model)}.sql`);
}

export class CompiledSqlProvider implements vscode.TextDocumentContentProvider {
  private readonly emitter = new vscode.EventEmitter<vscode.Uri>();
  readonly onDidChange = this.emitter.event;

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const model = modelFromUri(uri);
    if (!model) return "-- (no model selected)";
    try {
      // `--expand-macros` is mandatory — without it `expanded_sql` is empty.
      const out = await runRockyJson<CompileOutput>(
        ["compile", "--model", model, "--expand-macros", "--output", "json"],
        { cwd: resolveProjectRoot() },
      );
      const sql = out.expanded_sql?.[model];
      if (sql && sql.trim().length > 0) return sql;
      return [
        `-- No compiled SQL for '${model}'.`,
        "-- It may have compile errors, or be a config-only replication model",
        "-- (replication SQL is generated at run time, not from a model file).",
      ].join("\n");
    } catch (err) {
      const message =
        err instanceof RockyCliError ? err.stderr.trim() || err.message : String(err);
      return `-- Failed to compile '${model}':\n-- ${message.replace(/\n/g, "\n-- ")}`;
    }
  }

  /** Force a refresh of the compiled doc for `model` (if one is open). */
  refresh(model: string): void {
    this.emitter.fire(compiledUri(model));
  }
}

/** Resolve a model name from a command arg or the active editor. */
function resolveModel(arg: unknown): string | undefined {
  const fromArg = resolveModelName(arg);
  if (fromArg) return fromArg;
  const editor = vscode.window.activeTextEditor;
  if (!editor) {
    vscode.window.showWarningMessage("Open a Rocky model file first.");
    return undefined;
  }
  const base = path.basename(editor.document.fileName).replace(/\.(rocky|sql)$/, "");
  return base.length > 0 ? base : undefined;
}

/**
 * Register the compiled-SQL content provider, the save-triggered refresh, and
 * the `rocky.showCompiledSql` command.
 */
export function registerCompiledSqlProvider(context: vscode.ExtensionContext): void {
  const provider = new CompiledSqlProvider();
  context.subscriptions.push(
    vscode.workspace.registerTextDocumentContentProvider(COMPILED_SCHEME, provider),
    vscode.workspace.onDidSaveTextDocument((doc) => {
      // Refresh the compiled view for the saved model (no-op if not open).
      if (!/\.(rocky|sql)$/.test(doc.fileName)) return;
      const model = path.basename(doc.fileName).replace(/\.(rocky|sql)$/, "");
      provider.refresh(model);
    }),
    vscode.commands.registerCommand("rocky.showCompiledSql", showCompiledSql),
  );
}

/** `rocky.showCompiledSql` — open the model's compiled SQL beside the source. */
export async function showCompiledSql(arg?: unknown): Promise<void> {
  const model = resolveModel(arg);
  if (!model) return;
  const doc = await vscode.workspace.openTextDocument(compiledUri(model));
  await vscode.window.showTextDocument(doc, {
    viewColumn: vscode.ViewColumn.Beside,
    preview: true,
    preserveFocus: true,
  });
}
