import * as path from "path";
import * as vscode from "vscode";
import { getConfig, resolveProjectRoot } from "../config";
import { RockyCliError, runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type { PreviewRowsOutput } from "../types/generated/preview_rows";
import { getQueryResultsProvider } from "../views";
import {
  buildPreviewArgs,
  parseErrorEnvelope,
  toGridData,
} from "./previewRowsCore";
import { confirmAction, ensureWorkspace, resolveModelName } from "./ui";

/** Model name from an explicit command arg (CodeLens / tree item), if any. */
function explicitModel(arg: unknown): string | undefined {
  return resolveModelName(arg);
}

/** Model name derived from the active editor's file name. */
function activeEditorModel(): string | undefined {
  const editor = vscode.window.activeTextEditor;
  if (!editor) {
    vscode.window.showWarningMessage("Open a Rocky model file first.");
    return undefined;
  }
  const base = path.basename(editor.document.fileName).replace(/\.(rocky|sql)$/, "");
  return base.length > 0 ? base : undefined;
}

/**
 * Find the CTE whose range contains the active editor's cursor, for `.sql`
 * models, via the LSP's document symbols. Returns undefined for `.rocky` files
 * (which surface pipeline steps, not CTEs) or when the cursor isn't in a CTE.
 */
async function cteUnderCursor(): Promise<string | undefined> {
  const editor = vscode.window.activeTextEditor;
  if (!editor || !editor.document.fileName.endsWith(".sql")) return undefined;
  const pos = editor.selection.active;
  try {
    const symbols = await vscode.commands.executeCommand<vscode.DocumentSymbol[]>(
      "vscode.executeDocumentSymbolProvider",
      editor.document.uri,
    );
    return findCteContaining(symbols ?? [], pos);
  } catch {
    return undefined;
  }
}

function findCteContaining(
  symbols: vscode.DocumentSymbol[],
  pos: vscode.Position,
): string | undefined {
  for (const s of symbols) {
    if (s.detail === "CTE" && s.range.contains(pos)) return s.name;
    if (s.children && s.children.length > 0) {
      const nested = findCteContaining(s.children, pos);
      if (nested) return nested;
    }
  }
  return undefined;
}

/**
 * `rocky.previewModel` — preview output rows. When invoked without an explicit
 * model (the Cmd/Ctrl+Enter keybinding or the command palette) it's
 * cursor-aware: if the cursor sits inside a CTE of a `.sql` model, that CTE is
 * previewed instead of the whole model. The model-level "Preview" CodeLens
 * passes the model name explicitly and always previews the full model.
 */
export async function previewModel(arg?: unknown): Promise<void> {
  const explicit = explicitModel(arg);
  const model = explicit ?? activeEditorModel();
  if (!model) return;
  const cte = explicit ? undefined : await cteUnderCursor();
  await runPreview(model, cte);
}

/** `rocky.previewCte` — preview a single CTE's rows (from the CodeLens). */
export async function previewCte(arg?: {
  model?: string;
  cte?: string;
}): Promise<void> {
  if (arg?.model && arg?.cte) await runPreview(arg.model, arg.cte);
}

async function runPreview(model: string, cte: string | undefined): Promise<void> {
  if (!ensureWorkspace()) return;
  const provider = getQueryResultsProvider();
  if (!provider) return;

  const { previewRowLimit, previewAllowWarehouse } = getConfig();
  const cwd = resolveProjectRoot();
  const title = cte ? `${model} · ${cte}` : model;

  // Reveal the Query Results panel, then show a placeholder while we run.
  await vscode.commands.executeCommand("rocky.queryResults.focus");
  provider.setLoading(title);

  const run = (allowWarehouse: boolean): Promise<PreviewRowsOutput> =>
    runRockyJsonWithProgress<PreviewRowsOutput>(
      `Previewing ${title}…`,
      buildPreviewArgs(model, cte, previewRowLimit, allowWarehouse),
      { cwd },
    );

  try {
    provider.setData(title, toGridData(await run(false)));
  } catch (err) {
    const env = parseErrorEnvelope(err);

    // Warehouse execution is gated: prompt + retry only if the user opted in.
    if (env?.error_kind === "warehouse_gated") {
      if (!previewAllowWarehouse) {
        provider.setError(
          env.message ??
            "Preview is gated for warehouse adapters. Enable rocky.preview.allowWarehouse to run against your warehouse.",
        );
        return;
      }
      const adapter = env.adapter_kind ?? "your warehouse";
      const confirmed = await confirmAction(
        `Run this preview against ${adapter}? It executes a query and may incur cost.`,
        "Run on warehouse",
      );
      if (!confirmed) {
        provider.setError("Preview cancelled.");
        return;
      }
      try {
        provider.setData(title, toGridData(await run(true)));
      } catch (retryErr) {
        reportError(retryErr, provider);
      }
      return;
    }

    reportError(err, provider);
  }
}

/** Surface a preview failure in the panel (and the output channel for raw CLI errors). */
function reportError(
  err: unknown,
  provider: NonNullable<ReturnType<typeof getQueryResultsProvider>>,
): void {
  const env = parseErrorEnvelope(err);
  if (env) {
    provider.setError(env.message ?? `Preview failed (${env.error_kind}).`);
    return;
  }
  const message = err instanceof RockyCliError ? err.stderr.trim() || err.message : String(err);
  provider.setError(`Preview failed: ${message}`);
  showRockyError("Preview failed", err);
}
