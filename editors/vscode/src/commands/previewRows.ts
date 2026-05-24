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

/** Resolve a model name from a command arg or the active editor. */
function resolveModelToPreview(arg: unknown): string | undefined {
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

/** `rocky.previewModel` — preview a model's output rows. */
export async function previewModel(arg?: unknown): Promise<void> {
  const model = resolveModelToPreview(arg);
  if (model) await runPreview(model, undefined);
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
