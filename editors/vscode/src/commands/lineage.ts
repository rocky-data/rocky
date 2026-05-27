import * as path from "path";
import * as vscode from "vscode";
import { resolveProjectRoot } from "../config";
import { RockyCliError, runRockyJson } from "../rockyCli";
import type { CatalogOutput } from "../types/generated/catalog";
import type { CompileOutput, ModelDetail } from "../types/generated/compile";
import type { DriftOutput } from "../types/generated/drift";
import {
  registerWebviewViewApp,
  type WebviewViewController,
} from "../webviews/host/registerPanel";
import type {
  DriftData,
  GraphData,
  GraphEdge,
  GraphNode,
  ModelParam,
} from "../webviews/lineage/contract";
import { resolveModelName } from "./ui";

/** Panel webview-view id — must match `contributes.views.rockyLineagePanel`. */
const VIEW_TYPE = "rocky.lineageView";

let controller: WebviewViewController | undefined;

/**
 * Reveal the Lineage canvas (bottom panel) and focus it on a model. Resolves
 * the model from a tree-view node, then the active editor's file name.
 */
export async function showLineage(arg?: unknown): Promise<void> {
  let modelName = resolveModelName(arg);
  if (!modelName) {
    const editor = vscode.window.activeTextEditor;
    if (editor) {
      modelName = path
        .basename(editor.document.fileName)
        .replace(/\.(rocky|sql)$/i, "");
    }
  }
  await vscode.commands.executeCommand(`${VIEW_TYPE}.focus`);
  if (modelName) controller?.push("focus", { model: modelName });
}

/** Register the Lineage canvas view; called once from `registerViews`. */
export function registerLineageView(context: vscode.ExtensionContext): void {
  controller = registerWebviewViewApp(context, VIEW_TYPE, {
    entry: "lineage",
    title: "Lineage",
    setup: (host) => {
      host.onRequest("graph", () => buildGraph());
      host.onRequest("openFile", (params) =>
        openModelFile((params as ModelParam).model),
      );
      host.onRequest("openInspector", (params) => {
        void vscode.commands.executeCommand(
          "rocky.openInspector",
          (params as ModelParam).model,
        );
      });
      host.onRequest("drift", () => loadDrift());
    },
  });
}

/**
 * Build the project-wide graph by fanning in `rocky catalog` (assets +
 * model-level dependencies + kind) and `rocky compile` (materialization per
 * model). Compile is best-effort — the canvas still renders without it, just
 * without the materialization color mode.
 */
async function buildGraph(): Promise<GraphData> {
  const cwd = resolveProjectRoot();
  const [catalog, compile] = await Promise.all([
    runRockyJson<CatalogOutput>(["catalog", "--output", "json"], { cwd }),
    runRockyJson<CompileOutput>(["compile", "--output", "json"], { cwd }).catch(
      () => null,
    ),
  ]);

  const details = new Map<string, ModelDetail>();
  for (const detail of compile?.models_detail ?? []) {
    details.set(detail.name, detail);
  }

  const nodes: GraphNode[] = catalog.assets.map((asset) => {
    const detail = details.get(asset.model_name);
    return {
      id: asset.model_name,
      label: asset.model_name,
      kind: asset.kind,
      materialization: detail?.strategy.type ?? null,
      fqn: asset.fqn,
      costHint: detail?.cost_hint ?? null,
      freshness: detail?.freshness ?? null,
    };
  });

  const ids = new Set(nodes.map((n) => n.id));
  const edges: GraphEdge[] = [];
  for (const asset of catalog.assets) {
    for (const upstream of asset.upstream_models) {
      if (ids.has(upstream)) {
        edges.push({ source: upstream, target: asset.model_name });
      }
    }
  }

  return { nodes, edges };
}

async function openModelFile(model: string): Promise<void> {
  const matches = await vscode.workspace.findFiles(
    `**/models/**/${model}.{rocky,sql}`,
    undefined,
    1,
  );
  if (matches[0]) {
    void vscode.commands.executeCommand("vscode.open", matches[0]);
  }
}

/** Run `rocky drift` for the drift overlay; degrades gracefully when unavailable. */
async function loadDrift(): Promise<DriftData> {
  try {
    const out = await runRockyJson<DriftOutput>(["drift", "--output", "json"], {
      cwd: resolveProjectRoot(),
    });
    return {
      actions: out.drift.actions_taken.map((a) => ({
        table: a.table,
        action: a.action,
        reason: a.reason,
      })),
    };
  } catch (err) {
    const unavailable =
      err instanceof RockyCliError
        ? err.stderr.trim() || err.message
        : String(err);
    return { actions: [], unavailable };
  }
}
