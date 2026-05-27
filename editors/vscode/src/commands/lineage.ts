import * as path from "path";
import * as vscode from "vscode";
import { resolveProjectRoot } from "../config";
import { runRockyJson } from "../rockyCli";
import type { CatalogOutput } from "../types/generated/catalog";
import type { CompileOutput } from "../types/generated/compile";
import {
  registerWebviewViewApp,
  type WebviewViewController,
} from "../webviews/host/registerPanel";
import type {
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

  const materializations = new Map<string, string>();
  for (const detail of compile?.models_detail ?? []) {
    materializations.set(detail.name, detail.strategy.type);
  }

  const nodes: GraphNode[] = catalog.assets.map((asset) => ({
    id: asset.model_name,
    label: asset.model_name,
    kind: asset.kind,
    materialization: materializations.get(asset.model_name) ?? null,
    fqn: asset.fqn,
  }));

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
