import * as path from "path";
import * as vscode from "vscode";
import { getExtensionUri } from "../extensionState";
import { RockyCliError, runRockyJson } from "../rockyCli";
import type { CatalogOutput } from "../types/generated/catalog";
import type { CompileOutput } from "../types/generated/compile";
import type { PreviewRowsOutput } from "../types/generated/preview_rows";
import type { TestOutput } from "../types/generated/test";
import { createWebviewPanelApp } from "../webviews/host/registerPanel";
import type { WebviewHost } from "../webviews/host/WebviewHost";
import type {
  InspectorModelData,
  InspectorPreviewData,
  InspectorTestsData,
  ModelParam,
  ModelPush,
} from "../webviews/inspector/contract";

const VIEW_TYPE = "rocky.inspector";

// A single reusable panel, re-targeted across invocations.
let panel: vscode.WebviewPanel | undefined;
let host: WebviewHost | undefined;
// Monotonic id so a slow fan-in for an earlier model can't overwrite a newer one.
let generation = 0;

/**
 * Open — or re-target — the Rocky Inspector for a model. Accepts a model name,
 * a schema-tree node (model or column), or nothing (falls back to the active
 * editor's file name).
 */
export function openInspector(arg?: unknown): void {
  const model = resolveModelName(arg);
  if (!model) {
    void vscode.window.showInformationMessage(
      "Open a Rocky model file, or pick one from the Schema view, to inspect it.",
    );
    return;
  }

  if (panel && host) {
    panel.reveal(vscode.ViewColumn.Active);
    void pushModel(host, model);
    return;
  }

  panel = createWebviewPanelApp(getExtensionUri(), VIEW_TYPE, {
    entry: "inspector",
    title: "Rocky Inspector",
    setup: (h) => {
      host = h;
      h.onRequest("tests", (p) => loadTests((p as ModelParam).model));
      h.onRequest("preview", (p) => loadPreview((p as ModelParam).model));
    },
  });
  panel.onDidDispose(() => {
    panel = undefined;
    host = undefined;
  });
  if (host) void pushModel(host, model);
}

/** Fan-in catalog + compile, then push the model summary (or an error). */
async function pushModel(target: WebviewHost, model: string): Promise<void> {
  const mine = ++generation;
  try {
    const data = await loadModelData(model);
    if (mine !== generation) return; // a newer open superseded this fan-in
    target.push<"model", ModelPush>("model", { ok: true, data });
  } catch (err) {
    if (mine !== generation) return;
    target.push<"model", ModelPush>("model", {
      ok: false,
      error: errMessage(err),
    });
  }
}

async function loadModelData(model: string): Promise<InspectorModelData> {
  const [catalog, compile] = await Promise.all([
    runRockyJson<CatalogOutput>(["catalog", "--output", "json"]),
    runRockyJson<CompileOutput>(["compile", "--output", "json"]),
  ]);
  const asset = catalog.assets.find((a) => a.model_name === model);
  if (!asset) {
    throw new Error(`Model "${model}" was not found in the project catalog.`);
  }
  const detail = compile.models_detail?.find((d) => d.name === model);
  const columnEdges = catalog.edges.filter(
    (e) => e.source_model === model || e.target_model === model,
  );
  return {
    modelName: asset.model_name,
    fqn: asset.fqn,
    kind: asset.kind,
    intent: asset.intent ?? null,
    lastMaterializedAt: asset.last_materialized_at ?? null,
    columns: asset.columns,
    upstreamModels: asset.upstream_models,
    downstreamModels: asset.downstream_models,
    columnEdges,
    contractSource: detail?.contract_source ?? null,
    freshness: detail?.freshness ?? null,
    materialization: detail?.strategy.type ?? null,
    costHint: detail?.cost_hint ?? null,
  };
}

/**
 * Run declarative tests and scope the results to `model`. Invoked lazily (the
 * Tests tab) because it executes SQL.
 *
 * `rocky test` currently runs the whole project — its `model_filter` is an
 * unimplemented TODO in `commands/test.rs` — so we run the full declarative
 * suite and filter client-side. Switch to a per-model invocation once the
 * engine scopes test execution.
 */
async function loadTests(model: string): Promise<InspectorTestsData> {
  try {
    const out = await runRockyJson<TestOutput>([
      "test",
      "--declarative",
      "--output",
      "json",
    ]);
    const results = (out.declarative?.results ?? []).filter(
      (r) => r.model === model,
    );
    return { results };
  } catch (err) {
    return { results: [], unavailable: errMessage(err) };
  }
}

/**
 * Sample rows for `model`. DuckDB runs locally; non-DuckDB warehouses require
 * the `rocky.preview.allowWarehouse` setting (and the CLI still gates cost).
 */
async function loadPreview(model: string): Promise<InspectorPreviewData> {
  const config = vscode.workspace.getConfiguration("rocky");
  const limit = config.get<number>("preview.rowLimit", 100);
  const allowWarehouse = config.get<boolean>("preview.allowWarehouse", false);
  const args = [
    "preview",
    "rows",
    "--model",
    model,
    "--limit",
    String(limit),
    "--output",
    "json",
  ];
  if (allowWarehouse) args.push("--allow-warehouse");
  try {
    const preview = await runRockyJson<PreviewRowsOutput>(args);
    return { preview };
  } catch (err) {
    return { unavailable: errMessage(err) };
  }
}

function resolveModelName(arg: unknown): string | undefined {
  if (typeof arg === "string") return arg;
  if (arg && typeof arg === "object") {
    const node = arg as { asset?: { model_name?: string }; modelName?: string };
    if (node.asset?.model_name) return node.asset.model_name;
    if (node.modelName) return node.modelName;
  }
  const editor = vscode.window.activeTextEditor;
  if (editor) {
    return path.basename(editor.document.fileName).replace(/\.(rocky|sql)$/i, "");
  }
  return undefined;
}

function errMessage(err: unknown): string {
  if (err instanceof RockyCliError) return err.stderr.trim() || err.message;
  return err instanceof Error ? err.message : String(err);
}
