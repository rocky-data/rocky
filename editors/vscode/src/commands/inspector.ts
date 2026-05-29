import * as path from "path";
import * as vscode from "vscode";
import { RockyCliError, runRockyJson } from "../rockyCli";
import type { CatalogOutput } from "../types/generated/catalog";
import type { CompileOutput } from "../types/generated/compile";
import type { PreviewRowsOutput } from "../types/generated/preview_rows";
import type { ProfileOutput } from "../types/generated/profile";
import type { TestOutput } from "../types/generated/test";
import {
  registerWebviewViewApp,
  type WebviewViewController,
} from "../webviews/host/registerPanel";
import type {
  InspectorModelData,
  InspectorPreviewData,
  InspectorTarget,
  InspectorTestsData,
  ModelParam,
} from "../webviews/inspector/contract";
import type { AiActionParam } from "../webviews/lineage/contract";
import {
  buildGraph,
  loadBreaking,
  loadDrift,
  loadGovernance,
  loadReplay,
  openModelFile,
  runAiAction,
} from "./lineage";

/** Bottom-panel webview-view id — must match `contributes.views.rockyInspectorPanel`. */
const VIEW_TYPE = "rocky.inspector";

let controller: WebviewViewController | undefined;

/** Register the Rocky Inspector view (bottom panel); called once from `registerViews`. */
export function registerInspectorView(context: vscode.ExtensionContext): void {
  controller = registerWebviewViewApp(context, VIEW_TYPE, {
    entry: "inspector",
    title: "Rocky Inspector",
    setup: (h) => {
      h.onRequest("model", (p) => loadModelData((p as ModelParam).model));
      h.onRequest("tests", (p) => loadTests((p as ModelParam).model));
      h.onRequest("preview", (p) => loadPreview((p as ModelParam).model));
      h.onRequest("profile", (p) => loadProfile((p as ModelParam).model));
      // The Lineage tab embeds the project canvas, so the Inspector serves the
      // same graph + overlay + node-action requests the standalone view did.
      h.onRequest("graph", () => buildGraph());
      h.onRequest("openFile", (p) => openModelFile((p as ModelParam).model));
      h.onRequest("ai", (p) => runAiAction(p as AiActionParam));
      h.onRequest("drift", () => loadDrift());
      h.onRequest("breaking", () => loadBreaking());
      h.onRequest("replay", () => loadReplay());
      h.onRequest("governance", () => loadGovernance());
    },
  });

  registerActiveEditorFollow(context);
}

/**
 * Auto-follow: when the active editor switches to a model file, retarget the
 * Inspector to that model — but only while the panel is visible (so it never
 * steals focus or churns while hidden), debounced against rapid tab-flipping,
 * and skipping non-model files (a stray `.ts`/`.md` shouldn't blank the panel).
 * Explicit triggers (Open-in-Inspector, a lineage-canvas node click) still
 * work; this just makes browsing model files in the editor track along.
 */
function registerActiveEditorFollow(context: vscode.ExtensionContext): void {
  let lastModel: string | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const isModelFile = (doc: vscode.TextDocument): boolean =>
    doc.languageId === "rocky" ||
    (doc.fileName.endsWith(".sql") && /[\\/]models[\\/]/.test(doc.fileName));

  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor((editor) => {
      if (!editor || !controller?.visible || !isModelFile(editor.document)) {
        return;
      }
      const model = path
        .basename(editor.document.fileName)
        .replace(/\.(rocky|sql)$/i, "");
      if (model === lastModel) return;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        lastModel = model;
        controller?.push<InspectorTarget>("target", { model });
      }, 250);
    }),
  );
}

/**
 * Reveal the Rocky Inspector and target a model. Accepts a model name, a
 * schema-tree node (model or column), or nothing (falls back to the active
 * editor's file name).
 */
export async function openInspector(arg?: unknown): Promise<void> {
  const model = resolveModelName(arg);
  if (!model) {
    void vscode.window.showInformationMessage(
      "Open a Rocky model file, or pick one from the Schema view, to inspect it.",
    );
    return;
  }
  await vscode.commands.executeCommand(`${VIEW_TYPE}.focus`);
  controller?.push<InspectorTarget>("target", { model });
}

/** Reveal the Inspector on its Lineage tab, framed on a model when one resolves. */
export async function showLineage(arg?: unknown): Promise<void> {
  const model = resolveModelName(arg);
  await vscode.commands.executeCommand(`${VIEW_TYPE}.focus`);
  controller?.push<InspectorTarget>("target", { model, tab: "lineage" });
}

/** Fan-in `rocky catalog` + `rocky compile` into one model-scoped summary. */
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
 * Load tests for `model`, scoped to one model. Invoked lazily (the Tests tab)
 * because it executes SQL. Two engine calls:
 *
 * 1. `rocky test --declarative` — the `[[tests]]` assertions from sidecars.
 *    Still project-wide on the wire, so we filter client-side.
 * 2. `rocky test --model <model>` — the DuckDB model-execution check. The
 *    engine now scopes this to one model and itemizes passes (not just
 *    failures), so a model with no declarative assertions still reports
 *    green here instead of leaving the tab empty.
 */
async function loadTests(model: string): Promise<InspectorTestsData> {
  let results: InspectorTestsData["results"] = [];
  let modelExecution: InspectorTestsData["modelExecution"];
  let unavailable: string | undefined;

  try {
    const declarative = await runRockyJson<TestOutput>([
      "test",
      "--declarative",
      "--output",
      "json",
    ]);
    results = (declarative.declarative?.results ?? []).filter(
      (r) => r.model === model,
    );
  } catch (err) {
    unavailable = errMessage(err);
  }

  try {
    const execution = await runRockyJson<TestOutput>([
      "test",
      "--model",
      model,
      "--output",
      "json",
    ]);
    modelExecution = (execution.model_results ?? []).find(
      (r) => r.model === model,
    );
  } catch (err) {
    // Keep any declarative results we got; only report unavailable when both
    // calls failed (nothing to show).
    if (results.length === 0) unavailable = unavailable ?? errMessage(err);
  }

  return { results, modelExecution, unavailable };
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

/**
 * Profile `model`'s target table per column via `rocky profile` (DuckDB-only;
 * a single aggregate query per column, no LLM). Invoked lazily (Profile tab).
 */
async function loadProfile(model: string): Promise<ProfileOutput> {
  try {
    return await runRockyJson<ProfileOutput>([
      "profile",
      model,
      "--output",
      "json",
    ]);
  } catch (err) {
    return {
      version: "",
      command: "profile",
      model,
      columns: [],
      unavailable: errMessage(err),
    };
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
