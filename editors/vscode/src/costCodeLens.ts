import * as path from "path";
import * as vscode from "vscode";
import type { OptimizeOutput, OptimizeRecommendation } from "./types/generated/optimize";
import { getOutputChannel } from "./output";
import { runRockyJson } from "./rockyCli";
import { hasRockyProject, onDidChangeRockyProject } from "./views/getStartedView";

const COST_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

interface CostCache {
  data: Map<string, OptimizeRecommendation>;
  fetchedAt: number;
}

let costCache: CostCache | undefined;

/**
 * Fetch fresh cost data from `rocky optimize --output json`, populate the
 * cache, and return the Map. On any failure, logs to the output channel and
 * returns the empty map — callers treat missing entries as "no lens to show".
 */
async function fetchCostData(): Promise<Map<string, OptimizeRecommendation>> {
  try {
    const output = await runRockyJson<OptimizeOutput>([
      "optimize",
      "--output",
      "json",
    ]);
    const data = new Map<string, OptimizeRecommendation>();
    for (const rec of output.recommendations ?? []) {
      data.set(rec.model_name, rec);
    }
    costCache = { data, fetchedAt: Date.now() };
    return data;
  } catch (err) {
    getOutputChannel().appendLine(
      `[cost annotations] rocky optimize failed — no cost lenses will appear: ${(err as Error).message}`,
    );
    costCache = { data: new Map(), fetchedAt: Date.now() };
    return costCache.data;
  }
}

/**
 * Return the cached cost data if still within TTL, otherwise return undefined
 * (caller should trigger a refresh).
 */
function getCachedData(): Map<string, OptimizeRecommendation> | undefined {
  if (!costCache) return undefined;
  if (Date.now() - costCache.fetchedAt >= COST_CACHE_TTL_MS) return undefined;
  return costCache.data;
}

/** Invalidate the cost cache so the next `provideCodeLenses` triggers a fetch. */
export function invalidateCostCache(): void {
  costCache = undefined;
}

function isCostAnnotationsEnabled(): boolean {
  return vscode.workspace
    .getConfiguration("rocky")
    .get<boolean>("costAnnotations.enabled", true);
}

function formatCostLabel(rec: OptimizeRecommendation): string {
  const compute = rec.compute_cost_per_run.toFixed(4);
  const storage = rec.storage_cost_per_month.toFixed(2);
  const savings = rec.estimated_monthly_savings.toFixed(2);
  return `$(graph) ~$${compute}/run · $${storage}/mo storage · save $${savings}/mo`;
}

function formatCostDetail(rec: OptimizeRecommendation): string {
  const lines = [
    `Model: ${rec.model_name}`,
    `Compute cost per run: $${rec.compute_cost_per_run.toFixed(4)}`,
    `Storage cost per month: $${rec.storage_cost_per_month.toFixed(2)}`,
    `Estimated monthly savings: $${rec.estimated_monthly_savings.toFixed(2)}`,
    `Current strategy: ${rec.current_strategy}`,
    `Recommended strategy: ${rec.recommended_strategy}`,
    `Downstream references: ${rec.downstream_references}`,
    `Reasoning: ${rec.reasoning}`,
  ];
  return lines.join("\n");
}

/**
 * Returns true when the file sits under a `models/` directory segment.
 * Mirrors the logic in codeLens.ts.
 */
function isModelFile(filePath: string): boolean {
  const segments = filePath.split(path.sep);
  return segments.includes("models");
}

/**
 * Provides inline cost CodeLenses above every `.rocky` and `.sql` model file.
 *
 * The cache is populated on extension activation and on the `rocky.refreshCosts`
 * command. It expires after 5 minutes; once stale, lenses disappear until the
 * next refresh. Fetches are never triggered inside `provideCodeLenses` (which
 * fires on every edit/scroll) — all async work is separate.
 */
export class CostCodeLensProvider implements vscode.CodeLensProvider {
  private readonly emitter = new vscode.EventEmitter<void>();
  readonly onDidChangeCodeLenses = this.emitter.event;

  /**
   * Trigger a data fetch, then fire `onDidChangeCodeLenses` so VS Code
   * re-evaluates every open model file. Safe to call repeatedly — the second
   * call while a fetch is in-flight will just see the freshly-populated cache.
   *
   * No-ops (without fetching) when the workspace has no `rocky.toml`, so the
   * extension never shells out to `rocky optimize` in a non-Rocky project.
   */
  async refresh(): Promise<void> {
    if (!hasRockyProject()) {
      invalidateCostCache();
      this.emitter.fire();
      return;
    }
    await fetchCostData();
    this.emitter.fire();
  }

  /** Fire the change event without fetching (e.g. when the setting is toggled). */
  fireChange(): void {
    this.emitter.fire();
  }

  provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
    if (!isCostAnnotationsEnabled()) return [];

    const ext = path.extname(document.fileName);
    if (ext !== ".rocky" && ext !== ".sql") return [];
    if (!isModelFile(document.fileName)) return [];

    const modelName = path.basename(document.fileName, ext);
    if (!modelName) return [];

    const data = getCachedData();
    if (!data) return []; // cache miss or expired — no lenses until next refresh

    const rec = data.get(modelName);
    if (!rec) return [];

    const range = new vscode.Range(0, 0, 0, 0);
    return [
      new vscode.CodeLens(range, {
        title: formatCostLabel(rec),
        command: "rocky.showCostDetail",
        arguments: [rec],
      }),
    ];
  }
}

/**
 * Wire the provider, the `rocky.refreshCosts` command, and the configuration
 * change listener into the extension context.
 *
 * On activation we fire an initial refresh in the background (no-op on
 * failure). When `rocky.costAnnotations.enabled` changes we fire the change
 * event to clear or restore lenses immediately.
 */
export function registerCostCodeLensProvider(
  context: vscode.ExtensionContext,
): CostCodeLensProvider {
  const provider = new CostCodeLensProvider();

  context.subscriptions.push(
    vscode.languages.registerCodeLensProvider(
      [
        { scheme: "file", language: "rocky" },
        { scheme: "file", language: "sql", pattern: "**/models/**/*.sql" },
      ],
      provider,
    ),

    vscode.commands.registerCommand("rocky.refreshCosts", () => {
      invalidateCostCache();
      void provider.refresh();
    }),

    vscode.commands.registerCommand(
      "rocky.showCostDetail",
      (rec: OptimizeRecommendation) => {
        void vscode.window
          .showInformationMessage(formatCostDetail(rec), "Refresh Costs")
          .then((choice) => {
            if (choice === "Refresh Costs") {
              invalidateCostCache();
              void provider.refresh();
            }
          });
      },
    ),

    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("rocky.costAnnotations.enabled")) {
        provider.fireChange();
      }
    }),

    // When the project-detection signal flips (e.g. the user just ran
    // `rocky init` and a rocky.toml appears), trigger a fresh fetch so cost
    // lenses light up without requiring a manual refresh.
    onDidChangeRockyProject((has) => {
      if (has) {
        void provider.refresh();
      } else {
        invalidateCostCache();
        provider.fireChange();
      }
    }),
  );

  // Initial background fetch — `refresh()` itself gates on hasRockyProject(),
  // so this is a no-op in workspaces without a rocky.toml.
  void provider.refresh();

  return provider;
}
