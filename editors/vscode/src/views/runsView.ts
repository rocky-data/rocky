import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import type { HistoryResult } from "../types/rockyJson";
import type { RunModelRecord } from "../types/generated";

type Run = NonNullable<HistoryResult["runs"]>[number];

type Node = RunTreeItem | ModelTreeItem | MessageItem;

/**
 * Tree view for recent Rocky runs. Backed by `rocky history --output json` --
 * fetched on first expand and on manual refresh. Top-level nodes are runs
 * (with timestamp, status icon, and duration); expanding a run shows its
 * per-model execution details (duration, rows affected, status).
 */
export class RunsTreeProvider implements vscode.TreeDataProvider<Node> {
  private readonly emitter = new vscode.EventEmitter<
    Node | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  private cache: Run[] | undefined;
  private loadError: string | undefined;

  refresh(): void {
    this.cache = undefined;
    this.loadError = undefined;
    this.emitter.fire();
  }

  getTreeItem(element: Node): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: Node): Promise<Node[]> {
    // Per-model children of a run
    if (element instanceof RunTreeItem) {
      return element.run.models.map((m) => new ModelTreeItem(m));
    }

    // Leaf nodes have no children
    if (element) return [];

    // Root: fetch runs
    if (this.cache === undefined) {
      try {
        const result = await runRockyJson<HistoryResult>([
          "history",
          "--output",
          "json",
        ]);
        this.cache = result.runs ?? [];
      } catch (err) {
        this.cache = [];
        this.loadError = (err as Error).message;
      }
    }

    if (this.loadError) {
      return [new MessageItem(`Error: ${this.loadError}`, "error")];
    }

    if (this.cache.length === 0) {
      return [new MessageItem("No runs found", "info")];
    }

    return this.cache.map((run) => new RunTreeItem(run));
  }
}

export class RunTreeItem extends vscode.TreeItem {
  override readonly contextValue = "rockyRun";

  constructor(public readonly run: Run) {
    const hasModels = (run.models?.length ?? 0) > 0;
    const state = hasModels
      ? vscode.TreeItemCollapsibleState.Collapsed
      : vscode.TreeItemCollapsibleState.None;
    super(run.run_id ?? "(unknown run)", state);

    this.description = formatRunDescription(run);
    this.tooltip = formatRunTooltip(run);
    this.iconPath = statusIcon(run.status ?? "unknown");
  }
}

class ModelTreeItem extends vscode.TreeItem {
  override readonly contextValue = "rockyRunModel";

  constructor(model: RunModelRecord) {
    super(model.model_name, vscode.TreeItemCollapsibleState.None);
    this.description = formatModelDescription(model);
    this.tooltip = formatModelTooltip(model);
    this.iconPath = statusIcon(model.status);
  }
}

class MessageItem extends vscode.TreeItem {
  override readonly contextValue = "rockyMessage";

  constructor(label: string, icon: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
  }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function formatRunDescription(run: Run): string {
  const parts: string[] = [];
  if (run.started_at) parts.push(formatTimestamp(run.started_at));
  if (typeof run.duration_ms === "number") {
    parts.push(formatDuration(run.duration_ms));
  }
  if (typeof run.models_executed === "number") {
    parts.push(`${run.models_executed} model${run.models_executed === 1 ? "" : "s"}`);
  }
  return parts.join(" · ");
}

function formatRunTooltip(run: Run): string {
  const lines: string[] = [];
  if (run.run_id) lines.push(`Run: ${run.run_id}`);
  if (run.status) lines.push(`Status: ${run.status}`);
  if (run.trigger) lines.push(`Trigger: ${run.trigger}`);
  if (run.started_at) lines.push(`Started: ${run.started_at}`);
  if (typeof run.duration_ms === "number") {
    lines.push(`Duration: ${formatDuration(run.duration_ms)}`);
  }
  if (typeof run.models_executed === "number") {
    lines.push(`Models: ${run.models_executed}`);
  }
  return lines.join("\n");
}

function formatModelDescription(model: RunModelRecord): string {
  const parts: string[] = [];
  parts.push(formatDuration(model.duration_ms));
  if (typeof model.rows_affected === "number") {
    parts.push(`${model.rows_affected.toLocaleString()} rows`);
  }
  parts.push(model.status);
  return parts.join(" · ");
}

function formatModelTooltip(model: RunModelRecord): string {
  const lines: string[] = [];
  lines.push(`Model: ${model.model_name}`);
  lines.push(`Status: ${model.status}`);
  lines.push(`Duration: ${formatDuration(model.duration_ms)}`);
  if (typeof model.rows_affected === "number") {
    lines.push(`Rows affected: ${model.rows_affected.toLocaleString()}`);
  }
  return lines.join("\n");
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function statusIcon(status: string): vscode.ThemeIcon {
  switch (status) {
    case "success":
    case "Success":
      return new vscode.ThemeIcon(
        "pass",
        new vscode.ThemeColor("testing.iconPassed"),
      );
    case "failure":
    case "failed":
    case "Failure":
      return new vscode.ThemeIcon(
        "error",
        new vscode.ThemeColor("testing.iconFailed"),
      );
    case "partial":
    case "PartialFailure":
      return new vscode.ThemeIcon(
        "warning",
        new vscode.ThemeColor("testing.iconQueued"),
      );
    default:
      return new vscode.ThemeIcon("circle-outline");
  }
}

export function registerRunsView(
  context: vscode.ExtensionContext,
): RunsTreeProvider {
  const provider = new RunsTreeProvider();
  const view = vscode.window.createTreeView("rocky.runs", {
    treeDataProvider: provider,
    showCollapseAll: true,
  });
  context.subscriptions.push(view);

  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshRuns", () =>
      provider.refresh(),
    ),
  );

  return provider;
}
