import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import type {
  PreviewCostOutput,
  PreviewDiffOutput,
  PreviewModelCostDelta,
  PreviewModelDiff,
} from "../types/generated";
import { hasRockyProject, onDidChangeRockyProject } from "./getStartedView";

// ---------------------------------------------------------------------------
// Persisted state shape
// ---------------------------------------------------------------------------

/**
 * One active preview branch tracked in workspaceState.
 * We persist only summary-level output to keep the stored payload lean — full
 * diff payloads can be multi-MB on large projects.
 */
export interface ActivePreview {
  branchName: string;
  createdAt: string;
  /** Summary fields from the last `rocky preview cost` run. */
  lastCostSummary?: {
    delta_usd?: number | null;
    models_changed: number;
  };
  /** Summary fields from the last `rocky preview diff` run. */
  lastDiffSummary?: {
    models_with_changes: number;
    models_unchanged: number;
    total_rows_added: number;
    total_rows_removed: number;
    total_rows_changed: number;
    any_coverage_warning: boolean;
  };
  /** Per-model cost deltas from the last cost fetch (kept for the tree). */
  perModelCost?: PreviewModelCostDelta[];
  /** Per-model diffs from the last diff fetch (kept for the tree). */
  perModelDiff?: PreviewModelDiff[];
}

const STATE_KEY = "rocky.activePreviews";

// ---------------------------------------------------------------------------
// Module-level preview-created event bus
// (commands/preview.ts fires it; the view subscribes without circular deps)
// ---------------------------------------------------------------------------

type PreviewCreatedListener = (preview: ActivePreview) => void;
const previewCreatedListeners: PreviewCreatedListener[] = [];

/** Called by `commands/preview.ts` after a successful `rocky.previewCreate`. */
export function firePreviewCreated(preview: ActivePreview): void {
  for (const cb of previewCreatedListeners) cb(preview);
}

/** Register a listener for new previews. Returns a disposable. */
export function onPreviewCreated(cb: PreviewCreatedListener): {
  dispose(): void;
} {
  previewCreatedListeners.push(cb);
  return {
    dispose: (): void => {
      const idx = previewCreatedListeners.indexOf(cb);
      if (idx !== -1) previewCreatedListeners.splice(idx, 1);
    },
  };
}

// ---------------------------------------------------------------------------
// State manager (exported so commands/preview.ts can call it)
// ---------------------------------------------------------------------------

/** Thin wrapper around workspaceState so the view and the preview commands can
 *  share state without a circular dependency. Callers receive typed objects. */
export class PreviewStateManager {
  constructor(private readonly memento: vscode.Memento) {}

  getAll(): ActivePreview[] {
    return this.memento.get<ActivePreview[]>(STATE_KEY, []);
  }

  async add(preview: ActivePreview): Promise<void> {
    const existing = this.getAll().filter(
      (p) => p.branchName !== preview.branchName,
    );
    await this.memento.update(STATE_KEY, [...existing, preview]);
  }

  async remove(branchName: string): Promise<void> {
    const filtered = this.getAll().filter((p) => p.branchName !== branchName);
    await this.memento.update(STATE_KEY, filtered);
  }

  async updateBranch(
    branchName: string,
    patch: Partial<ActivePreview>,
  ): Promise<void> {
    const all = this.getAll().map((p) =>
      p.branchName === branchName ? { ...p, ...patch } : p,
    );
    await this.memento.update(STATE_KEY, all);
  }
}

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

type PreviewNode =
  | PreviewBranchNode
  | PreviewCategoryNode
  | PreviewCostModelNode
  | PreviewDiffModelNode
  | PreviewMessageNode;

export class PreviewBranchNode extends vscode.TreeItem {
  override readonly contextValue = "rockyPreviewBranch";

  constructor(public readonly preview: ActivePreview) {
    super(preview.branchName, vscode.TreeItemCollapsibleState.Collapsed);

    const cost = preview.lastCostSummary;
    const diff = preview.lastDiffSummary;

    const parts: string[] = [];
    if (cost?.delta_usd != null) {
      const sign = cost.delta_usd >= 0 ? "+" : "";
      parts.push(`Δ$${sign}${cost.delta_usd.toFixed(4)}/mo`);
    }
    if (diff != null) {
      parts.push(`${diff.models_with_changes} model${diff.models_with_changes === 1 ? "" : "s"} changed`);
    }

    this.description = parts.length > 0 ? parts.join(" · ") : "not yet fetched";
    this.tooltip = buildBranchTooltip(preview);
    this.iconPath = new vscode.ThemeIcon("git-branch");
  }
}

export class PreviewCategoryNode extends vscode.TreeItem {
  override readonly contextValue = "rockyPreviewCategory";

  constructor(
    public readonly branchName: string,
    public readonly category: "cost" | "diff" | "models",
    label: string,
    description: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.Collapsed);
    this.description = description;
    this.iconPath = categoryIcon(category);
  }
}

export class PreviewCostModelNode extends vscode.TreeItem {
  override readonly contextValue = "rockyPreviewCostModel";

  constructor(public readonly delta: PreviewModelCostDelta) {
    super(delta.model_name, vscode.TreeItemCollapsibleState.None);

    const parts: string[] = [];
    if (delta.delta_usd != null) {
      const sign = delta.delta_usd >= 0 ? "+" : "";
      parts.push(`Δ$${sign}${delta.delta_usd.toFixed(4)}`);
    }
    if (delta.skipped_via_copy) parts.push("copied");
    const durMs = delta.branch_duration_ms;
    if (durMs > 0) parts.push(formatDuration(durMs));

    this.description = parts.join(" · ");
    this.tooltip = buildCostModelTooltip(delta);
    this.iconPath = new vscode.ThemeIcon("graph");
  }
}

export class PreviewDiffModelNode extends vscode.TreeItem {
  override readonly contextValue = "rockyPreviewDiffModel";

  /** Status: "added" | "removed" | "modified" | "unchanged" */
  public readonly diffStatus: string;

  constructor(
    public readonly branchName: string,
    public readonly model: PreviewModelDiff,
    public readonly markdownPayload: string,
  ) {
    super(model.model_name, vscode.TreeItemCollapsibleState.None);

    const { type_changes } = model.structural;
    const added_columns = model.structural.added_columns ?? [];
    const removed_columns = model.structural.removed_columns ?? [];
    const hasAdded = added_columns.length > 0;
    const hasRemoved = removed_columns.length > 0;
    const hasTypeChanges = (type_changes?.length ?? 0) > 0;

    // Heuristic: classify at column-level (rows may differ independently)
    if (hasAdded && !hasRemoved && !hasTypeChanges) {
      this.diffStatus = "added";
    } else if (hasRemoved && !hasAdded && !hasTypeChanges) {
      this.diffStatus = "removed";
    } else if (hasAdded || hasRemoved || hasTypeChanges) {
      this.diffStatus = "modified";
    } else {
      this.diffStatus = "unchanged";
    }

    const rowInfo = this.buildRowInfo();
    this.description = `${this.diffStatus}${rowInfo ? ` · ${rowInfo}` : ""}`;
    this.tooltip = buildDiffModelTooltip(model);
    this.iconPath = diffStatusIcon(this.diffStatus);

    // Clicking opens an in-memory markdown document with the diff payload.
    this.command = {
      title: "Show diff",
      command: "rocky.showPreviewDiffMarkdown",
      arguments: [branchName, model.model_name, markdownPayload],
    };
  }

  private buildRowInfo(): string {
    const algo = this.model.algorithm;
    if (!algo) return "";
    if (algo.kind === "sampled") {
      const s = algo.sampled;
      const parts: string[] = [];
      if (s.rows_added > 0) parts.push(`+${s.rows_added}`);
      if (s.rows_removed > 0) parts.push(`-${s.rows_removed}`);
      if (s.rows_changed > 0) parts.push(`~${s.rows_changed}`);
      return parts.join("/");
    }
    if (algo.kind === "bisection") {
      const d = algo.diff;
      const parts: string[] = [];
      if (d.rows_added > 0) parts.push(`+${d.rows_added}`);
      if (d.rows_removed > 0) parts.push(`-${d.rows_removed}`);
      if (d.rows_changed > 0) parts.push(`~${d.rows_changed}`);
      return parts.join("/");
    }
    return "";
  }
}

class PreviewMessageNode extends vscode.TreeItem {
  override readonly contextValue = "rockyPreviewMessage";

  constructor(label: string, icon: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
  }
}

// ---------------------------------------------------------------------------
// Tree provider
// ---------------------------------------------------------------------------

/**
 * Tree provider for the Previews sidebar view.
 *
 * Root level: one `PreviewBranchNode` per active preview tracked in
 * `workspaceState`. Children: Cost / Diff / Models Changed sub-trees showing
 * per-model data from the last `rocky preview cost` and `rocky preview diff`
 * fetches. Data is refreshed manually (view-title button) or after
 * `rocky.previewCreate` completes.
 */
export class PreviewTreeProvider
  implements vscode.TreeDataProvider<PreviewNode>
{
  private readonly emitter = new vscode.EventEmitter<
    PreviewNode | undefined | void
  >();
  readonly onDidChangeTreeData = this.emitter.event;

  constructor(public readonly stateManager: PreviewStateManager) {}

  refresh(): void {
    this.emitter.fire();
  }

  getTreeItem(element: PreviewNode): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: PreviewNode): Promise<PreviewNode[]> {
    // Root level
    if (!element) {
      if (!hasRockyProject()) return [];

      const previews = this.stateManager.getAll();
      if (previews.length === 0) return [];

      return previews.map((p) => new PreviewBranchNode(p));
    }

    // Children of a branch node → 3 category nodes
    if (element instanceof PreviewBranchNode) {
      const { preview } = element;
      const diff = preview.lastDiffSummary;
      const cost = preview.lastCostSummary;

      const costDesc = cost
        ? cost.delta_usd != null
          ? `Δ$${cost.delta_usd >= 0 ? "+" : ""}${cost.delta_usd.toFixed(4)}`
          : "no cost data"
        : "not fetched";

      const diffDesc = diff
        ? `${diff.models_with_changes} changed`
        : "not fetched";

      const modelsDesc = preview.perModelDiff
        ? `${preview.perModelDiff.length} models`
        : "not fetched";

      return [
        new PreviewCategoryNode(preview.branchName, "cost", "Cost", costDesc),
        new PreviewCategoryNode(preview.branchName, "diff", "Diff", diffDesc),
        new PreviewCategoryNode(preview.branchName, "models", "Models Changed", modelsDesc),
      ];
    }

    // Children of a category node
    if (element instanceof PreviewCategoryNode) {
      const previews = this.stateManager.getAll();
      const preview = previews.find(
        (p) => p.branchName === element.branchName,
      );
      if (!preview) return [];

      if (element.category === "cost") {
        const items = preview.perModelCost;
        if (!items || items.length === 0) {
          return [new PreviewMessageNode("No cost data — click Refresh", "info")];
        }
        return items.map((d) => new PreviewCostModelNode(d));
      }

      if (element.category === "diff") {
        const models = preview.perModelDiff;
        if (!models || models.length === 0) {
          return [new PreviewMessageNode("No diff data — click Refresh", "info")];
        }
        // Open the branch-level markdown payload (from PreviewDiffOutput) in
        // the "Diff" category — per-model nodes open per-model sections.
        return models.map(
          (m) =>
            new PreviewDiffModelNode(
              element.branchName,
              m,
              formatModelDiffMarkdown(m),
            ),
        );
      }

      if (element.category === "models") {
        const models = preview.perModelDiff;
        if (!models || models.length === 0) {
          return [new PreviewMessageNode("No model data — click Refresh", "info")];
        }
        return models.map(
          (m) =>
            new PreviewDiffModelNode(
              element.branchName,
              m,
              formatModelDiffMarkdown(m),
            ),
        );
      }

      return [];
    }

    return [];
  }

  /**
   * Fetch fresh cost + diff data for every tracked branch (in parallel).
   * A failure on one branch does not cancel the others.
   */
  async fetchAllBranches(): Promise<void> {
    if (!hasRockyProject()) return;

    const previews = this.stateManager.getAll();
    if (previews.length === 0) return;

    await Promise.allSettled(
      previews.map(async (preview) => {
        await this.fetchBranch(preview.branchName);
      }),
    );

    this.refresh();
  }

  async fetchBranch(branchName: string): Promise<void> {
    const [costResult, diffResult] = await Promise.allSettled([
      runRockyJson<PreviewCostOutput>([
        "preview",
        "cost",
        "--name",
        branchName,
        "--output",
        "json",
      ]),
      runRockyJson<PreviewDiffOutput>([
        "preview",
        "diff",
        "--name",
        branchName,
        "--output",
        "json",
      ]),
    ]);

    const patch: Partial<ActivePreview> = {};

    if (costResult.status === "fulfilled") {
      const cost = costResult.value;
      patch.lastCostSummary = {
        delta_usd: cost.summary.delta_usd,
        models_changed: cost.per_model.length,
      };
      patch.perModelCost = cost.per_model;
    }

    if (diffResult.status === "fulfilled") {
      const diff = diffResult.value;
      patch.lastDiffSummary = {
        models_with_changes: diff.summary.models_with_changes,
        models_unchanged: diff.summary.models_unchanged,
        total_rows_added: diff.summary.total_rows_added,
        total_rows_removed: diff.summary.total_rows_removed,
        total_rows_changed: diff.summary.total_rows_changed,
        any_coverage_warning: diff.summary.any_coverage_warning,
      };
      patch.perModelDiff = diff.models;
    }

    await this.stateManager.updateBranch(branchName, patch);
  }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function buildBranchTooltip(preview: ActivePreview): string {
  const lines: string[] = [];
  lines.push(`Branch: ${preview.branchName}`);
  lines.push(`Created: ${new Date(preview.createdAt).toLocaleString()}`);
  const cost = preview.lastCostSummary;
  if (cost) {
    const delta =
      cost.delta_usd != null
        ? `${cost.delta_usd >= 0 ? "+" : ""}$${cost.delta_usd.toFixed(4)}`
        : "n/a";
    lines.push(`Cost delta: ${delta}/mo`);
  }
  const diff = preview.lastDiffSummary;
  if (diff) {
    lines.push(
      `Diff: ${diff.models_with_changes} changed, ${diff.models_unchanged} unchanged`,
    );
    lines.push(
      `Rows: +${diff.total_rows_added} / -${diff.total_rows_removed} / ~${diff.total_rows_changed}`,
    );
    if (diff.any_coverage_warning) lines.push("⚠ Coverage warning");
  }
  return lines.join("\n");
}

function buildCostModelTooltip(delta: PreviewModelCostDelta): string {
  const lines: string[] = [];
  lines.push(`Model: ${delta.model_name}`);
  if (delta.skipped_via_copy) lines.push("Strategy: copied from base");
  if (delta.delta_usd != null) {
    lines.push(
      `Cost delta: ${delta.delta_usd >= 0 ? "+" : ""}$${delta.delta_usd.toFixed(6)}`,
    );
  }
  lines.push(
    `Branch duration: ${formatDuration(delta.branch_duration_ms)}`,
  );
  if (delta.branch_bytes_scanned != null) {
    lines.push(
      `Branch bytes scanned: ${delta.branch_bytes_scanned.toLocaleString()}`,
    );
  }
  return lines.join("\n");
}

function buildDiffModelTooltip(model: PreviewModelDiff): string {
  const lines: string[] = [];
  lines.push(`Model: ${model.model_name}`);
  const { type_changes } = model.structural;
  const added_columns = model.structural.added_columns ?? [];
  const removed_columns = model.structural.removed_columns ?? [];
  if (added_columns.length > 0) {
    lines.push(`Added columns: ${added_columns.join(", ")}`);
  }
  if (removed_columns.length > 0) {
    lines.push(`Removed columns: ${removed_columns.join(", ")}`);
  }
  if ((type_changes?.length ?? 0) > 0) {
    for (const tc of type_changes ?? []) {
      lines.push(`Type change: ${tc.name} ${tc.from} → ${tc.to}`);
    }
  }
  return lines.join("\n");
}

function formatModelDiffMarkdown(model: PreviewModelDiff): string {
  const lines: string[] = [];
  lines.push(`# Diff: ${model.model_name}`);
  lines.push("");
  const { type_changes } = model.structural;
  const added_columns = model.structural.added_columns ?? [];
  const removed_columns = model.structural.removed_columns ?? [];
  if (
    added_columns.length === 0 &&
    removed_columns.length === 0 &&
    (type_changes?.length ?? 0) === 0
  ) {
    lines.push("No structural changes.");
  } else {
    if (added_columns.length > 0) {
      lines.push("## Added Columns");
      for (const c of added_columns) lines.push(`- \`${c}\``);
      lines.push("");
    }
    if (removed_columns.length > 0) {
      lines.push("## Removed Columns");
      for (const c of removed_columns) lines.push(`- \`${c}\``);
      lines.push("");
    }
    if ((type_changes?.length ?? 0) > 0) {
      lines.push("## Type Changes");
      for (const tc of type_changes ?? []) {
        lines.push(`- \`${tc.name}\`: \`${tc.from}\` → \`${tc.to}\``);
      }
      lines.push("");
    }
  }

  const algo = model.algorithm;
  if (algo) {
    lines.push("## Row Diff");
    if (algo.kind === "sampled") {
      const s = algo.sampled;
      lines.push(
        `Rows added: ${s.rows_added} · Removed: ${s.rows_removed} · Changed: ${s.rows_changed}`,
      );
      if (algo.sampling_window.coverage_warning) {
        lines.push("");
        lines.push(
          "> ⚠ Coverage warning: changes outside the sampling window may not be reflected.",
        );
      }
      const samples = s.samples ?? [];
      if (samples.length > 0) {
        lines.push("");
        lines.push("### Samples");
        for (const sample of samples) {
          lines.push(`**Row (pk=${sample.primary_key})**`);
          for (const change of sample.changes) {
            lines.push(
              `  - \`${change.column}\`: \`${change.base_value}\` → \`${change.branch_value}\``,
            );
          }
        }
      }
    } else if (algo.kind === "bisection") {
      const d = algo.diff;
      lines.push(
        `Rows added: ${d.rows_added} · Removed: ${d.rows_removed} · Changed: ${d.rows_changed}`,
      );
      if (algo.bisection_stats.depth_capped) {
        lines.push("");
        lines.push(
          "> ⚠ Bisection depth capped — diff may be slower but is still correct.",
        );
      }
    }
  }

  return lines.join("\n");
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function categoryIcon(category: "cost" | "diff" | "models"): vscode.ThemeIcon {
  switch (category) {
    case "cost":
      return new vscode.ThemeIcon("graph");
    case "diff":
      return new vscode.ThemeIcon("diff");
    case "models":
      return new vscode.ThemeIcon("list-tree");
  }
}

function diffStatusIcon(status: string): vscode.ThemeIcon {
  switch (status) {
    case "added":
      return new vscode.ThemeIcon(
        "diff-added",
        new vscode.ThemeColor("gitDecoration.addedResourceForeground"),
      );
    case "removed":
      return new vscode.ThemeIcon(
        "diff-removed",
        new vscode.ThemeColor("gitDecoration.deletedResourceForeground"),
      );
    case "modified":
      return new vscode.ThemeIcon(
        "diff-modified",
        new vscode.ThemeColor("gitDecoration.modifiedResourceForeground"),
      );
    default:
      return new vscode.ThemeIcon("circle-outline");
  }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

export function registerPreviewView(
  context: vscode.ExtensionContext,
): PreviewTreeProvider {
  const stateManager = new PreviewStateManager(context.workspaceState);
  const provider = new PreviewTreeProvider(stateManager);

  const view = vscode.window.createTreeView("rocky.previews", {
    treeDataProvider: provider,
    showCollapseAll: true,
  });
  context.subscriptions.push(view);

  // Refresh toolbar button — re-fetches cost + diff for every tracked branch.
  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.refreshPreviews", () => {
      void provider.fetchAllBranches();
    }),
  );

  // Right-click remove — UI-only; does not delete the engine branch.
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "rocky.removePreview",
      async (node: PreviewBranchNode) => {
        await stateManager.remove(node.preview.branchName);
        provider.refresh();
      },
    ),
  );

  // Open per-model diff as an in-memory markdown document.
  context.subscriptions.push(
    vscode.commands.registerCommand(
      "rocky.showPreviewDiffMarkdown",
      async (
        _branchName: string,
        modelName: string,
        markdown: string,
      ) => {
        const uri = vscode.Uri.parse(
          `untitled:Rocky Preview Diff — ${modelName}.md`,
        );
        const doc = await vscode.workspace.openTextDocument(uri);
        const editor = await vscode.window.showTextDocument(doc, {
          preview: true,
        });
        // Replace the whole document: the untitled URI is keyed by model name,
        // so a repeat click reuses the already-populated document — an insert
        // at (0,0) would stack a second copy above the first.
        await editor.edit((edit) => {
          const lastLine = editor.document.lineAt(editor.document.lineCount - 1);
          const fullRange = new vscode.Range(new vscode.Position(0, 0), lastLine.range.end);
          edit.replace(fullRange, markdown);
        });
      },
    ),
  );

  // When previewCreate succeeds, add to state and fetch fresh data.
  context.subscriptions.push(
    onPreviewCreated(async (preview) => {
      await stateManager.add(preview);
      provider.refresh();
      // Best-effort background fetch so the tree shows real data immediately.
      void provider.fetchBranch(preview.branchName).then(() => {
        provider.refresh();
      });
    }),
  );

  // Refresh when a rocky.toml appears or disappears.
  context.subscriptions.push(onDidChangeRockyProject(() => provider.refresh()));

  return provider;
}
