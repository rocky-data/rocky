import * as vscode from "vscode";

/** QuickPick item that carries the VS Code command ID to execute on selection. */
interface CommandPickItem extends vscode.QuickPickItem {
  commandId: string;
}

/** Separator-only item for grouping categories in the QuickPick. */
function separator(label: string): vscode.QuickPickItem {
  return { label, kind: vscode.QuickPickItemKind.Separator };
}

/** A command entry with its display label, description, and backing command ID. */
function cmd(
  label: string,
  description: string,
  commandId: string,
): CommandPickItem {
  return { label, description, commandId };
}

/**
 * Builds the grouped list of Rocky commands for the QuickPick.
 *
 * Categories match the logical groupings in commands/index.ts:
 *   Pipeline → run, plan, discover, compare
 *   Model    → compile, test, lineage, metrics, optimize
 *   Infra    → doctor, compact, archive, profile storage
 *   AI       → AI explain, AI sync, AI test, AI generate
 */
function buildItems(): (vscode.QuickPickItem | CommandPickItem)[] {
  return [
    // --- Pipeline ---
    separator("Pipeline"),
    cmd("$(play) Run", "Execute the full pipeline", "rocky.run"),
    cmd("$(eye) Plan", "Preview SQL (dry run)", "rocky.plan"),
    cmd("$(search) Discover", "List connectors and tables", "rocky.discover"),
    cmd("$(diff) Compare", "Shadow vs production comparison", "rocky.compare"),

    // --- Model ---
    separator("Model"),
    cmd("$(check) Compile", "Type-check models, validate contracts", "rocky.compile"),
    cmd("$(beaker) Test", "Run local tests via DuckDB", "rocky.test"),
    cmd("$(type-hierarchy) Lineage", "Render model DAG as interactive SVG", "rocky.showLineage"),
    cmd("$(database) Catalog", "Build project-wide column-lineage snapshot", "rocky.catalog"),
    cmd("$(graph) Metrics", "Quality metrics with alerts", "rocky.metrics"),
    cmd("$(lightbulb) Optimize", "Cost & strategy analysis", "rocky.optimize"),

    // --- Infra ---
    separator("Infra"),
    cmd("$(heart) Doctor", "Run health checks", "rocky.doctor"),
    cmd("$(archive) Compact", "Generate OPTIMIZE/VACUUM SQL", "rocky.compact"),

    // --- AI ---
    separator("AI"),
    cmd("$(comment-discussion) AI Explain", "Generate intent docs from model", "rocky.aiExplain"),
    cmd("$(sync) AI Sync", "Detect + propose schema changes", "rocky.aiSync"),
    cmd("$(test-view-icon) AI Test", "Generate test cases from intent", "rocky.aiTest"),
    cmd("$(sparkle) AI Generate", "Create model from natural language", "rocky.aiGenerate"),
  ];
}

/**
 * Shows the Rocky Command Palette — a grouped QuickPick of all user-facing
 * commands. Bound to Cmd+Shift+R (Mac) / Ctrl+Shift+R (Win/Linux).
 */
export async function commandPalette(): Promise<void> {
  const items = buildItems();

  const picked = await vscode.window.showQuickPick(items, {
    title: "Rocky Command Palette",
    placeHolder: "Type to filter commands...",
    matchOnDescription: true,
  });

  if (picked && "commandId" in picked) {
    await vscode.commands.executeCommand(picked.commandId);
  }
}
