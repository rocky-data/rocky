import * as path from "path";
import * as vscode from "vscode";
import { runRockyJson, RockyCliError } from "./rockyCli";
import { getOutputChannel } from "./output";
import type { ModelHistoryOutput } from "./types/generated";

const SUPPORTED_EXTENSIONS = new Set([".rocky", ".sql"]);

/** Only decorate files under a `models/` directory segment. */
function isModelFile(filePath: string): boolean {
  const segments = filePath.split(path.sep);
  return segments.includes("models");
}

/**
 * Find the line index of the first `-- model:` comment, or 0 if absent.
 * Mirrors the same logic used by the code-lens provider so the decoration
 * appears on the same line as the action lenses.
 */
function findModelCommentLine(document: vscode.TextDocument): number {
  const lineCount = Math.min(document.lineCount, 30);
  for (let i = 0; i < lineCount; i++) {
    const text = document.lineAt(i).text;
    if (/^--\s*model:/i.test(text)) return i;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatRowCount(rows: number): string {
  if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)}M rows`;
  if (rows >= 1_000) return `${(rows / 1_000).toFixed(1)}k rows`;
  return `${rows} rows`;
}

// ---------------------------------------------------------------------------
// Decoration types — created once, reused across refreshes
// ---------------------------------------------------------------------------

const successDecorationType = vscode.window.createTextEditorDecorationType({
  after: {
    margin: "0 0 0 2em",
    color: new vscode.ThemeColor("testing.iconPassed"),
  },
  isWholeLine: true,
});

const failureDecorationType = vscode.window.createTextEditorDecorationType({
  after: {
    margin: "0 0 0 2em",
    color: new vscode.ThemeColor("testing.iconFailed"),
  },
  isWholeLine: true,
});

const noHistoryDecorationType = vscode.window.createTextEditorDecorationType({
  after: {
    margin: "0 0 0 2em",
    color: new vscode.ThemeColor("disabledForeground"),
  },
  isWholeLine: true,
});

/** All three decoration types so we can clear them uniformly. */
const ALL_DECORATION_TYPES = [
  successDecorationType,
  failureDecorationType,
  noHistoryDecorationType,
];

// ---------------------------------------------------------------------------
// Core logic
// ---------------------------------------------------------------------------

/**
 * Fetches the latest run for a model via `rocky history --model <name>` and
 * applies an inline after-text decoration on the model comment line (or line 0).
 *
 * The decoration is intentionally lightweight: a single line showing duration
 * and row count on success, or "FAILED" on failure.
 */
async function applyRunDecoration(editor: vscode.TextEditor): Promise<void> {
  const document = editor.document;
  const ext = path.extname(document.fileName);

  if (!SUPPORTED_EXTENSIONS.has(ext)) return;
  if (!isModelFile(document.fileName)) return;

  const modelName = path.basename(document.fileName, ext);
  if (!modelName) return;

  // Clear existing decorations before applying new ones
  for (const dt of ALL_DECORATION_TYPES) {
    editor.setDecorations(dt, []);
  }

  const line = findModelCommentLine(document);
  const range = new vscode.Range(line, 0, line, document.lineAt(line).text.length);

  let result: ModelHistoryOutput;
  try {
    result = await runRockyJson<ModelHistoryOutput>(
      ["history", "--model", modelName, "--output", "json"],
      { timeoutMs: 10_000 },
    );
  } catch (err) {
    // CLI not available or model not found — show gray "no history"
    if (err instanceof RockyCliError) {
      const channel = getOutputChannel();
      channel.appendLine(
        `[runDecorations] history lookup failed for ${modelName}: ${err.message}`,
      );
    }
    editor.setDecorations(noHistoryDecorationType, [
      {
        range,
        renderOptions: {
          after: { contentText: "$(circle-outline)  no run history" },
        },
      },
    ]);
    return;
  }

  const executions = result.executions ?? [];
  if (executions.length === 0) {
    editor.setDecorations(noHistoryDecorationType, [
      {
        range,
        renderOptions: {
          after: { contentText: "$(circle-outline)  no run history" },
        },
      },
    ]);
    return;
  }

  // Latest execution is the first element (sorted by started_at desc)
  const latest = executions[0];
  const isSuccess =
    latest.status === "success" || latest.status === "Success";
  const isFailed =
    latest.status === "failed" ||
    latest.status === "failure" ||
    latest.status === "Failure";

  if (isFailed) {
    editor.setDecorations(failureDecorationType, [
      {
        range,
        renderOptions: {
          after: {
            contentText: `$(error)  FAILED  ${formatDuration(latest.duration_ms)}`,
          },
        },
      },
    ]);
    return;
  }

  // Success or any other status — show duration + rows
  const parts: string[] = [];
  parts.push(formatDuration(latest.duration_ms));
  if (typeof latest.rows_affected === "number") {
    parts.push(formatRowCount(latest.rows_affected));
  }

  const decorationType = isSuccess
    ? successDecorationType
    : noHistoryDecorationType;
  const icon = isSuccess ? "$(pass)" : "$(circle-outline)";

  editor.setDecorations(decorationType, [
    {
      range,
      renderOptions: {
        after: { contentText: `${icon}  ${parts.join(", ")}` },
      },
    },
  ]);
}

// ---------------------------------------------------------------------------
// EventEmitter for cross-module refresh (e.g., after a terminal run)
// ---------------------------------------------------------------------------

const refreshEmitter = new vscode.EventEmitter<void>();

/**
 * Fire this event to refresh decorations on all visible editors.
 * Exported so that command handlers (e.g., `rocky.codeLens.runModel`) can
 * trigger a refresh after a run completes.
 */
export const onRunDecorationsRefreshRequested = refreshEmitter.event;

/** Request a decoration refresh on all visible editors. */
export function refreshRunDecorations(): void {
  refreshEmitter.fire();
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/**
 * Registers the inline run-summary decoration provider. Should be called
 * once from `activate()`.
 *
 * Decorations refresh on:
 * - Editor becoming visible (opening a file or switching tabs)
 * - File save
 * - Explicit refresh via `refreshRunDecorations()`
 */
export function registerRunDecorations(
  context: vscode.ExtensionContext,
): void {
  // Decorate when an editor becomes active
  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor((editor) => {
      if (editor) applyRunDecoration(editor);
    }),
  );

  // Refresh on file save — the model may have been re-run
  context.subscriptions.push(
    vscode.workspace.onDidSaveTextDocument((document) => {
      const editor = vscode.window.visibleTextEditors.find(
        (e) => e.document === document,
      );
      if (editor) applyRunDecoration(editor);
    }),
  );

  // Refresh all visible editors when explicitly requested
  context.subscriptions.push(
    onRunDecorationsRefreshRequested(() => {
      for (const editor of vscode.window.visibleTextEditors) {
        applyRunDecoration(editor);
      }
    }),
  );

  // Dispose decoration types on deactivation
  context.subscriptions.push(
    ...ALL_DECORATION_TYPES,
    refreshEmitter,
  );

  // Apply immediately if there is already an active editor
  if (vscode.window.activeTextEditor) {
    applyRunDecoration(vscode.window.activeTextEditor);
  }
}
