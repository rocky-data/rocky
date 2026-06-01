import * as path from "path";
import * as vscode from "vscode";
import { runRockyJson, RockyCliError } from "./rockyCli";
import { getOutputChannel } from "./output";
import type { CompileOutput, Diagnostic, Severity } from "./types/generated/compile";
import { hasRockyProject, onDidChangeRockyProject } from "./views/getStartedView";

const SUPPORTED_EXTENSIONS = new Set([".rocky", ".sql"]);

/** Returns true when the file sits under a `models/` directory segment. */
function isModelFile(filePath: string): boolean {
  const segments = filePath.split(path.sep);
  return segments.includes("models");
}

/** Derive the Rocky model name from the file path (basename without extension). */
function modelNameFromFile(filePath: string): string | undefined {
  const ext = path.extname(filePath);
  if (!SUPPORTED_EXTENSIONS.has(ext)) return undefined;
  if (!isModelFile(filePath)) return undefined;
  const name = path.basename(filePath, ext);
  return name || undefined;
}

/** Map a Rocky severity string to a VS Code DiagnosticSeverity. */
function mapSeverity(severity: Severity): vscode.DiagnosticSeverity {
  switch (severity) {
    case "Error":
      return vscode.DiagnosticSeverity.Error;
    case "Warning":
      return vscode.DiagnosticSeverity.Warning;
    case "Info":
      return vscode.DiagnosticSeverity.Information;
  }
}

/**
 * Convert a Rocky CLI {@link Diagnostic} to a VS Code diagnostic.
 *
 * When the diagnostic carries a {@link SourceSpan} we use its line/col
 * for a precise underline.  Otherwise the diagnostic is placed on line 0.
 */
function toVscodeDiagnostic(d: Diagnostic): vscode.Diagnostic {
  const line = d.span?.line ? d.span.line - 1 : 0; // Rocky lines are 1-based
  const col = d.span?.col ? d.span.col - 1 : 0;

  const range = new vscode.Range(line, col, line, Number.MAX_SAFE_INTEGER);

  const diag = new vscode.Diagnostic(range, d.message, mapSeverity(d.severity));
  diag.code = d.code;
  diag.source = "rocky";

  if (d.suggestion) {
    diag.relatedInformation = [
      new vscode.DiagnosticRelatedInformation(
        new vscode.Location(vscode.Uri.file(""), new vscode.Position(0, 0)),
        `Suggestion: ${d.suggestion}`,
      ),
    ];
  }

  return diag;
}

/**
 * Code-action provider for Rocky drift diagnostics.
 *
 * For every Rocky diagnostic in scope it offers:
 * - "Rocky: Run compile to refresh" — re-runs compile on the current model
 * - "Rocky: Accept schema change" — runs `rocky.acceptDrift` if registered,
 *   otherwise shows an informational placeholder message.
 */
export class DriftCodeActionProvider implements vscode.CodeActionProvider {
  static readonly metadata: vscode.CodeActionProviderMetadata = {
    providedCodeActionKinds: [vscode.CodeActionKind.QuickFix],
  };

  provideCodeActions(
    document: vscode.TextDocument,
    _range: vscode.Range | vscode.Selection,
    context: vscode.CodeActionContext,
  ): vscode.CodeAction[] {
    const rockyDiags = context.diagnostics.filter(
      (d) => d.source === "rocky",
    );
    if (rockyDiags.length === 0) return [];

    const actions: vscode.CodeAction[] = [];

    // Quick fix 1: re-run compile
    const compileAction = new vscode.CodeAction(
      "Rocky: Run compile to refresh",
      vscode.CodeActionKind.QuickFix,
    );
    compileAction.command = {
      command: "rocky.compile",
      title: "Run compile to refresh",
    };
    compileAction.diagnostics = rockyDiags;
    actions.push(compileAction);

    // Quick fix 2: accept drift (placeholder if the command isn't registered)
    const acceptAction = new vscode.CodeAction(
      "Rocky: Accept schema change",
      vscode.CodeActionKind.QuickFix,
    );
    acceptAction.command = {
      command: "rocky.acceptDrift",
      title: "Accept schema change",
      arguments: [document.uri],
    };
    acceptAction.diagnostics = rockyDiags;
    actions.push(acceptAction);

    return actions;
  }
}

/**
 * Run `rocky compile --model <name> --output json` and update the
 * diagnostic collection for the given document.
 */
async function refreshDiagnostics(
  document: vscode.TextDocument,
  collection: vscode.DiagnosticCollection,
): Promise<void> {
  const modelName = modelNameFromFile(document.fileName);
  if (!modelName) {
    // Not a model file — clear any stale diagnostics and bail.
    collection.delete(document.uri);
    return;
  }

  const channel = getOutputChannel();

  try {
    const result = await runRockyJson<CompileOutput>(
      ["compile", "--model", modelName, "--output", "json"],
      { timeoutMs: 30_000 },
    );

    // Filter diagnostics to only those for this specific model.
    const modelDiags = result.diagnostics.filter(
      (d) => d.model === modelName,
    );

    const vscodeDiags = modelDiags.map(toVscodeDiagnostic);
    collection.set(document.uri, vscodeDiags);
  } catch (err) {
    if (err instanceof RockyCliError) {
      channel.appendLine(
        `[drift-diagnostics] compile failed for model ${modelName}: ${err.message}`,
      );
    }
    // On failure, clear diagnostics rather than showing stale results.
    collection.delete(document.uri);
  }
}

/**
 * Register the drift diagnostics provider and code-action quick fixes.
 *
 * Diagnostics are refreshed:
 * - When a model file is opened
 * - When a model file is saved (debounced to avoid rapid-fire compiles)
 *
 * The diagnostic collection is disposed when the extension deactivates.
 *
 * When `rocky.diagnostics.enabled` is `false` the provider is registered but
 * immediately no-ops so existing diagnostics are cleared and no new ones fire.
 */
export function registerDriftDiagnostics(
  context: vscode.ExtensionContext,
): void {
  const collection = vscode.languages.createDiagnosticCollection("rocky-drift");
  context.subscriptions.push(collection);

  // Register the code-action provider for Rocky diagnostics.
  context.subscriptions.push(
    vscode.languages.registerCodeActionsProvider(
      [
        { scheme: "file", language: "rocky" },
        { scheme: "file", language: "sql", pattern: "**/models/**/*.sql" },
      ],
      new DriftCodeActionProvider(),
      DriftCodeActionProvider.metadata,
    ),
  );

  // Register a placeholder `rocky.acceptDrift` command so the code action
  // never throws "command not found" when selected by the user.
  context.subscriptions.push(
    vscode.commands.registerCommand("rocky.acceptDrift", () => {
      void vscode.window.showInformationMessage(
        "Rocky: Accept schema change is not yet implemented. Run `rocky compile` after manually updating your schema.",
      );
    }),
  );

  // Track pending debounce timers per document URI so we can cancel on
  // rapid successive saves.
  const pendingTimers = new Map<string, ReturnType<typeof setTimeout>>();

  /** Returns `true` when drift diagnostics are enabled in settings. */
  function isDiagnosticsEnabled(): boolean {
    return vscode.workspace
      .getConfiguration("rocky")
      .get<boolean>("diagnostics.enabled", true);
  }

  /**
   * The CLI can only run when both the user has diagnostics enabled and a
   * Rocky project is present in the workspace — otherwise `rocky compile`
   * just fails with "no rocky.toml found".
   */
  function shouldRunCli(): boolean {
    return isDiagnosticsEnabled() && hasRockyProject();
  }

  /** Schedule a debounced diagnostic refresh (500ms). */
  function scheduleRefresh(document: vscode.TextDocument): void {
    if (!shouldRunCli()) {
      collection.clear();
      return;
    }
    const key = document.uri.toString();
    const existing = pendingTimers.get(key);
    if (existing !== undefined) {
      clearTimeout(existing);
    }
    pendingTimers.set(
      key,
      setTimeout(() => {
        pendingTimers.delete(key);
        void refreshDiagnostics(document, collection);
      }, 500),
    );
  }

  // Refresh on file open. The compile spawn (model files) is debounced through
  // the same per-URI 500ms timer as save, so opening a burst of model files —
  // or VS Code re-opening a session's tabs on startup — doesn't fire a
  // `rocky compile --model` per file at once. Non-model files spawn nothing, so
  // they clear any stale diagnostics eagerly (no point debouncing a cheap delete).
  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((document) => {
      if (!modelNameFromFile(document.fileName)) {
        collection.delete(document.uri);
        return;
      }
      scheduleRefresh(document);
    }),
  );

  // Refresh on file save (debounced).
  context.subscriptions.push(
    vscode.workspace.onDidSaveTextDocument((document) => {
      scheduleRefresh(document);
    }),
  );

  // Clear diagnostics when a file is closed to avoid stale markers.
  context.subscriptions.push(
    vscode.workspace.onDidCloseTextDocument((document) => {
      const key = document.uri.toString();
      const timer = pendingTimers.get(key);
      if (timer !== undefined) {
        clearTimeout(timer);
        pendingTimers.delete(key);
      }
      collection.delete(document.uri);
    }),
  );

  // When the diagnostics.enabled setting is toggled off, clear all diagnostics.
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("rocky.diagnostics.enabled")) {
        if (!isDiagnosticsEnabled()) {
          collection.clear();
        }
      }
    }),
  );

  // When the workspace gains or loses a rocky.toml, re-sweep open documents
  // (gain) or clear stale diagnostics (loss).
  context.subscriptions.push(
    onDidChangeRockyProject((has) => {
      if (!has) {
        collection.clear();
        for (const timer of pendingTimers.values()) clearTimeout(timer);
        pendingTimers.clear();
        return;
      }
      if (!isDiagnosticsEnabled()) return;
      for (const document of vscode.workspace.textDocuments) {
        void refreshDiagnostics(document, collection);
      }
    }),
  );

  // Run on any already-open model files at activation time — but only when
  // a Rocky project is present. `hasRockyProject()` resolves to true
  // asynchronously after activation; the onDidChangeRockyProject listener
  // above catches that transition.
  if (shouldRunCli()) {
    for (const document of vscode.workspace.textDocuments) {
      void refreshDiagnostics(document, collection);
    }
  }
}
