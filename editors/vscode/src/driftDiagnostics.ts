import * as path from "path";
import * as vscode from "vscode";
import { runRockyJson, RockyCliError } from "./rockyCli";
import { getOutputChannel } from "./output";
import type { CompileOutput, Diagnostic, Severity } from "./types/generated/compile";

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
 * Register the drift diagnostics provider.
 *
 * Diagnostics are refreshed:
 * - When a model file is opened
 * - When a model file is saved (debounced to avoid rapid-fire compiles)
 *
 * The diagnostic collection is disposed when the extension deactivates.
 */
export function registerDriftDiagnostics(
  context: vscode.ExtensionContext,
): void {
  const collection = vscode.languages.createDiagnosticCollection("rocky-drift");
  context.subscriptions.push(collection);

  // Track pending debounce timers per document URI so we can cancel on
  // rapid successive saves.
  const pendingTimers = new Map<string, ReturnType<typeof setTimeout>>();

  /** Schedule a debounced diagnostic refresh (500ms). */
  function scheduleRefresh(document: vscode.TextDocument): void {
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

  // Refresh on file open.
  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument((document) => {
      void refreshDiagnostics(document, collection);
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

  // Run on any already-open model files at activation time.
  for (const document of vscode.workspace.textDocuments) {
    void refreshDiagnostics(document, collection);
  }
}
