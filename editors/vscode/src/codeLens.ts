import * as path from "path";
import * as vscode from "vscode";
import { getConfig } from "./config";

const SUPPORTED_EXTENSIONS = new Set([".rocky", ".sql"]);

/**
 * Returns true when the file sits under a `models/` directory segment,
 * excluding seeds/ or other non-model SQL files.
 */
function isModelFile(filePath: string): boolean {
  const segments = filePath.split(path.sep);
  return segments.includes("models");
}

/**
 * Find the line index of the first `-- model:` comment, or 0 if absent.
 * The code lens is placed on this line so it appears directly above the
 * model declaration when one exists.
 */
function findModelCommentLine(document: vscode.TextDocument): number {
  const lineCount = Math.min(document.lineCount, 30); // only scan first 30 lines
  for (let i = 0; i < lineCount; i++) {
    const text = document.lineAt(i).text;
    if (/^--\s*model:/i.test(text)) return i;
  }
  return 0;
}

/**
 * Provides a row of action lenses at the top of every `.rocky` and `.sql`
 * model file. Each lens dispatches to an existing command, passing the model
 * name as the first argument so the user is not re-prompted.
 *
 * "Run Model" and "Compile Model" execute directly in the integrated terminal
 * for live output streaming.  The remaining lenses dispatch to the standard
 * Rocky extension commands.
 */
export class RockyCodeLensProvider implements vscode.CodeLensProvider {
  private readonly emitter = new vscode.EventEmitter<void>();
  readonly onDidChangeCodeLenses = this.emitter.event;

  refresh(): void {
    this.emitter.fire();
  }

  async provideCodeLenses(
    document: vscode.TextDocument,
  ): Promise<vscode.CodeLens[]> {
    const ext = path.extname(document.fileName);
    if (!SUPPORTED_EXTENSIONS.has(ext)) return [];
    if (!isModelFile(document.fileName)) return [];

    const modelName = path.basename(document.fileName, ext);
    if (!modelName) return [];

    const line = findModelCommentLine(document);
    const range = new vscode.Range(line, 0, line, 0);

    const lenses = [
      makeLens(range, "$(play) Run Model", "rocky.codeLens.runModel", [modelName]),
      makeLens(range, "$(table) Preview", "rocky.previewModel", [modelName]),
      makeLens(range, "$(file-code) Compile Model", "rocky.codeLens.compileModel", [
        modelName,
      ]),
      makeLens(range, "$(open-preview) Compiled SQL", "rocky.showCompiledSql", [
        modelName,
      ]),
      makeLens(range, "$(beaker) Test", "rocky.test", [modelName]),
      makeLens(range, "$(graph) Lineage", "rocky.showLineage", [modelName]),
    ];

    // Per-CTE preview lenses. CTE symbols are emitted by the LSP only for
    // `.sql` models (`.rocky` files surface pipeline steps instead). If the
    // LSP isn't ready or returns nothing, the base lenses still apply.
    if (ext === ".sql") {
      try {
        const symbols = await vscode.commands.executeCommand<
          vscode.DocumentSymbol[]
        >("vscode.executeDocumentSymbolProvider", document.uri);
        for (const cte of collectCteSymbols(symbols ?? [])) {
          const cteRange = new vscode.Range(cte.line, 0, cte.line, 0);
          lenses.push(
            makeLens(cteRange, `$(table) Preview CTE: ${cte.name}`, "rocky.previewCte", [
              { model: modelName, cte: cte.name },
            ]),
          );
        }
      } catch {
        // Symbol provider unavailable — keep the base lenses.
      }
    }

    return lenses;
  }
}

/**
 * Recursively collect CTE symbols from a document-symbol tree. The Rocky LSP
 * tags each CTE with `detail: "CTE"` nested under the model symbol.
 */
function collectCteSymbols(
  symbols: vscode.DocumentSymbol[],
): { name: string; line: number }[] {
  const out: { name: string; line: number }[] = [];
  const walk = (syms: vscode.DocumentSymbol[]): void => {
    for (const s of syms) {
      if (s.detail === "CTE") {
        out.push({ name: s.name, line: s.range.start.line });
      }
      if (s.children && s.children.length > 0) walk(s.children);
    }
  };
  walk(symbols);
  return out;
}

function makeLens(
  range: vscode.Range,
  title: string,
  command: string,
  args: unknown[] = [],
): vscode.CodeLens {
  return new vscode.CodeLens(range, {
    title,
    command,
    arguments: args,
  });
}

/**
 * Build the argv for `rocky run --filter name=<model> --output json`.
 *
 * The model name is a discrete argv element, never spliced into a shell command
 * string. Exported so the security invariant (the model name reaches the CLI as
 * one literal argument, with zero shell interpretation) can be unit-tested.
 */
export function buildRunModelArgs(modelName: string): string[] {
  return ["run", "--filter", `name=${modelName}`, "--output", "json"];
}

/**
 * Build the argv for `rocky compile --model <model> --output json`. See
 * [`buildRunModelArgs`] for the literal-argv-element invariant.
 */
export function buildCompileModelArgs(modelName: string): string[] {
  return ["compile", "--model", modelName, "--output", "json"];
}

/**
 * Execute a model-scoped rocky command as a VS Code task.
 *
 * Uses [`vscode.ProcessExecution`] with an argv array rather than a shell
 * command string passed to `Terminal.sendText`, so the model name (derived from
 * a workspace file name an attacker could control via a cloned repo) is passed
 * as a single literal argument the shell never parses. Mirrors `taskProvider.ts`.
 * The task terminal still streams live output, matching the previous UX.
 */
function runModelTask(args: string[], label: string): void {
  const cfg = getConfig();
  const exec = new vscode.ProcessExecution(cfg.serverPath, args);
  const task = new vscode.Task(
    { type: "rocky", command: args[0] },
    vscode.TaskScope.Workspace,
    label,
    "rocky",
    exec,
  );
  task.presentationOptions = {
    reveal: vscode.TaskRevealKind.Always,
    panel: vscode.TaskPanelKind.Dedicated,
    clear: true,
  };
  void vscode.tasks.executeTask(task);
}

/**
 * Runs `rocky run --filter name=<model>` as a task (argv, no shell).
 */
function runModelInTerminal(modelName: string): void {
  runModelTask(buildRunModelArgs(modelName), `run ${modelName}`);
}

/**
 * Runs `rocky compile --model <model>` as a task (argv, no shell).
 */
function compileModelInTerminal(modelName: string): void {
  runModelTask(buildCompileModelArgs(modelName), `compile ${modelName}`);
}

export function registerCodeLensProvider(
  context: vscode.ExtensionContext,
): RockyCodeLensProvider {
  const provider = new RockyCodeLensProvider();
  context.subscriptions.push(
    vscode.languages.registerCodeLensProvider(
      [
        { scheme: "file", language: "rocky" },
        { scheme: "file", language: "sql", pattern: "**/models/**/*.sql" },
      ],
      provider,
    ),
    vscode.commands.registerCommand(
      "rocky.codeLens.runModel",
      runModelInTerminal,
    ),
    vscode.commands.registerCommand(
      "rocky.codeLens.compileModel",
      compileModelInTerminal,
    ),
  );
  return provider;
}
