import * as path from "path";
import * as vscode from "vscode";
import { getConfig } from "./config";
import { shellQuote } from "./shell";

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
 * Runs `rocky run --filter name=<model>` in the integrated terminal.
 */
function runModelInTerminal(modelName: string): void {
  const cfg = getConfig();
  // Quote the entire `name=<model>` token together so the `name=` prefix sits
  // inside the same shell-quoted argv slot as the value — keeps it a single
  // argv element even if the model name contains shell metacharacters.
  const cmd = `${shellQuote(cfg.serverPath)} run --filter ${shellQuote(`name=${modelName}`)} --output json`;
  const terminal = vscode.window.createTerminal({
    name: `Rocky: run ${modelName}`,
    iconPath: new vscode.ThemeIcon("play"),
  });
  terminal.show();
  terminal.sendText(cmd);
}

/**
 * Runs `rocky compile --model <model>` in the integrated terminal.
 */
function compileModelInTerminal(modelName: string): void {
  const cfg = getConfig();
  const cmd = `${shellQuote(cfg.serverPath)} compile --model ${shellQuote(modelName)} --output json`;
  const terminal = vscode.window.createTerminal({
    name: `Rocky: compile ${modelName}`,
    iconPath: new vscode.ThemeIcon("file-code"),
  });
  terminal.show();
  terminal.sendText(cmd);
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
