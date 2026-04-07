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

  provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
    const ext = path.extname(document.fileName);
    if (!SUPPORTED_EXTENSIONS.has(ext)) return [];
    if (!isModelFile(document.fileName)) return [];

    const modelName = path.basename(document.fileName, ext);
    if (!modelName) return [];

    const line = findModelCommentLine(document);
    const range = new vscode.Range(line, 0, line, 0);

    return [
      makeLens(range, "$(play) Run Model", "rocky.codeLens.runModel", [
        modelName,
      ]),
      makeLens(range, "$(file-code) Compile Model", "rocky.codeLens.compileModel", [
        modelName,
      ]),
      makeLens(range, "$(beaker) Test", "rocky.test", [modelName]),
      makeLens(range, "$(graph) Lineage", "rocky.showLineage"),
      makeLens(range, "$(history) History", "rocky.history", [modelName]),
      makeLens(range, "$(pulse) Metrics", "rocky.metrics", [modelName]),
      makeLens(range, "$(rocket) Optimize", "rocky.optimize", [modelName]),
    ];
  }
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
  const cmd = `${cfg.serverPath} run --filter name=${modelName} --output json`;
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
  const cmd = `${cfg.serverPath} compile --model ${modelName} --output json`;
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
        { scheme: "file", pattern: "**/*.rocky" },
        { scheme: "file", language: "sql", pattern: "**/models/**/*.sql" },
        { scheme: "file", pattern: "**/models/**/*.sql" },
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
