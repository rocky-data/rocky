import * as vscode from "vscode";
import { registerChatParticipant } from "./chatParticipant";
import { registerCodeLensProvider } from "./codeLens";
import { registerCommands } from "./commands";
import { registerCompiledSqlProvider } from "./compiledSqlProvider";
import { registerCostCodeLensProvider } from "./costCodeLens";
import { registerDriftDiagnostics } from "./driftDiagnostics";
import { setExtensionUri } from "./extensionState";
import { registerFoldingProvider } from "./foldingProvider";
import { registerFormattingProvider } from "./formattingProvider";
import { startLspClient, stopLspClient } from "./lspClient";
import { registerRockyMcpProvider } from "./mcpServer";
import { disposeOutputChannel } from "./output";
import { registerRockyCacheInvalidation } from "./rockyCli";
import { registerRunDecorations } from "./runDecorations";
import { registerTaskProvider } from "./taskProvider";
import { registerTestExplorer } from "./testExplorer";
import { registerDeclarativeTestProvider } from "./testProvider";
import { registerViews } from "./views";

export function activate(context: vscode.ExtensionContext): void {
  setExtensionUri(context.extensionUri);
  // Gates dev-only affordances (e.g. the webview devtools command) so they
  // never surface in a released build's command palette.
  void vscode.commands.executeCommand(
    "setContext",
    "rocky.developmentMode",
    context.extensionMode === vscode.ExtensionMode.Development,
  );
  startLspClient(context);
  registerRockyCacheInvalidation(context);
  registerCommands(context);
  registerChatParticipant(context);
  registerRockyMcpProvider(context);
  registerTestExplorer(context);
  registerDeclarativeTestProvider(context);
  registerCodeLensProvider(context);
  registerCostCodeLensProvider(context);
  registerFoldingProvider(context);
  registerFormattingProvider(context);
  registerRunDecorations(context);
  registerDriftDiagnostics(context);
  registerTaskProvider(context);
  registerCompiledSqlProvider(context);
  registerViews(context);
}

export async function deactivate(): Promise<void> {
  try {
    await stopLspClient();
  } finally {
    disposeOutputChannel();
  }
}
