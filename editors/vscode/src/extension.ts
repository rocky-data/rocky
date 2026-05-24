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
import { disposeOutputChannel } from "./output";
import { registerRunDecorations } from "./runDecorations";
import { registerTaskProvider } from "./taskProvider";
import { registerTestExplorer } from "./testExplorer";
import { registerDeclarativeTestProvider } from "./testProvider";
import { registerViews } from "./views";

export function activate(context: vscode.ExtensionContext): void {
  setExtensionUri(context.extensionUri);
  startLspClient(context);
  registerCommands(context);
  registerChatParticipant(context);
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
