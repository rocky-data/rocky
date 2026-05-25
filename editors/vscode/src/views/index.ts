import * as vscode from "vscode";
import { registerBranchesView } from "./branchesView";
import { registerExtensionInfoView } from "./extensionInfoView";
import type { ExtensionInfoTreeProvider } from "./extensionInfoView";
import { registerGetStartedView } from "./getStartedView";
import { registerHelpView } from "./helpView";
import { registerModelsView } from "./modelsView";
import { registerPreviewView } from "./previewView";
import type { PreviewTreeProvider } from "./previewView";
import { registerRunsView } from "./runsView";
import { registerSchemaView } from "./schemaView";
import { registerSourcesView } from "./sourcesView";
import { registerQueryResultsView } from "../webviews/resultGrid";
import type { ResultGridViewProvider } from "../webviews/resultGrid";
import { registerLineageView } from "../commands/lineage";

let infoProvider: ExtensionInfoTreeProvider | undefined;
let previewProvider: PreviewTreeProvider | undefined;
let queryResultsProvider: ResultGridViewProvider | undefined;

/**
 * Registers all eight Rocky tree views on the activity bar:
 *   Get Started → Extension Info → Models → Runs → Sources → Schema → Previews → Help
 *
 * The Get Started view also wires the `rocky.hasProject` context that gates
 * the welcome content of every other view.
 */
export function registerViews(context: vscode.ExtensionContext): void {
  registerGetStartedView(context);
  infoProvider = registerExtensionInfoView(context);
  registerModelsView(context);
  registerRunsView(context);
  registerSourcesView(context);
  registerSchemaView(context);
  previewProvider = registerPreviewView(context);
  registerBranchesView(context);
  registerHelpView(context);
  queryResultsProvider = registerQueryResultsView(context);
  registerLineageView(context);
}

export function getPreviewProvider(): PreviewTreeProvider | undefined {
  return previewProvider;
}

export function getQueryResultsProvider(): ResultGridViewProvider | undefined {
  return queryResultsProvider;
}

export function getInfoProvider(): ExtensionInfoTreeProvider | undefined {
  return infoProvider;
}
