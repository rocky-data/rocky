import * as vscode from "vscode";
import { registerModelsView } from "./modelsView";
import { registerRunsView } from "./runsView";
import { registerSourcesView } from "./sourcesView";

/**
 * Registers the three Rocky tree views (Models, Runs, Sources) on the
 * activity bar. Each view is self-contained and exposes its own refresh
 * command via package.json menu contributions.
 */
export function registerViews(context: vscode.ExtensionContext): void {
  registerModelsView(context);
  registerRunsView(context);
  registerSourcesView(context);
}
