import * as vscode from "vscode";
import { registerExtensionInfoView } from "./extensionInfoView";
import type { ExtensionInfoTreeProvider } from "./extensionInfoView";
import { registerGetStartedView } from "./getStartedView";
import { registerHelpView } from "./helpView";
import { registerModelsView } from "./modelsView";
import { registerRunsView } from "./runsView";
import { registerSourcesView } from "./sourcesView";

let infoProvider: ExtensionInfoTreeProvider | undefined;

/**
 * Registers all six Rocky tree views on the activity bar:
 *   Get Started → Extension Info → Models → Runs → Sources → Help
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
  registerHelpView(context);
}

export function getInfoProvider(): ExtensionInfoTreeProvider | undefined {
  return infoProvider;
}
