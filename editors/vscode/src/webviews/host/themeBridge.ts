import * as vscode from "vscode";
import type { ThemeTokens } from "./protocol";
import type { WebviewHost } from "./WebviewHost";

/** Map a {@link vscode.ColorThemeKind} to coarse theme tokens. */
function tokensFor(kind: vscode.ColorThemeKind): ThemeTokens {
  if (kind === vscode.ColorThemeKind.Light) return { kind: "light" };
  if (kind === vscode.ColorThemeKind.HighContrastLight) {
    return { kind: "high-contrast-light" };
  }
  if (kind === vscode.ColorThemeKind.HighContrast) {
    return { kind: "high-contrast" };
  }
  return { kind: "dark" };
}

/** The active color theme as coarse tokens. */
export function currentThemeTokens(): ThemeTokens {
  return tokensFor(vscode.window.activeColorTheme.kind);
}

/**
 * Push the current theme to `host` and keep it updated as the theme changes.
 * Only needed where JS must branch on the theme (e.g. a canvas minimap
 * palette); ordinary styling tracks the theme through `--vscode-*` CSS
 * variables. Returns a disposable that stops the updates.
 */
export function bridgeTheme(host: WebviewHost): vscode.Disposable {
  host.push("theme", currentThemeTokens());
  return vscode.window.onDidChangeActiveColorTheme((theme) => {
    host.push("theme", tokensFor(theme.kind));
  });
}
