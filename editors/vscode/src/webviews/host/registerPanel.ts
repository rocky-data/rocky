import * as vscode from "vscode";
import { WebviewHost } from "./WebviewHost";

/** Shared options for registering a React webview app. */
export interface PanelAppOptions {
  /** Bundle name under `dist/webviews/`. */
  entry: string;
  /** Document/panel title. */
  title: string;
  /**
   * Wire request handlers and initial pushes once the host exists. Return a
   * disposable to clean up any subscriptions when the panel/view is disposed.
   */
  setup?: (host: WebviewHost) => vscode.Disposable | void;
}

/**
 * Open an editor-area {@link vscode.WebviewPanel} running the React app `entry`.
 * Returns the panel so the caller can reveal or dispose it.
 */
export function createWebviewPanelApp(
  extensionUri: vscode.Uri,
  viewType: string,
  opts: PanelAppOptions,
  column: vscode.ViewColumn = vscode.ViewColumn.Active,
): vscode.WebviewPanel {
  const panel = vscode.window.createWebviewPanel(viewType, opts.title, column, {
    enableScripts: true,
    retainContextWhenHidden: true,
  });
  const host = new WebviewHost(panel.webview, extensionUri);
  const extra = opts.setup?.(host);
  host.render({ entry: opts.entry, title: opts.title });
  panel.onDidDispose(() => {
    host.dispose();
    if (extra) extra.dispose();
  });
  return panel;
}

/**
 * Handle to a registered webview-view app. Lets a command push data into the
 * view, buffering until the view is first resolved (the {@link WebviewHost}
 * then buffers further until the React app posts `ready`).
 *
 * The latest payload of every message type is remembered across the view's
 * lifetime, not just until first resolve: dragging the panel to another dock
 * (or hiding it in a way that disposes the webview) destroys the host, and
 * the re-resolved view must be re-seeded with the state the old one showed —
 * otherwise it renders blank until the next push.
 */
export class WebviewViewController {
  private host: WebviewHost | undefined;
  private readonly lastByType = new Map<string, unknown>();
  private isVisible = false;

  /** @internal Bind the live host once the view resolves; replays remembered state. */
  attach(host: WebviewHost): void {
    this.host = host;
    for (const [type, payload] of this.lastByType) {
      host.push(type, payload);
    }
  }

  /** @internal Unbind when the view is disposed. */
  detach(): void {
    this.host = undefined;
    this.isVisible = false;
  }

  /** @internal Track the view's visibility (panel tab shown vs hidden). */
  setVisible(visible: boolean): void {
    this.isVisible = visible;
  }

  /**
   * Whether the view is currently visible (resolved and its panel tab shown).
   * Callers use this to avoid pushing/retargeting while the panel is hidden.
   */
  get visible(): boolean {
    return this.host !== undefined && this.isVisible;
  }

  /** Push to the view, buffering (latest per type) until it is resolved and ready. */
  push<P>(type: string, payload: P): void {
    this.lastByType.set(type, payload);
    if (this.host) this.host.push(type, payload);
  }
}

/**
 * Register a {@link vscode.WebviewViewProvider} (sidebar/panel view) running the
 * React app `entry`. Returns a {@link WebviewViewController} the caller can push
 * data through before or after the view is first revealed.
 */
export function registerWebviewViewApp(
  context: vscode.ExtensionContext,
  viewType: string,
  opts: PanelAppOptions,
): WebviewViewController {
  const controller = new WebviewViewController();
  const provider: vscode.WebviewViewProvider = {
    resolveWebviewView: (view) => {
      const host = new WebviewHost(view.webview, context.extensionUri);
      const extra = opts.setup?.(host);
      // Render first: it resets the host's ready flag and push buffer. Only
      // then attach the controller, whose replay of remembered pushes (e.g. a
      // `target` queued before the view existed, or shown by a previous host)
      // must land in the fresh buffer rather than being wiped by render().
      host.render({ entry: opts.entry, title: opts.title });
      controller.attach(host);
      controller.setVisible(view.visible);
      view.onDidChangeVisibility(() => controller.setVisible(view.visible));
      view.onDidDispose(() => {
        host.dispose();
        if (extra) extra.dispose();
        controller.detach();
      });
    },
  };
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(viewType, provider, {
      webviewOptions: { retainContextWhenHidden: true },
    }),
  );
  return controller;
}
