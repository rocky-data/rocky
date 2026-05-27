import * as vscode from "vscode";
import { renderAppHtml, type RenderAppOptions } from "./html";
import type {
  HostToWebview,
  PushMessage,
  RpcRequest,
  WebviewToHost,
} from "./protocol";

/** Handles an RPC request's params, returning the result sent back to the webview. */
export type RequestHandler = (params: unknown) => Promise<unknown> | unknown;

/**
 * Wraps a single {@link vscode.Webview} — the interface common to WebviewView
 * and WebviewPanel — with Rocky's webview conventions: a CSP-correct scaffold
 * for a code-split React app, typed request/response handlers, and buffered
 * push (queued until the app posts `ready`, mirroring the handshake in
 * resultGrid.ts).
 */
export class WebviewHost {
  private ready = false;
  private readonly pending: HostToWebview[] = [];
  private readonly handlers = new Map<string, RequestHandler>();
  private readonly disposables: vscode.Disposable[] = [];

  constructor(
    private readonly webview: vscode.Webview,
    private readonly extensionUri: vscode.Uri,
  ) {
    webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(extensionUri, "dist", "webviews"),
      ],
    };
    this.disposables.push(
      webview.onDidReceiveMessage((msg: WebviewToHost) => {
        void this.onMessage(msg);
      }),
    );
  }

  /** Render the React app identified by `entry`, resetting the ready/buffer state. */
  render(opts: Omit<RenderAppOptions, "extensionUri">): void {
    this.ready = false;
    this.pending.length = 0;
    this.webview.html = renderAppHtml(this.webview, {
      ...opts,
      extensionUri: this.extensionUri,
    });
  }

  /** Register a handler for RPC `method`; its return value is sent back to the caller. */
  onRequest(method: string, handler: RequestHandler): void {
    this.handlers.set(method, handler);
  }

  /** Push a fire-and-forget message, buffered until the webview is ready. */
  push<T extends string, P>(type: T, payload: P): void {
    const msg: PushMessage<T, P> = { kind: "push", type, payload };
    if (this.ready) {
      void this.webview.postMessage(msg);
    } else {
      this.pending.push(msg);
    }
  }

  dispose(): void {
    for (const d of this.disposables) d.dispose();
    this.disposables.length = 0;
    this.handlers.clear();
    this.pending.length = 0;
  }

  private async onMessage(msg: WebviewToHost): Promise<void> {
    if (msg.kind === "ready") {
      this.ready = true;
      for (const queued of this.pending.splice(0)) {
        void this.webview.postMessage(queued);
      }
      return;
    }
    if (msg.kind === "req") {
      await this.handleRequest(msg);
    }
  }

  private async handleRequest(req: RpcRequest): Promise<void> {
    const handler = this.handlers.get(req.method);
    if (!handler) {
      this.respondError(req.id, `No handler for method "${req.method}"`);
      return;
    }
    try {
      const result = await handler(req.params);
      void this.webview.postMessage({
        kind: "res",
        id: req.id,
        ok: true,
        result,
      });
    } catch (err) {
      this.respondError(req.id, err instanceof Error ? err.message : String(err));
    }
  }

  private respondError(id: string, error: string): void {
    void this.webview.postMessage({ kind: "res", id, ok: false, error });
  }
}
