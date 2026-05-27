import type {
  HostToWebview,
  RpcRequest,
} from "../../src/webviews/host/protocol";
import { getVscodeApi } from "./useVscodeApi";

/** Minimal surface of the VS Code API the client needs (injectable for tests). */
export interface PostsMessages {
  postMessage(message: unknown): void;
}

type PushListener = (payload: unknown) => void;

/**
 * Typed client for the host ↔ webview {@link protocol}. `request()` posts an
 * RpcRequest and resolves with the correlated response; `onPush()` subscribes
 * to host pushes by type. The webview-side mirror of the host's WebviewHost.
 */
export class RpcClient {
  private nextId = 0;
  private readonly pendingRequests = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  private readonly pushListeners = new Map<string, Set<PushListener>>();
  // Pushes that arrived before anyone subscribed to their type, kept so a late
  // subscriber still receives them (see onPush / handle for the race).
  private readonly bufferedPushes = new Map<string, unknown[]>();

  constructor(private readonly api: PostsMessages) {
    window.addEventListener("message", (ev: MessageEvent<HostToWebview>) => {
      this.handle(ev.data);
    });
  }

  /** Send an RPC request; resolves with the host's result or rejects on error. */
  request<R = unknown>(method: string, params?: unknown): Promise<R> {
    const id = `r${this.nextId++}`;
    return new Promise<R>((resolve, reject) => {
      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      const msg: RpcRequest = { kind: "req", id, method, params };
      this.api.postMessage(msg);
    });
  }

  /** Subscribe to host pushes of `type`; returns an unsubscribe function. */
  onPush<P = unknown>(type: string, listener: (payload: P) => void): () => void {
    let set = this.pushListeners.get(type);
    if (!set) {
      set = new Set();
      this.pushListeners.set(type, set);
    }
    set.add(listener as PushListener);
    // A push can land before its subscriber mounts: the host flushes buffered
    // pushes when AppShell signals `ready`, which happens while a lazy panel is
    // still suspended and has not yet called onPush. Replay anything buffered
    // for this type so that first subscriber still sees it.
    const buffered = this.bufferedPushes.get(type);
    if (buffered) {
      this.bufferedPushes.delete(type);
      for (const payload of buffered) (listener as PushListener)(payload);
    }
    return () => {
      this.pushListeners.get(type)?.delete(listener as PushListener);
    };
  }

  /** Tell the host the app has mounted, flushing any buffered pushes. */
  signalReady(): void {
    this.api.postMessage({ kind: "ready" });
  }

  private handle(msg: HostToWebview): void {
    if (msg?.kind === "res") {
      const pending = this.pendingRequests.get(msg.id);
      if (!pending) return;
      this.pendingRequests.delete(msg.id);
      if (msg.ok) pending.resolve(msg.result);
      else pending.reject(new Error(msg.error));
    } else if (msg?.kind === "push") {
      const set = this.pushListeners.get(msg.type);
      if (set && set.size > 0) {
        for (const listener of set) listener(msg.payload);
      } else {
        // No subscriber yet — buffer until onPush() attaches for this type.
        const buf = this.bufferedPushes.get(msg.type);
        if (buf) buf.push(msg.payload);
        else this.bufferedPushes.set(msg.type, [msg.payload]);
      }
    }
  }
}

let singleton: RpcClient | undefined;

/** The shared RpcClient for this webview, created lazily on first use. */
export function getRpc(): RpcClient {
  if (!singleton) singleton = new RpcClient(getVscodeApi());
  return singleton;
}
