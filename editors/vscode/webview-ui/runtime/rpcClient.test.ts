import { describe, expect, it } from "vitest";
import { RpcClient } from "./rpcClient";

/** Deliver a host→webview message as the real `window` "message" event would. */
function postFromHost(data: unknown): void {
  window.dispatchEvent(new MessageEvent("message", { data }));
}

describe("RpcClient", () => {
  it("resolves a request when a correlated response arrives", async () => {
    const posted: Array<{ id: string; method: string; params: unknown }> = [];
    const client = new RpcClient({
      postMessage: (m) => posted.push(m as (typeof posted)[number]),
    });

    const promise = client.request<string>("echo", "hi");
    expect(posted[0].method).toBe("echo");
    expect(posted[0].params).toBe("hi");

    postFromHost({ kind: "res", id: posted[0].id, ok: true, result: "host: hi" });
    await expect(promise).resolves.toBe("host: hi");
  });

  it("rejects when the response is an error", async () => {
    const posted: Array<{ id: string }> = [];
    const client = new RpcClient({
      postMessage: (m) => posted.push(m as (typeof posted)[number]),
    });

    const promise = client.request("boom");
    postFromHost({ kind: "res", id: posted[0].id, ok: false, error: "nope" });
    await expect(promise).rejects.toThrow("nope");
  });

  it("delivers pushes to subscribers by type and stops after unsubscribe", () => {
    const client = new RpcClient({ postMessage: () => {} });
    const seen: unknown[] = [];
    const off = client.onPush("theme", (p) => seen.push(p));

    postFromHost({ kind: "push", type: "theme", payload: { kind: "dark" } });
    off();
    postFromHost({ kind: "push", type: "theme", payload: { kind: "light" } });

    expect(seen).toEqual([{ kind: "dark" }]);
  });

  it("buffers a push that arrives before any subscriber and replays it on subscribe", () => {
    const client = new RpcClient({ postMessage: () => {} });

    // Push lands before onPush — mirrors the host flushing buffered pushes on
    // `ready` while a lazy panel is still suspended and hasn't subscribed.
    postFromHost({ kind: "push", type: "model", payload: { ok: true } });

    const seen: unknown[] = [];
    client.onPush("model", (p) => seen.push(p));
    expect(seen).toEqual([{ ok: true }]); // replayed to the late subscriber

    // Drained once: a second subscriber attaching later does not re-receive it.
    const seen2: unknown[] = [];
    client.onPush("model", (p) => seen2.push(p));
    expect(seen2).toEqual([]);
  });
});
