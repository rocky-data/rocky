import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const posted: Array<{ id: string; method: string; params?: unknown }> = [];

vi.mock("../../runtime/rpcClient", async () => {
  const actual =
    await vi.importActual<typeof import("../../runtime/rpcClient")>(
      "../../runtime/rpcClient",
    );
  let client: InstanceType<typeof actual.RpcClient> | undefined;
  return {
    ...actual,
    getRpc: () => {
      client ??= new actual.RpcClient({
        postMessage: (m: unknown) => {
          const req = m as { kind: string; id: string; method: string; params?: unknown };
          if (req.kind !== "req") return;
          posted.push(req);
          const result =
            req.method === "graph"
              ? { nodes: [], edges: [], projectRoot: "/proj/a" }
              : null;
          queueMicrotask(() =>
            window.dispatchEvent(
              new MessageEvent("message", {
                data: { kind: "res", id: req.id, ok: true, result },
              }),
            ),
          );
        },
      } as never);
      return client;
    },
  };
});

import { useLineageGraph } from "./useLineageGraph";

const planCostCalls = (): typeof posted =>
  posted.filter((p) => p.method === "planCost");

function hostPush(type: string): void {
  window.dispatchEvent(
    new MessageEvent("message", { data: { kind: "push", type, payload: {} } }),
  );
}

describe("useLineageGraph plan cost", () => {
  afterEach(cleanup);
  beforeEach(() => {
    posted.length = 0;
  });

  it("drops pushes sent while the overlay is off: toggle-on loads exactly once", async () => {
    const { result } = renderHook(() => useLineageGraph());
    await waitFor(() => expect(result.current.graph).not.toBeNull());

    act(() => {
      for (let i = 0; i < 5; i++) hostPush("planCostChanged");
    });
    expect(planCostCalls()).toHaveLength(0);

    act(() => result.current.toggleOverlay("cost"));
    await waitFor(() => expect(planCostCalls()).toHaveLength(1));
    // Settle: no late replay of the earlier pushes.
    await act(async () => {
      await Promise.resolve();
    });
    expect(planCostCalls()).toHaveLength(1);
  });

  it("asks for the cost of the root the graph was built from", async () => {
    const { result } = renderHook(() => useLineageGraph());
    await waitFor(() => expect(result.current.graph).not.toBeNull());
    act(() => result.current.toggleOverlay("cost"));
    await waitFor(() => expect(planCostCalls()).toHaveLength(1));
    expect(planCostCalls()[0].params).toEqual({ projectRoot: "/proj/a" });
  });

  it("refetches on a push while the overlay is on", async () => {
    const { result } = renderHook(() => useLineageGraph());
    await waitFor(() => expect(result.current.graph).not.toBeNull());
    act(() => result.current.toggleOverlay("cost"));
    await waitFor(() => expect(planCostCalls()).toHaveLength(1));
    act(() => hostPush("planCostChanged"));
    await waitFor(() => expect(planCostCalls()).toHaveLength(2));
  });
});
