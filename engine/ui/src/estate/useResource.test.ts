import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useResource } from "./useResource";

/** A promise whose settlement the test holds. */
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("useResource", () => {
  /// The value on hand belongs to the identity it was loaded for. When the
  /// identity moves, the hook must not show the old value under the new one
  /// for even a frame: on the review screen that frame was plan A's approve
  /// command under plan B's heading (#1815). So the check is synchronous,
  /// right after the rerender, before any load has settled.
  it("shows loading, not the previous value, the moment its identity changes", async () => {
    const reads: Record<string, ReturnType<typeof deferred<string>>> = {
      a: deferred<string>(),
      b: deferred<string>(),
    };
    const { result, rerender } = renderHook(
      ({ id }: { id: string }) => useResource(() => reads[id].promise, [id]),
      { initialProps: { id: "a" } },
    );
    expect(result.current.kind).toBe("loading");

    await act(async () => reads.a.resolve("A's value"));
    expect(result.current).toMatchObject({ kind: "ready", value: "A's value" });

    rerender({ id: "b" });
    expect(result.current.kind).toBe("loading");

    await act(async () => reads.b.resolve("B's value"));
    expect(result.current).toMatchObject({ kind: "ready", value: "B's value" });

    // And back: A's old value is not a cache. It was for an identity the hook
    // has since left, and a fresh read is in flight.
    reads.a = deferred<string>();
    rerender({ id: "a" });
    expect(result.current.kind).toBe("loading");
  });

  /// A reload of the SAME identity — the interval, a refresh button — keeps
  /// the value up until the new one lands. A panel that blinked to "loading"
  /// on every tick would be worse than one that never refreshed.
  it("keeps the current value across a reload of the same identity", async () => {
    let pending = deferred<number>();
    const { result } = renderHook(() => useResource(() => pending.promise, ["same"]));
    await act(async () => pending.resolve(1));
    expect(result.current).toMatchObject({ kind: "ready", value: 1 });

    pending = deferred<number>();
    act(() => result.current.reload());
    expect(result.current).toMatchObject({ kind: "ready", value: 1 });

    await act(async () => pending.resolve(2));
    expect(result.current).toMatchObject({ kind: "ready", value: 2 });
  });

  /// The interval does not start a read while one is out. A new read drops
  /// the one before it, so a route slower than the interval would otherwise
  /// never land a value after the first. `reload` still starts one at once.
  it("does not let the interval drop a read that is still in flight", async () => {
    vi.useFakeTimers();
    try {
      let pending = deferred<number>();
      const load = vi.fn(() => pending.promise);
      const { result } = renderHook(() => useResource(load, ["same"], 1_000));
      await act(async () => pending.resolve(1));
      expect(load).toHaveBeenCalledTimes(1);

      pending = deferred<number>();
      await act(() => vi.advanceTimersByTimeAsync(1_000));
      expect(load).toHaveBeenCalledTimes(2);
      await act(() => vi.advanceTimersByTimeAsync(3_000));
      expect(load).toHaveBeenCalledTimes(2);

      await act(async () => pending.resolve(2));
      expect(result.current).toMatchObject({ kind: "ready", value: 2 });

      // Settled, so the interval reads again; a reload never waits.
      pending = deferred<number>();
      await act(() => vi.advanceTimersByTimeAsync(1_000));
      expect(load).toHaveBeenCalledTimes(3);
      act(() => result.current.reload());
      expect(load).toHaveBeenCalledTimes(4);
    } finally {
      vi.useRealTimers();
    }
  });
});
