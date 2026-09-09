import { useCallback, useEffect, useState, type DependencyList } from "react";
import { ApiError } from "../api";

/** One producer's state: the four ways a typed read can stand. */
export type Resource<T> =
  | { kind: "loading" }
  | { kind: "ready"; value: T }
  | { kind: "refused"; error: ApiError }
  | { kind: "unreachable"; message: string };

/** A settled read, tagged with the identity it was loaded for. */
interface Settled<T> {
  deps: DependencyList;
  state: Resource<T>;
}

function sameDeps(a: DependencyList, b: DependencyList): boolean {
  return a.length === b.length && a.every((value, index) => Object.is(value, b[index]));
}

/**
 * Load a typed payload, expose its state, and reload on demand or on an
 * interval. A refused route is a state, not an exception: the panel shows
 * the envelope and the other panels keep rendering.
 *
 * `deps` is the identity of what `load` reads — a plan id, a product name.
 * A value on hand belongs to the identity it was loaded for, and when the
 * identity moves the value is not shown under the new one, not even for a
 * frame: it is plan A's approve command under plan B's heading (#1815). A
 * reload of the SAME identity (the interval, a refresh button) keeps the old
 * value up until the new one lands, so a refresh never blinks.
 */
export function useResource<T>(
  load: () => Promise<T>,
  deps: DependencyList,
  intervalMs?: number,
): Resource<T> & { reload: () => void } {
  const [settled, setSettled] = useState<Settled<T> | null>(null);
  const [tick, setTick] = useState(0);
  const reload = useCallback(() => setTick((t) => t + 1), []);

  // Derived in render, not reset in an effect: an effect runs after the
  // frame with the stale value has already painted.
  const state: Resource<T> =
    settled !== null && sameDeps(settled.deps, deps) ? settled.state : { kind: "loading" };

  useEffect(() => {
    let cancelled = false;
    // The identity this read is for. A late answer carries it, so even one
    // that slipped past `cancelled` cannot render under a different identity.
    const identity = deps;
    load()
      .then((value) => {
        if (!cancelled) setSettled({ deps: identity, state: { kind: "ready", value } });
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        const state: Resource<T> =
          error instanceof ApiError
            ? { kind: "refused", error }
            : { kind: "unreachable", message: String(error) };
        setSettled({ deps: identity, state });
      });
    return () => {
      cancelled = true;
    };
    // `deps` is the caller's identity for `load`; `tick` forces a reload.
  }, [...deps, tick]);

  useEffect(() => {
    if (intervalMs === undefined || intervalMs <= 0) return;
    const timer = setInterval(reload, intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs, reload]);

  return { ...state, reload };
}
