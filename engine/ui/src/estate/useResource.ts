import { useCallback, useEffect, useState, type DependencyList } from "react";
import { ApiError } from "../api";

/** One producer's state: the four ways a typed read can stand. */
export type Resource<T> =
  | { kind: "loading" }
  | { kind: "ready"; value: T }
  | { kind: "refused"; error: ApiError }
  | { kind: "unreachable"; message: string };

/**
 * Load a typed payload, expose its state, and reload on demand or on an
 * interval. A refused route is a state, not an exception: the panel shows
 * the envelope and the other panels keep rendering.
 */
export function useResource<T>(
  load: () => Promise<T>,
  deps: DependencyList,
  intervalMs?: number,
): Resource<T> & { reload: () => void } {
  const [state, setState] = useState<Resource<T>>({ kind: "loading" });
  const [tick, setTick] = useState(0);
  const reload = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    load()
      .then((value) => {
        if (!cancelled) setState({ kind: "ready", value });
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        if (error instanceof ApiError) setState({ kind: "refused", error });
        else setState({ kind: "unreachable", message: String(error) });
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
