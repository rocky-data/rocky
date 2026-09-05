/**
 * The shell's client router: three lanes under `/ui/`. The server answers
 * the shell for every `/ui/*` path (U2-P1), so a deep link loads; in the
 * page, a lane change is a `pushState`, not a reload.
 */

import { useEffect, useState } from "react";

export const UI_BASE = "/ui";

export type Lane = "estate" | "review" | "governor";

export const LANES: readonly { id: Lane; label: string }[] = [
  { id: "estate", label: "Estate" },
  { id: "review", label: "Review" },
  { id: "governor", label: "Governor" },
];

/** The lane a path selects. Anything unknown, including `/ui/`, is the estate. */
export function laneFromPath(pathname: string): Lane {
  const rest = pathname.startsWith(UI_BASE) ? pathname.slice(UI_BASE.length) : pathname;
  const first = rest.split("/").filter((s) => s.length > 0)[0];
  return first === "review" || first === "governor" ? first : "estate";
}

export function pathForLane(lane: Lane): string {
  return `${UI_BASE}/${lane}`;
}

/** Change lane without a reload; `useLane` hears the event. */
export function navigate(lane: Lane, win: Window = window): void {
  win.history.pushState(null, "", pathForLane(lane));
  win.dispatchEvent(new PopStateEvent("popstate"));
}

/** The current lane, following the address bar and `navigate`. */
export function useLane(): Lane {
  const [lane, setLane] = useState<Lane>(() => laneFromPath(window.location.pathname));
  useEffect(() => {
    const onChange = () => setLane(laneFromPath(window.location.pathname));
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);
  return lane;
}
