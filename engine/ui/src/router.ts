/**
 * The shell's client router: three lanes under `/ui/`, each with optional
 * sub-routes (`/ui/governor/scorecard`). The server answers the shell for
 * every `/ui/*` path (U2-P1), so a deep link loads; in the page, a route
 * change is a `pushState`, not a reload.
 */

import { useEffect, useState } from "react";

export const UI_BASE = "/ui";

export type Lane = "estate" | "review" | "governor";

export const LANES: readonly { id: Lane; label: string }[] = [
  { id: "estate", label: "Estate" },
  { id: "review", label: "Review" },
  { id: "governor", label: "Governor" },
];

/** The segments after `/ui`, empty for the shell's root. */
export function segmentsFromPath(pathname: string): string[] {
  const rest = pathname.startsWith(UI_BASE) ? pathname.slice(UI_BASE.length) : pathname;
  return rest.split("/").filter((s) => s.length > 0);
}

/** The lane a path selects. Anything unknown, including `/ui/`, is the estate. */
export function laneFromPath(pathname: string): Lane {
  const first = segmentsFromPath(pathname)[0];
  return first === "review" || first === "governor" ? first : "estate";
}

/** The segment after the lane, or `null`: `/ui/governor/scorecard` → `scorecard`. */
export function subpathFromPath(pathname: string): string | null {
  return segmentsFromPath(pathname)[1] ?? null;
}

export function pathForLane(lane: Lane, subpath?: string): string {
  return subpath ? `${UI_BASE}/${lane}/${subpath}` : `${UI_BASE}/${lane}`;
}

/** Change route without a reload; the hooks below hear the event. */
export function navigateTo(path: string, win: Window = window): void {
  win.history.pushState(null, "", path);
  win.dispatchEvent(new PopStateEvent("popstate"));
}

export function navigate(lane: Lane, win: Window = window): void {
  navigateTo(pathForLane(lane), win);
}

function usePathname(): string {
  const [pathname, setPathname] = useState(() => window.location.pathname);
  useEffect(() => {
    const onChange = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", onChange);
    return () => window.removeEventListener("popstate", onChange);
  }, []);
  return pathname;
}

/** The current lane, following the address bar and `navigate`. */
export function useLane(): Lane {
  return laneFromPath(usePathname());
}

/** The current sub-route within the lane, following the address bar. */
export function useSubpath(): string | null {
  return subpathFromPath(usePathname());
}
