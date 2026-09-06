import { UI_BASE } from "../router";

/** The review lane's root, or one plan's detail page. */
export function reviewPath(planId?: string): string {
  return planId ? `${UI_BASE}/review/${encodeURIComponent(planId)}` : `${UI_BASE}/review`;
}
