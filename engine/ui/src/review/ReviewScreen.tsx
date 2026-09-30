import { useSegments } from "../router";
import { PlanDetail, type PlanLoaders } from "./PlanDetail";
import { QueuePanel, type QueueLoader } from "./QueuePanel";

/**
 * Decodes a URL segment, the way `reviewPath()` encoded it going in.
 *
 * `decodeURIComponent` throws `URIError` on a segment that is not a valid
 * percent-escape (a bare `%`, a cut-off multi-byte sequence) — reachable
 * from a hand-edited address bar or a stale link, not only from this
 * screen's own `reviewPath()` output. This component renders under the
 * shell's top-level error boundary (`App.tsx`), which is not scoped to one
 * screen: an uncaught throw here would blank the whole page, sidebar and
 * navigation included, leaving no way to leave the broken URL. Falling back
 * to the raw segment keeps that case exactly as harmless as it was before
 * this screen decoded at all — the loaders and the escalation filter still
 * see a string, just not the one a valid escape would have decoded to.
 */
function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

/**
 * The review lane: the queue at `/ui/review`, one plan at
 * `/ui/review/{plan_id}`.
 *
 * Read-only, all the way down. The lane offers no control that changes
 * anything — approving is a command the plan page shows you to copy.
 */
export function ReviewScreen({
  queueLoad,
  planLoaders,
  now,
}: {
  queueLoad?: QueueLoader;
  planLoaders?: PlanLoaders;
  now?: number;
}) {
  const segments = useSegments();
  // `/ui/review` → [], `/ui/review/<id>` → ["<id>"]. The segment is still
  // `encodeURIComponent`-escaped (`reviewPath` encodes it going in); decode it
  // here the way `GovernorScreen` decodes its own subject segments, or a plan
  // id with a character that escapes (`draft:orders`) reaches the loaders
  // still encoded and the escalation filter compares against the wrong string
  // (#2090).
  const planId = segments[1] !== undefined ? decodeSegment(segments[1]) : null;

  if (planId !== null) {
    return <PlanDetail planId={planId} loaders={planLoaders} />;
  }
  return <QueuePanel load={queueLoad} now={now} />;
}
