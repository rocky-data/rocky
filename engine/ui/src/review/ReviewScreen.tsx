import { useSegments } from "../router";
import { PlanDetail, type PlanLoaders } from "./PlanDetail";
import { QueuePanel, type QueueLoader } from "./QueuePanel";

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
  // `/ui/review` → [], `/ui/review/<id>` → ["<id>"].
  const planId = segments[1] ?? null;

  if (planId !== null) {
    return <PlanDetail planId={planId} loaders={planLoaders} />;
  }
  return <QueuePanel load={queueLoad} now={now} />;
}
