import type { ReviewQueueEntry, ReviewQueueOutput } from "@rocky-types/review_queue";
import { apiGet } from "../api";
import { useResource } from "../estate/useResource";
import { ResourceState } from "./ResourceState";
import { Clip } from "../components";
import { formatInstant } from "../format";
import { navigateTo } from "../router";
import { reviewPath } from "./paths";

export type QueueLoader = () => Promise<ReviewQueueOutput>;

export const defaultQueueLoader: QueueLoader = () => apiGet<ReviewQueueOutput>("review/queue");

/** Seconds a plan has waited, as a phrase rather than a number. */
function waited(seconds: number): string {
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function QueueRow({ entry, now }: { entry: ReviewQueueEntry; now?: number }) {
  const href = reviewPath(entry.plan_id);
  return (
    <li className="rounded-md border border-zinc-200 p-3 dark:border-zinc-700">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <a
          href={href}
          onClick={(event) => {
            event.preventDefault();
            navigateTo(href);
          }}
          className="font-mono text-sm text-sky-700 underline-offset-2 hover:underline dark:text-sky-400"
        >
          <Clip value={entry.plan_id} />
        </a>
        <span className="text-[11px] uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
          {entry.capability} · {entry.principal}
        </span>
      </div>
      <p className="mt-1 text-sm text-zinc-800 dark:text-zinc-200">{entry.reason}</p>
      <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-zinc-600 sm:grid-cols-4 dark:text-zinc-300">
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">model</dt>
          <dd className="font-mono break-all">{entry.model}</dd>
        </div>
        {!(entry.models.length === 1 && entry.models[0] === entry.model) && (
          // On a plan-level row `model` is a label — "backfill: 3 model(s)" —
          // and the names are in `models`. Shown only when they differ; on an
          // ordinary row the set is the label and the cell would repeat it.
          <div>
            <dt className="text-zinc-500 dark:text-zinc-400">models</dt>
            <dd className="font-mono break-all">
              {entry.models.length === 0 ? "none the graph can name" : entry.models.join(", ")}
            </dd>
          </div>
        )}
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">blast radius</dt>
          <dd>{entry.blast_radius ?? "not computed"}</dd>
        </div>
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">waited</dt>
          <dd>{waited(entry.staleness_seconds)}</dd>
        </div>
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">recorded</dt>
          <dd>{formatInstant(entry.timestamp, now)}</dd>
        </div>
      </dl>
    </li>
  );
}

/**
 * The pending-review queue, `GET /api/v1/review/queue`, in the engine's own
 * order — the screen never re-ranks.
 *
 * The excluded-row count is shown beside the total on purpose: a queue of 3
 * next to a ledger holding 11 rows is a question, and the engine already
 * answers it (those rows resolve to no plan file, so nothing could approve
 * them).
 */
export function QueuePanel({ load = defaultQueueLoader, now }: { load?: QueueLoader; now?: number }) {
  const queue = useResource(load, [load]);

  if (queue.kind !== "ready") {
    return (
      <section aria-label="The review queue" className="space-y-3">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">The review queue</h2>
        <ResourceState resource={queue} loadingLine="reading the queue…" />
      </section>
    );
  }

  const { pending, total, excluded_non_plan_rows: excluded, ranking } = queue.value;

  return (
    <section aria-label="The review queue" className="space-y-3">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          {total === 0 ? "Nothing is waiting for review" : `${total} waiting for review`}
        </h2>
        <span className="text-[11px] text-zinc-500 dark:text-zinc-400">ranked by {ranking}</span>
      </div>
      {excluded > 0 && (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">
          {excluded} further {excluded === 1 ? "row" : "rows"} in the ledger resolve to no plan
          file, so nothing could approve them. They stay in the audit ledger.
        </p>
      )}
      {pending.length === 0 ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-300">
          No escalation is outstanding. A plan appears here when something asks for a change that
          policy will not let through unreviewed. <code>rocky backfill --model &lt;name&gt;</code>{" "}
          makes one, because a backfill is always review-gated; the model has to be one this
          project compiles, and <code>rocky compile</code> lists those names. Nothing needs to have
          run first. This queue is not only for AI work — a plan is listed the same way whoever
          asked for it.
        </p>
      ) : (
        <ul className="space-y-2">
          {pending.map((entry) => (
            <QueueRow key={entry.decision_ref} entry={entry} now={now} />
          ))}
        </ul>
      )}
    </section>
  );
}
