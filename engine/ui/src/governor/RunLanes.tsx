import type { MouseEvent, ReactNode } from "react";
import type { HistoryOutput, RunHistoryRecord } from "@rocky-types/history";
import { apiGet } from "../api";
import { Missing, runStatusLabel } from "../components";
import type { Resource } from "../estate/useResource";
import { formatInstant } from "../format";
import { custodyPath, navigateTo } from "../router";
import { CustodyLink } from "./links";
import {
  BUCKET_FILL,
  BUCKET_LABEL,
  bucketCounts,
  groupLanes,
  laneHealth,
  modelOk,
  runBucket,
  RUNS_ROUTE_CAP,
  windowPhrase,
  type RunBucket,
} from "./runLaneModel";

export type RunsLoader = () => Promise<HistoryOutput>;

/** Module scope, so its identity never changes and the read happens once. */
export const defaultRunsLoader: RunsLoader = () => apiGet<HistoryOutput>("runs");

const ROUTE = "GET /api/v1/runs";

/** Why every runs number is a grey "-": the read did not land. */
function unreadReason(runs: Resource<HistoryOutput>): string {
  switch (runs.kind) {
    case "loading":
      return "the run history is still loading";
    case "refused":
      return `${ROUTE} was refused (${runs.error.status} ${runs.error.envelope.code}): ${
        runs.error.envelope.remediation_hint ?? runs.error.envelope.message
      }`;
    case "unreachable":
      return `${ROUTE} could not be read: ${runs.message}`;
    case "ready":
      return "";
  }
}

function Tile({ label, value, sub }: { label: string; value: ReactNode; sub: string }) {
  return (
    <div className="rounded-tile border border-line bg-surface-2 px-4 py-3">
      <p className="text-xs font-medium text-muted">{label}</p>
      <p className="mt-1 text-xl font-semibold text-ink tabular-nums">{value}</p>
      <p className="mt-0.5 text-xs text-muted">{sub}</p>
    </div>
  );
}

const TILE_BUCKETS: RunBucket[] = ["succeeded", "partly_failed", "failed"];

function cellTitle(run: RunHistoryRecord): string {
  return `run ${run.run_id} · ${runStatusLabel(run.status)} · ${run.trigger} · started ${formatInstant(run.started_at)}`;
}

/** One run, as a cell that leads to its custody chain. */
function RunCell({ run }: { run: RunHistoryRecord }) {
  const href = custodyPath(run.run_id);
  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    navigateTo(href);
  };
  const title = cellTitle(run);
  return (
    <a
      href={href}
      onClick={onClick}
      title={title}
      aria-label={title}
      data-bucket={runBucket(run.status)}
      className={`block h-6 w-3 shrink-0 rounded-sm ${BUCKET_FILL[runBucket(run.status)]} hover:ring-2 hover:ring-accent`}
    />
  );
}

function laneName(pipeline: string | null): ReactNode {
  return pipeline ?? (
    <span className="text-muted">
      no pipeline <Missing reason="these runs recorded no pipeline: a model-only or backfill run carries none" />
    </span>
  );
}

/**
 * Recent runs from `GET /api/v1/runs`, read once when the Inbox opens: the
 * route locks the state file briefly, so it is never polled. Tiles first,
 * then one lane per pipeline (one cell per run, oldest to newest), then each
 * pipeline's newest run model by model. Every number names its window, the
 * route's newest 50; nothing here uses the digest's window. A read that did
 * not land shows a grey "-" in every tile, never a zero.
 */
export function RunLanes({ runs, now }: { runs: Resource<HistoryOutput>; now?: number }) {
  const ready = runs.kind === "ready" ? runs.value : null;
  const list = ready?.runs ?? null;
  const unrecorded = ready?.unrecorded_runs ?? [];

  return (
    <section aria-label="Recent runs" className="r-card overflow-hidden">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-5 py-4">
        <h3 className="text-base font-semibold text-ink">Recent runs</h3>
        <span className="text-sm text-muted">
          {list === null ? (
            <>
              window <Missing reason={unreadReason(runs)} />
            </>
          ) : (
            `One lane per pipeline, one cell per run, ${windowPhrase(list.length)}`
          )}
        </span>
      </div>

      {list !== null && list.length === 0 ? (
        <p className="r-row text-sm text-muted">No run is recorded in the state store yet.</p>
      ) : (
        <>
          <ul aria-label="Run health" className="grid gap-3 border-t border-divider px-5 py-4 sm:grid-cols-3">
            {TILE_BUCKETS.map((bucket) => {
              const counts = list === null ? null : bucketCounts(list);
              return (
                <li key={bucket} aria-label={`${BUCKET_LABEL[bucket]} tile`}>
                  <Tile
                    label={BUCKET_LABEL[bucket].replace(/^./, (c) => c.toUpperCase())}
                    value={counts === null ? <Missing reason={unreadReason(runs)} /> : counts[bucket]}
                    sub={list === null ? `of up to the last ${RUNS_ROUTE_CAP} runs, not read` : windowPhrase(list.length)}
                  />
                </li>
              );
            })}
          </ul>
          {list !== null && <Lanes runs={list} now={now} />}
        </>
      )}

      {unrecorded.length > 0 && (
        <p className="r-row text-xs text-muted">
          {unrecorded.length} {unrecorded.length === 1 ? "run" : "runs"} started with no run record (still running,
          crashed, or the record write failed), so the list above is not complete. They are not counted.
        </p>
      )}

      <p className="border-t border-divider px-5 py-3 text-xs text-muted">
        Read once from <code>{ROUTE}</code>, which returns the newest 50 runs.
      </p>
    </section>
  );
}

function Lanes({ runs, now }: { runs: RunHistoryRecord[]; now?: number }) {
  const lanes = groupLanes(runs);
  const health = laneHealth(lanes);
  const span = windowPhrase(runs.length);
  return (
    <>
      <ul aria-label="Run lanes" className="space-y-2 border-t border-divider px-5 py-4">
        {lanes.map((lane) => (
          <li key={lane.pipeline ?? "\u0000none"} className="flex items-center gap-3">
            <span className="w-32 shrink-0 truncate font-mono text-sm text-ink-2">{laneName(lane.pipeline)}</span>
            <div className="flex min-w-0 flex-wrap gap-1">
              {lane.runs.map((run) => (
                <RunCell key={run.run_id} run={run} />
              ))}
            </div>
          </li>
        ))}
        <li aria-hidden="true" className="flex flex-wrap gap-x-4 gap-y-1 pt-1 text-xs text-muted">
          {(["succeeded", "partly_failed", "failed", "skipped"] as RunBucket[]).map((bucket) => (
            <span key={bucket} className="inline-flex items-center gap-1.5">
              <span className={`size-2 rounded-sm ${BUCKET_FILL[bucket]}`} />
              {BUCKET_LABEL[bucket]}
            </span>
          ))}
        </li>
      </ul>

      <div className="border-t border-divider px-5 py-4">
        <h4 className="text-sm font-semibold text-ink">Health by pipeline</h4>
        <p className="text-xs text-muted">Each pipeline's newest run {span}, one square per model.</p>
        <ul aria-label="Health by pipeline" className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {health.map((entry) => (
            <li
              key={entry.pipeline ?? "\u0000none"}
              aria-label={`${entry.pipeline ?? "no pipeline"} health`}
              className="rounded-tile border border-line bg-surface-2 px-4 py-3"
            >
              <p className="flex items-baseline justify-between gap-2">
                <span className="truncate font-mono text-sm font-semibold text-ink">{laneName(entry.pipeline)}</span>
                <span className="text-xs text-muted">{runStatusLabel(entry.latest.status)}</span>
              </p>
              {entry.total > 0 && (
                <div aria-hidden="true" className="mt-2 flex flex-wrap gap-1">
                  {entry.latest.models.map((model) => (
                    <span
                      key={model.model_name}
                      title={`${model.model_name}: ${model.status}`}
                      className={`size-3 rounded-sm ${modelOk(model.status) ? "bg-ok" : "bg-fail"}`}
                    />
                  ))}
                </div>
              )}
              <p className="mt-2 text-xs text-ink-2">
                {entry.total === 0
                  ? "the run recorded no model"
                  : `${entry.ok} of ${entry.total} models ok`}
                {entry.notOk.length > 0 &&
                  ` · ${entry.notOk.map((model) => `${model.model} (${model.status})`).join(", ")}`}
              </p>
              <p className="mt-1 text-xs text-muted">
                run <CustodyLink subject={entry.latest.run_id} clip /> · {formatInstant(entry.latest.started_at, now)}
              </p>
            </li>
          ))}
        </ul>
      </div>
    </>
  );
}
