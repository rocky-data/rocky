import type { RunHistoryRecord } from "@rocky-types/history";

/**
 * `GET /api/v1/runs` returns at most this many runs, newest first
 * (`history_runs_output`, `store.list_runs(50)`). Every number built from it
 * is a number "of the last N runs", never of all history.
 */
export const RUNS_ROUTE_CAP = 50;

/** How a run's status reads on the Inbox. Skipped did no work; it is not blocked. */
export type RunBucket = "succeeded" | "partly_failed" | "failed" | "skipped" | "other";

export function runBucket(status: string): RunBucket {
  switch (status.toLowerCase()) {
    case "success":
      return "succeeded";
    case "partialfailure":
    case "partial_failure":
      return "partly_failed";
    case "failure":
      return "failed";
    case "skippedidempotent":
    case "skippedinflight":
      return "skipped";
    default:
      return "other";
  }
}

/** The fill for each bucket, as theme tokens. */
export const BUCKET_FILL: Record<RunBucket, string> = {
  succeeded: "bg-ok",
  partly_failed: "bg-warn",
  failed: "bg-fail",
  skipped: "bg-line",
  other: "bg-line",
};

export const BUCKET_LABEL: Record<RunBucket, string> = {
  succeeded: "succeeded",
  partly_failed: "partly failed",
  failed: "failed",
  skipped: "skipped",
  other: "other status",
};

/**
 * The window every runs number names: the route's newest-50 cap, or all of
 * recorded history when fewer runs exist.
 */
export function windowPhrase(count: number): string {
  return count < RUNS_ROUTE_CAP
    ? `of the last ${count} ${count === 1 ? "run" : "runs"} (all that are recorded)`
    : `of the last ${count} runs`;
}

export type BucketCounts = Record<RunBucket, number>;

/** Counts per bucket. They always sum to `runs.length`. */
export function bucketCounts(runs: RunHistoryRecord[]): BucketCounts {
  const counts: BucketCounts = { succeeded: 0, partly_failed: 0, failed: 0, skipped: 0, other: 0 };
  for (const run of runs) counts[runBucket(run.status)] += 1;
  return counts;
}

/** A lane key: a pipeline name, or `null` for runs that recorded none. */
export interface RunLane {
  pipeline: string | null;
  /** Oldest first, so a lane reads left to right in time. */
  runs: RunHistoryRecord[];
}

/**
 * Runs grouped by pipeline. The route carries `pipeline` for every
 * `rocky run --pipeline` run and `null` for a model-only or backfill run,
 * so those share one lane that says so. Lanes are ordered by their newest
 * run, newest lane first; the no-pipeline lane goes last.
 */
export function groupLanes(runs: RunHistoryRecord[]): RunLane[] {
  const byNewest = [...runs].sort((a, b) => b.started_at.localeCompare(a.started_at));
  const lanes = new Map<string | null, RunHistoryRecord[]>();
  for (const run of byNewest) {
    const key = run.pipeline ?? null;
    const list = lanes.get(key);
    if (list) list.push(run);
    else lanes.set(key, [run]);
  }
  const named = [...lanes].filter(([key]) => key !== null);
  const unnamed = lanes.get(null);
  const ordered: [string | null, RunHistoryRecord[]][] = unnamed ? [...named, [null, unnamed]] : named;
  return ordered.map(([pipeline, list]) => ({ pipeline, runs: [...list].reverse() }));
}

/** One model of a run is ok only when it says `success`. */
export function modelOk(status: string): boolean {
  return status.toLowerCase() === "success";
}

/**
 * A model square's fill. The run path writes `success` or `failed`; a status
 * this does not know is grey, neither ok nor failed.
 */
export function modelFill(status: string): string {
  switch (status.toLowerCase()) {
    case "success":
      return "bg-ok";
    case "failed":
    case "failure":
    case "fail":
    case "error":
      return "bg-fail";
    default:
      return "bg-line";
  }
}

export interface LaneHealth {
  pipeline: string | null;
  latest: RunHistoryRecord;
  ok: number;
  total: number;
  notOk: { model: string; status: string }[];
}

/** Each lane's newest run, with its per-model reading. */
export function laneHealth(lanes: RunLane[]): LaneHealth[] {
  return lanes.map((lane) => {
    const latest = lane.runs[lane.runs.length - 1];
    const notOk = latest.models
      .filter((model) => !modelOk(model.status))
      .map((model) => ({ model: model.model_name, status: model.status }));
    return {
      pipeline: lane.pipeline,
      latest,
      ok: latest.models.length - notOk.length,
      total: latest.models.length,
      notOk,
    };
  });
}
