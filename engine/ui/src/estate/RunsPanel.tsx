import type { HistoryOutput } from "@rocky-types/history";
import { EmptyState, type Tone } from "../components";
import { formatDuration, formatInstant, orNotRecorded, shortId } from "../format";

function statusTone(status: string): Tone {
  switch (status.toLowerCase()) {
    case "success":
      return "ok";
    case "partialfailure":
    case "partial_failure":
      return "warn";
    case "failure":
      return "risk";
    default:
      return "muted";
  }
}

const TONE_TEXT: Record<Tone, string> = {
  ok: "text-emerald-700 dark:text-emerald-400",
  warn: "text-amber-700 dark:text-amber-400",
  risk: "text-red-700 dark:text-red-400",
  muted: "text-zinc-600 dark:text-zinc-300",
  pending: "text-zinc-600 dark:text-zinc-300",
};

/**
 * The run ledger's newest rows, from `GET /api/v1/runs` (the last 50). A
 * run is a custody fact; the rate of runs is Grafana's and is not shown.
 */
export function RunsPanel({ history, now }: { history: HistoryOutput; now?: number }) {
  if (history.runs.length === 0) {
    return (
      <EmptyState title="No runs recorded" detail="The state store holds no run yet." />
    );
  }
  return (
    <div>
      <table className="w-full text-left text-xs" aria-label="Runs">
        <thead className="text-zinc-500 dark:text-zinc-400">
          <tr>
            <th className="pr-3 font-medium">run</th>
            <th className="pr-3 font-medium">started</th>
            <th className="pr-3 font-medium">status</th>
            <th className="pr-3 font-medium">trigger</th>
            <th className="pr-3 font-medium">pipeline</th>
            <th className="pr-3 text-right font-medium">models</th>
            <th className="text-right font-medium">duration</th>
          </tr>
        </thead>
        <tbody className="text-zinc-900 dark:text-zinc-100">
          {history.runs.map((run) => (
            <tr key={run.run_id} className="border-t border-zinc-100 dark:border-zinc-800">
              <td className="pr-3 font-mono" title={run.run_id}>
                {shortId(run.run_id)}
              </td>
              <td className="pr-3">{formatInstant(run.started_at, now)}</td>
              <td className={`pr-3 font-medium ${TONE_TEXT[statusTone(run.status)]}`}>
                {run.status}
              </td>
              <td className="pr-3">{run.trigger}</td>
              <td className="pr-3">{orNotRecorded(run.pipeline)}</td>
              <td className="pr-3 text-right">{run.models_executed}</td>
              <td className="text-right">{formatDuration(run.duration_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
        {history.count} run(s) in the newest 50
      </p>
    </div>
  );
}
