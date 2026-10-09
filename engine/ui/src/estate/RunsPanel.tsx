import type { HistoryOutput } from "@rocky-types/history";
import { Clip, EmptyState, TABLE, ToneDot, type Tone } from "../components";
import { formatDuration, formatInstant, orNotRecorded } from "../format";

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
      {/*
        The scroller is on the table, not the page. Seven columns do not fit a
        phone, and without this the widest cell pushes the whole page body
        sideways — every panel above and below it included.
      */}
      <div
        className={TABLE.scroller}
        tabIndex={0}
        role="group"
        aria-label="Runs, scrollable"
      >
      <table className={TABLE.table} aria-label="Runs">
        <thead className={TABLE.head}>
          <tr>
            <th className={TABLE.th}>run</th>
            <th className={TABLE.th}>started</th>
            <th className={TABLE.th}>status</th>
            <th className={TABLE.th}>trigger</th>
            <th className={TABLE.th}>pipeline</th>
            <th className={`${TABLE.th} text-right`}>models</th>
            <th className={`${TABLE.th} text-right`}>duration</th>
          </tr>
        </thead>
        <tbody className="text-zinc-900 dark:text-zinc-100">
          {history.runs.map((run) => (
            <tr key={run.run_id} className={TABLE.row}>
              <td className={`${TABLE.td} font-mono`} title={run.run_id}>
                <Clip value={run.run_id} />
              </td>
              <td className={TABLE.td}>{formatInstant(run.started_at, now)}</td>
              <td className={`${TABLE.td} font-medium ${TONE_TEXT[statusTone(run.status)]}`}>
                <span className="inline-flex items-center gap-2">
                  <ToneDot tone={statusTone(run.status)} />
                  {run.status}
                </span>
              </td>
              <td className={TABLE.td}>{run.trigger}</td>
              <td className={TABLE.td}>{orNotRecorded(run.pipeline)}</td>
              <td className={`${TABLE.td} text-right tabular-nums`}>{run.models_executed}</td>
              <td className={`${TABLE.td} text-right tabular-nums`}>{formatDuration(run.duration_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      </div>
      <p className="mt-2 text-xs text-zinc-500 dark:text-zinc-400">
        {history.count} run(s) in the newest 50
      </p>
    </div>
  );
}
