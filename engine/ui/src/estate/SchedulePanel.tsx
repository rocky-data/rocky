import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import { EmptyState, StatusCard, type Tone } from "../components";
import { NOT_RECORDED, formatInstant, orNotRecorded } from "../format";

function lockTone(state: string): Tone {
  switch (state) {
    case "free":
    case "never":
      return "ok";
    case "held":
      return "pending";
    case "wedged":
      return "risk";
    default:
      return "muted";
  }
}

/**
 * The scheduler's posture, from `GET /api/v1/schedule`: state, not a series.
 * A field the engine did not record renders as "not recorded".
 */
export function SchedulePanel({ status, now }: { status: ScheduleStatusOutput; now?: number }) {
  const { counts } = status;
  return (
    <div className="space-y-3">
      <div className="grid gap-2 sm:grid-cols-3 lg:grid-cols-6">
        <StatusCard label="scheduled" value={counts.scheduled} />
        <StatusCard label="enabled" value={counts.enabled} tone={counts.enabled > 0 ? "ok" : "muted"} />
        <StatusCard label="overdue" value={counts.overdue} tone={counts.overdue > 0 ? "warn" : "muted"} />
        <StatusCard label="in flight" value={counts.in_flight} tone={counts.in_flight > 0 ? "pending" : "muted"} />
        <StatusCard label="throttled" value={counts.throttled} tone={counts.throttled > 0 ? "warn" : "muted"} />
        <StatusCard
          label="config errors"
          value={counts.config_errors}
          tone={counts.config_errors > 0 ? "risk" : "muted"}
        />
      </div>
      <StatusCard
        label="tick lock"
        value={status.tick_lock.state}
        tone={lockTone(status.tick_lock.state)}
        sub={
          status.tick_lock.heartbeat_age_seconds === null ||
          status.tick_lock.heartbeat_age_seconds === undefined
            ? `heartbeat ${NOT_RECORDED} · evaluated in ${status.timezone}`
            : `heartbeat ${status.tick_lock.heartbeat_age_seconds}s ago · evaluated in ${status.timezone}`
        }
      />
      {status.pipelines.length === 0 ? (
        <EmptyState
          title="No schedules configured"
          detail="No pipeline declares a [schedule], so the resident scheduler has nothing to evaluate."
        />
      ) : (
        <table className="w-full text-left text-xs" aria-label="Schedules">
          <thead className="text-zinc-500 dark:text-zinc-400">
            <tr>
              <th className="pr-3 font-medium">pipeline</th>
              <th className="pr-3 font-medium">enabled</th>
              <th className="pr-3 font-medium">cron</th>
              <th className="pr-3 font-medium">next fire</th>
              <th className="pr-3 font-medium">last outcome</th>
              <th className="pr-3 text-right font-medium">failures</th>
              <th className="font-medium">throttle</th>
            </tr>
          </thead>
          <tbody className="text-zinc-900 dark:text-zinc-100">
            {status.pipelines.map((pipeline) => (
              <tr key={pipeline.pipeline} className="border-t border-zinc-100 dark:border-zinc-800">
                <td className="pr-3 font-medium">
                  {pipeline.pipeline}
                  {pipeline.config_error ? (
                    <span className="ml-1 text-red-700 dark:text-red-400">
                      config error: {pipeline.config_error}
                    </span>
                  ) : null}
                </td>
                <td className="pr-3">{pipeline.enabled ? "yes" : "no"}</td>
                <td className="pr-3 font-mono">{orNotRecorded(pipeline.cron)}</td>
                <td className="pr-3">{formatInstant(pipeline.next_fire_at, now)}</td>
                <td className="pr-3">{orNotRecorded(pipeline.last_attempt_outcome)}</td>
                <td className={`pr-3 text-right ${pipeline.consecutive_failures > 0 ? "text-red-700 dark:text-red-400" : ""}`}>
                  {pipeline.consecutive_failures}
                </td>
                <td>
                  {pipeline.throttle
                    ? `${pipeline.throttle.kind} until ${formatInstant(pipeline.throttle.resume_at)}`
                    : "none"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
