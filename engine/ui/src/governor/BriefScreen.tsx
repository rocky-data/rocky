import { useCallback, useState } from "react";
import type { BriefOutput, BriefSinceMode } from "@rocky-types/brief";
import { apiGet } from "../api";
import { StatusCard } from "../components";
import { useResource } from "../estate/useResource";
import { formatDuration, formatInstant, orNotRecorded } from "../format";
import { Rows, SectionCard } from "./SectionCard";

export type BriefLoader = (since: BriefSinceMode) => Promise<BriefOutput>;

export const defaultBriefLoader: BriefLoader = (since) =>
  apiGet<BriefOutput>(`brief?since=${since}`);

const WINDOWS: { id: BriefSinceMode; label: string; hint: string }[] = [
  { id: "7d", label: "7 days", hint: "the route's default" },
  { id: "24h", label: "24 hours", hint: "a rolling day" },
  {
    id: "last",
    label: "since the last digest",
    hint: "reads the Slack hook's cursor without moving it",
  },
];

function usd(value: number | null | undefined): string {
  return value === null || value === undefined ? orNotRecorded(null) : `$${value.toFixed(2)}`;
}

function bytes(value: number | null | undefined): string {
  if (value === null || value === undefined) return orNotRecorded(null);
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

function percent(value: number | null | undefined): string {
  return value === null || value === undefined ? orNotRecorded(null) : `${(value * 100).toFixed(1)}%`;
}

/**
 * The governor's estate digest, `GET /api/v1/brief`: nine sections, every
 * line citing a ledger id, each section rendered by its availability.
 * Escalations first: "what needs me" is the question the digest answers.
 */
export function BriefScreen({ load = defaultBriefLoader, now }: { load?: BriefLoader; now?: number }) {
  const [since, setSince] = useState<BriefSinceMode>("7d");
  const loader = useCallback(() => load(since), [load, since]);
  const brief = useResource(loader, [loader]);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-xs text-zinc-600 dark:text-zinc-300" htmlFor="brief-since">
          Window
        </label>
        <select
          id="brief-since"
          value={since}
          onChange={(event) => setSince(event.target.value as BriefSinceMode)}
          className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900"
        >
          {WINDOWS.map((window) => (
            <option key={window.id} value={window.id}>
              {window.label}
            </option>
          ))}
        </select>
        <span className="text-xs text-zinc-500 dark:text-zinc-400">
          {WINDOWS.find((w) => w.id === since)?.hint}
        </span>
        <button
          type="button"
          onClick={brief.reload}
          className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
        >
          Refresh
        </button>
        <code className="text-[11px] text-zinc-500 dark:text-zinc-400">GET /api/v1/brief?since={since}</code>
      </div>
      {brief.kind === "loading" && <p className="text-sm text-zinc-500">Loading the digest…</p>}
      {brief.kind === "refused" && (
        <StatusCard
          label={`refused (${brief.error.status})`}
          value={brief.error.envelope.code}
          tone="risk"
          sub={brief.error.envelope.remediation_hint ?? brief.error.envelope.message}
        />
      )}
      {brief.kind === "unreachable" && (
        <StatusCard label="engine" value="unreachable" tone="risk" sub={brief.message} />
      )}
      {brief.kind === "ready" && <BriefBody brief={brief.value} now={now} />}
    </div>
  );
}

function BriefBody({ brief, now }: { brief: BriefOutput; now?: number }) {
  const {
    escalations,
    agent_activity: activity,
    runs,
    autonomy,
    cost,
    drift,
    freshness,
    quality,
    scheduler,
  } = brief;
  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        Digest generated {formatInstant(brief.generated_at, now)}, window <code>{brief.since_mode}</code>
        {brief.since_timestamp ? ` from ${formatInstant(brief.since_timestamp)}` : ", all of recorded history"}.
      </p>

      <SectionCard
        title="Needs you"
        availability={escalations.availability}
        note={escalations.note}
        emptyLine="no escalation is pending"
        summary={`${escalations.total} pending, ranked by ${escalations.ranking}`}
      >
        <Rows
          ariaLabel="Pending escalations"
          columns={["plan", "model", "capability", "principal", "reason", "decision", "when"]}
          rows={escalations.pending.map((entry) => [
            entry.plan_id,
            entry.model,
            entry.capability,
            entry.principal,
            entry.reason,
            entry.decision_ref,
            formatInstant(entry.timestamp, now),
          ])}
        />
      </SectionCard>

      <SectionCard
        title="Agent activity"
        availability={activity.availability}
        note={activity.note}
        summary={`${activity.total} decisions: ${activity.allow} allow, ${activity.require_review} require review, ${activity.deny} deny`}
      >
        <div className="space-y-2">
          <Rows
            ariaLabel="Activity by principal"
            columns={["principal", "total", "allow", "require review", "deny"]}
            rows={activity.by_principal.map((row) => [
              row.principal,
              row.total,
              row.allow,
              row.require_review,
              row.deny,
            ])}
          />
          <Rows
            ariaLabel="Decisions"
            columns={["when", "principal", "capability", "model", "effect", "rule", "decision", "reason"]}
            rows={activity.decisions.map((entry) => [
              formatInstant(entry.timestamp, now),
              entry.principal,
              entry.capability,
              entry.model,
              entry.effect,
              entry.rule_id === null || entry.rule_id === undefined ? "default" : `rule ${entry.rule_id}`,
              entry.decision_ref,
              entry.reason,
            ])}
          />
        </div>
      </SectionCard>

      <SectionCard
        title="Runs"
        availability={runs.availability}
        note={runs.note}
        summary={`${runs.total} runs: ${runs.succeeded} succeeded, ${runs.partial_failure} partial, ${runs.failed} failed`}
      >
        {runs.attention.length === 0 ? (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">no run needs attention</p>
        ) : (
          <Rows
            ariaLabel="Runs needing attention"
            columns={["run", "status", "trigger", "started", "finished", "failed models"]}
            rows={runs.attention.map((run) => [
              run.run_id,
              run.status,
              run.trigger,
              formatInstant(run.started_at, now),
              formatInstant(run.finished_at, now),
              run.failed_models.map((m) => `${m.model_name} (${m.status})`).join(", ") || "none",
            ])}
          />
        )}
      </SectionCard>

      <SectionCard
        title="Autonomy"
        availability={autonomy.availability}
        note={autonomy.note}
        emptyLine="no rule is degraded and no freeze is in force"
        summary={`${autonomy.degraded_rules.length} degraded rule(s), ${autonomy.active_freezes.length} freeze(s)`}
      >
        <div className="space-y-2">
          <Rows
            ariaLabel="Degraded rules"
            columns={["rule", "failures", "limit", "window"]}
            rows={autonomy.degraded_rules.map((rule) => [
              `rule ${rule.rule_id}`,
              rule.failures,
              rule.limit,
              rule.window,
            ])}
          />
          <Rows
            ariaLabel="Active freezes"
            columns={["scope", "principal", "frozen", "decision"]}
            rows={autonomy.active_freezes.map((freeze) => [
              freeze.scope,
              freeze.principal,
              formatInstant(freeze.frozen_at, now),
              freeze.plan_id,
            ])}
          />
        </div>
      </SectionCard>

      <SectionCard
        title="Cost"
        availability={cost.availability}
        note={cost.note}
        summary={`${cost.run_count} run(s), ${formatDuration(cost.total_duration_ms)}`}
      >
        <div className="space-y-2">
          <div className="grid gap-2 sm:grid-cols-3">
            <StatusCard label="total cost" value={usd(cost.total_cost_usd)} sub={orNotRecorded(cost.adapter_type)} />
            <StatusCard label="bytes scanned" value={bytes(cost.total_bytes_scanned)} />
            <StatusCard
              label="budget"
              value={
                cost.budget
                  ? `${cost.budget.runs_over_budget} over $${cost.budget.max_usd_per_run.toFixed(2)}/run`
                  : orNotRecorded(null)
              }
              tone={cost.budget && cost.budget.runs_over_budget > 0 ? "warn" : "muted"}
              sub={
                cost.budget?.worst_run_id
                  ? `worst ${cost.budget.worst_run_id}: ${usd(cost.budget.worst_run_cost_usd)}`
                  : undefined
              }
            />
          </div>
          <Rows
            ariaLabel="Cost per run"
            columns={["run", "duration", "bytes scanned", "cost"]}
            rows={cost.per_run.map((run) => [
              run.run_id,
              formatDuration(run.duration_ms),
              bytes(run.bytes_scanned),
              usd(run.cost_usd),
            ])}
          />
        </div>
      </SectionCard>

      <SectionCard title="Drift" availability={drift.availability} note={drift.note} summary={`${drift.events.length} event(s)`}>
        <Rows
          ariaLabel="Drift events"
          columns={["when", "change", "graph"]}
          rows={drift.events.map((event) => [formatInstant(event.timestamp, now), event.change, event.graph_hash])}
        />
      </SectionCard>

      <SectionCard title="Freshness" availability={freshness.availability} note={freshness.note} summary={`${freshness.models.length} model(s)`}>
        <Rows
          ariaLabel="Freshness"
          columns={["model", "lag", "observed", "run"]}
          rows={freshness.models.map((entry) => [
            entry.model_name,
            formatDuration(entry.freshness_lag_seconds * 1000),
            formatInstant(entry.observed_at, now),
            entry.run_id,
          ])}
        />
      </SectionCard>

      <SectionCard title="Quality" availability={quality.availability} note={quality.note} summary={`${quality.models.length} model(s)`}>
        <Rows
          ariaLabel="Quality"
          columns={["model", "rows", "max null rate", "observed", "run"]}
          rows={quality.models.map((entry) => [
            entry.model_name,
            entry.row_count,
            percent(entry.max_null_rate),
            formatInstant(entry.observed_at, now),
            entry.run_id,
          ])}
        />
      </SectionCard>

      <SectionCard
        title="Scheduler"
        availability={scheduler.availability}
        note={scheduler.note}
        emptyLine="nothing is scheduled"
        summary={`${scheduler.scheduled_pipelines} scheduled, ${scheduler.runs_in_window} run(s) in the window, ${scheduler.failed_in_window} failed`}
      >
        <div className="space-y-2 text-xs text-zinc-900 dark:text-zinc-100">
          <p>Paused: {scheduler.paused.length === 0 ? "none" : scheduler.paused.join(", ")}</p>
          <p>
            Incidents: {scheduler.incident_count}
            {scheduler.latest_incident ? `, latest ${scheduler.latest_incident}` : ""}
          </p>
          <Rows
            ariaLabel="Consecutive failures"
            columns={["pipeline", "consecutive failures"]}
            rows={scheduler.consecutive_failures.map((entry) => [entry.pipeline, entry.consecutive_failures])}
          />
        </div>
      </SectionCard>
    </div>
  );
}
