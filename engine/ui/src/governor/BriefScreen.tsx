import { useCallback, useState } from "react";
import type { BriefOutput, BriefSinceMode } from "@rocky-types/brief";
import { apiGet } from "../api";
import { ArrowPathIcon, ArrowRightIcon, ExclamationTriangleIcon } from "@heroicons/react/20/solid";
import { Clip, READ_BUTTON, ScreenHeader, StatusCard, ToneDot, type Tone } from "../components";
import { useResource } from "../estate/useResource";
import { formatDuration, formatInstant, orNotRecorded } from "../format";
import { reviewPath } from "../review/paths";
import { navigateTo } from "../router";
import { CustodyLink } from "./links";
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
    <div className="space-y-6">
      <ScreenHeader title="Needs you" detail="What waits on a person, then what the estate did.">
        <div className="flex flex-col items-start gap-1">
          <div className="flex items-center gap-2">
            <label className="text-sm text-zinc-600 dark:text-zinc-300" htmlFor="brief-since">
              Window
            </label>
            <select
              id="brief-since"
              value={since}
              onChange={(event) => setSince(event.target.value as BriefSinceMode)}
              className="h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900"
            >
              {WINDOWS.map((window) => (
                <option key={window.id} value={window.id}>
                  {window.label}
                </option>
              ))}
            </select>
          </div>
          <span className="text-xs text-zinc-500 dark:text-zinc-400">
            {WINDOWS.find((w) => w.id === since)?.hint}
          </span>
        </div>
        <button type="button" onClick={brief.reload} className={READ_BUTTON}>
          <ArrowPathIcon aria-hidden="true" className="size-4" />
          Refresh
        </button>
      </ScreenHeader>
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
    <div className="space-y-5">
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        Digest generated {formatInstant(brief.generated_at, now)}, window <code>{brief.since_mode}</code>
        {brief.since_timestamp ? ` from ${formatInstant(brief.since_timestamp)}` : ", all of recorded history"}
        . Read from <code>GET /api/v1/brief?since={brief.since_mode}</code>.
      </p>

      <Headline brief={brief} />

      <SectionCard
        title="Needs you"
        availability={escalations.availability}
        note={escalations.note}
        emptyLine="no escalation is pending"
        summary={`${escalations.total} pending, ranked by ${escalations.ranking}`}
      >
        <ul aria-label="Pending escalations" className="space-y-3">
          {escalations.pending.map((entry) => (
            <li
              key={entry.decision_ref}
              className="flex flex-wrap items-start gap-x-4 gap-y-3 rounded-lg border border-zinc-200 p-4 dark:border-zinc-700"
            >
              <span className="flex size-9 shrink-0 items-center justify-center rounded-md bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300">
                <ExclamationTriangleIcon aria-hidden="true" className="size-5" />
              </span>
              <div className="min-w-0 flex-[1_1_20rem] space-y-1">
                <p className="flex flex-wrap items-center gap-2 text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                  <span className="font-mono break-all">{entry.model}</span>
                  <span className="rounded-md bg-zinc-100 px-1.5 py-0.5 font-mono text-xs font-medium text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300">
                    {entry.capability}
                  </span>
                </p>
                <p className="text-sm text-zinc-700 dark:text-zinc-300">{entry.reason}</p>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  {entry.principal} · {formatInstant(entry.timestamp, now)} · plan{" "}
                  <CustodyLink subject={entry.plan_id} clip /> · <Clip value={entry.decision_ref} keepEnds />
                </p>
              </div>
              <a
                href={reviewPath(entry.plan_id)}
                onClick={(event) => {
                  event.preventDefault();
                  navigateTo(reviewPath(entry.plan_id));
                }}
                className="inline-flex h-9 shrink-0 items-center gap-1.5 rounded-md bg-orange-500 px-3 text-sm font-semibold text-zinc-950 hover:bg-orange-400 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-500"
              >
                Review plan
                <ArrowRightIcon aria-hidden="true" className="size-4" />
              </a>
            </li>
          ))}
        </ul>
      </SectionCard>

      <SectionCard
        title="Agent activity"
        availability={activity.availability}
        note={activity.note}
        summary={`${activity.total} policy evaluations: ${activity.allow} allow, ${activity.require_review} require review, ${activity.deny} deny`}
      >
        <div className="space-y-4">
          <EffectBar allow={activity.allow} review={activity.require_review} deny={activity.deny} />
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
            columns={["when", "principal", "capability", "model", "kind", "effect", "rule", "decision", "reason"]}
            rows={activity.decisions.map((entry) => [
              formatInstant(entry.timestamp, now),
              entry.principal,
              entry.capability,
              entry.model,
              // The counters above count evaluations only (#2043); a freeze,
              // unfreeze or verification row is listed with its kind so its
              // recorded effect never reads as a policy verdict.
              entry.kind,
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
          <p className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-300">
            <ToneDot tone="ok" />
            no run needs attention
          </p>
        ) : (
          <Rows
            ariaLabel="Runs needing attention"
            columns={["run", "status", "trigger", "started", "finished", "failed models"]}
            rows={runs.attention.map((run) => [
              <CustodyLink key={run.run_id} subject={run.run_id} />,
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
              <CustodyLink key={freeze.plan_id} subject={freeze.plan_id} />,
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
        <div className="space-y-3">
          <div className="grid gap-3 sm:grid-cols-3">
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
              <CustodyLink key={run.run_id} subject={run.run_id} />,
              formatDuration(run.duration_ms),
              bytes(run.bytes_scanned),
              usd(run.cost_usd),
            ])}
          />
        </div>
      </SectionCard>

      <div className="grid gap-5 lg:grid-cols-2">
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

      </div>

      <SectionCard
        title="Scheduler"
        availability={scheduler.availability}
        note={scheduler.note}
        emptyLine="nothing is scheduled"
        summary={`${scheduler.scheduled_pipelines} scheduled, ${scheduler.runs_in_window} run(s) in the window, ${scheduler.failed_in_window} failed`}
      >
        <div className="space-y-2 text-sm text-zinc-900 dark:text-zinc-100">
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

/**
 * One line over the whole digest: what waits on a person, how the runs
 * went, whether autonomy is curtailed. A section that is not available is
 * said so, never counted as zero.
 */
function Headline({ brief }: { brief: BriefOutput }) {
  const { escalations, runs, autonomy } = brief;
  const chips: { key: string; tone: Tone; text: string }[] = [];
  if (escalations.availability === "available") {
    chips.push({
      key: "escalations",
      tone: escalations.total > 0 ? "warn" : "ok",
      text:
        escalations.total === 0
          ? "nothing waits on you"
          : `${escalations.total} ${escalations.total === 1 ? "decision waits" : "decisions wait"} on you`,
    });
  }
  if (runs.availability === "available") {
    const bad = runs.failed + runs.partial_failure;
    chips.push({
      key: "runs",
      tone: runs.failed > 0 ? "risk" : bad > 0 ? "warn" : "ok",
      text:
        bad === 0
          ? `runs healthy: ${runs.succeeded} of ${runs.total} succeeded`
          : `${[
              runs.failed > 0 ? `${runs.failed} failed` : null,
              runs.partial_failure > 0 ? `${runs.partial_failure} partial` : null,
            ]
              .filter((part) => part !== null)
              .join(", ")} of ${runs.total} runs`,
    });
  }
  if (autonomy.availability === "available") {
    const curtailed = autonomy.degraded_rules.length + autonomy.active_freezes.length;
    chips.push({
      key: "autonomy",
      tone: curtailed > 0 ? "warn" : "ok",
      text:
        curtailed === 0
          ? "no freeze, no degraded rule"
          : `${autonomy.active_freezes.length} freeze(s), ${autonomy.degraded_rules.length} degraded rule(s)`,
    });
  }
  if (chips.length === 0) return null;
  return (
    <ul aria-label="Summary" className="flex flex-wrap gap-2">
      {chips.map((chip) => (
        <li
          key={chip.key}
          className="inline-flex items-center gap-2 rounded-full border border-zinc-200 bg-white px-3 py-1 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-200"
        >
          <ToneDot tone={chip.tone} />
          <span className="inline-block first-letter:uppercase">{chip.text}</span>
        </li>
      ))}
    </ul>
  );
}

/**
 * Allow, review and deny as one bar, so the share reads at a glance. The
 * counts are the section's own evaluation counters; the bar is drawn only
 * when there is something to divide.
 */
function EffectBar({ allow, review, deny }: { allow: number; review: number; deny: number }) {
  const total = allow + review + deny;
  if (total === 0) return null;
  const parts = [
    { label: "allowed", count: allow, color: "bg-emerald-500" },
    { label: "needed review", count: review, color: "bg-amber-500" },
    { label: "denied", count: deny, color: "bg-red-500" },
  ];
  return (
    <div>
      <div
        role="img"
        aria-label={parts.map((part) => `${part.count} ${part.label}`).join(", ")}
        className="flex h-2.5 gap-0.5 overflow-hidden rounded-full bg-zinc-100 dark:bg-zinc-800"
      >
        {parts
          .filter((part) => part.count > 0)
          .map((part) => (
            <span key={part.label} className={part.color} style={{ flexGrow: part.count }} />
          ))}
      </div>
      <ul aria-hidden="true" className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-zinc-600 dark:text-zinc-300">
        {parts.map((part) => (
          <li key={part.label} className="inline-flex items-center gap-1.5">
            <span className={`size-2 rounded-sm ${part.color}`} />
            {part.count} {part.label}
          </li>
        ))}
      </ul>
    </div>
  );
}
