import { useCallback, useState, type ReactNode } from "react";
import type { BriefDecisionEntry, BriefOutput, BriefRunEntry, BriefSinceMode } from "@rocky-types/brief";
import { apiGet } from "../api";
import { ArrowPathIcon, ArrowRightIcon, ExclamationTriangleIcon } from "@heroicons/react/20/solid";
import { Clip, Missing, READ_BUTTON, ScreenHeader, StatusCard, ToneDot, runStatusTone } from "../components";
import { useResource } from "../estate/useResource";
import { NOT_RECORDED, capabilityLabel, formatDuration, formatInstant, verdictLabel } from "../format";
import { reviewPath } from "../review/paths";
import { navigateTo } from "../router";
import { defaultJobClient, type JobClient } from "../operator";
import { FreezeControls } from "./FreezeControls";
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

/** The one reading of a missing value in the Inbox: a grey "-", the reason in `title`. */
const NO_VALUE = "the engine did not record it";

function usd(value: number | null | undefined): ReactNode {
  return value === null || value === undefined ? (
    <Missing reason="no cost was recorded; some warehouses, DuckDB among them, have no billing" />
  ) : (
    `$${value.toFixed(2)}`
  );
}

function bytes(value: number | null | undefined): ReactNode {
  if (value === null || value === undefined) return <Missing reason="no bytes-scanned figure was recorded" />;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}

function percent(value: number | null | undefined): ReactNode {
  return value === null || value === undefined ? <Missing reason={NO_VALUE} /> : `${(value * 100).toFixed(1)}%`;
}

function duration(ms: number): ReactNode {
  const text = formatDuration(ms);
  return text === NOT_RECORDED ? <Missing reason="the duration was not recorded" /> : text;
}

function instant(iso: string | null | undefined, now?: number): ReactNode {
  const text = formatInstant(iso, now);
  return text === NOT_RECORDED ? <Missing reason="no timestamp was recorded" /> : text;
}

/**
 * The governor's estate digest, `GET /api/v1/brief`: nine sections, every
 * line citing a ledger id, each section rendered by its availability.
 * Escalations first: "what needs me" is the question the digest answers.
 */
export function BriefScreen({
  load = defaultBriefLoader,
  now,
  jobs = defaultJobClient,
}: {
  load?: BriefLoader;
  now?: number;
  jobs?: JobClient;
}) {
  const [since, setSince] = useState<BriefSinceMode>("7d");
  const loader = useCallback(() => load(since), [load, since]);
  const brief = useResource(loader, [loader]);

  return (
    <div className="space-y-6">
      <ScreenHeader title="Inbox" detail="What waits on a person, then what the project did.">
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
      {brief.kind === "ready" && <BriefBody brief={brief.value} now={now} jobs={jobs} onChanged={brief.reload} />}
    </div>
  );
}

function BriefBody({
  brief,
  now,
  jobs,
  onChanged,
}: {
  brief: BriefOutput;
  now?: number;
  jobs: JobClient;
  onChanged: () => void;
}) {
  const {
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
      <StatusLine brief={brief} />

      <WaitingCard brief={brief} now={now} />

      <p className="text-xs text-muted">
        Digest generated {formatInstant(brief.generated_at, now)}, window <code>{brief.since_mode}</code>
        {brief.since_timestamp ? ` from ${formatInstant(brief.since_timestamp)}` : ", all of recorded history"}
        . Read from <code>GET /api/v1/brief?since={brief.since_mode}</code>.
      </p>

      <h3 className="pt-2 text-xs font-semibold tracking-wider text-muted uppercase">Details</h3>

      <SectionCard
        greyMissing
        title="Agent activity"
        availability={activity.availability}
        note={activity.note}
        summary={`${activity.total} policy evaluations: ${activity.allow} allowed, ${activity.require_review} needed approval, ${activity.deny} blocked`}
      >
        <div className="space-y-4">
          <EffectBar allow={activity.allow} review={activity.require_review} deny={activity.deny} />
          <Rows
            ariaLabel="Activity by actor"
            columns={["actor", "total", "allowed", "needed approval", "blocked"]}
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
            columns={["when", "actor", "action", "model", "kind", "outcome", "rule", "decision", "reason"]}
            rows={activity.decisions.map((entry) => [
              instant(entry.timestamp, now),
              entry.principal,
              entry.capability,
              entry.model,
              // The counters above count evaluations only (#2043); a freeze,
              // unfreeze or verification row is listed with its kind so its
              // recorded effect never reads as a policy verdict.
              entry.kind,
              verdictLabel(entry),
              entry.rule_id === null || entry.rule_id === undefined ? "default" : `rule ${entry.rule_id}`,
              entry.decision_ref,
              entry.reason,
            ])}
          />
        </div>
      </SectionCard>

      <SectionCard
        greyMissing
        title="Runs"
        availability={runs.availability}
        note={runs.note}
        summary={`${runs.total} runs: ${runs.succeeded} succeeded, ${runs.partial_failure} partly failed, ${runs.failed} failed`}
      >
        {runs.attention.length === 0 ? (
          <p className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-300">
            <ToneDot tone="ok" />
            no run needs attention
          </p>
        ) : (
          <p className="text-sm text-ink-2">
            {runs.attention.length} {runs.attention.length === 1 ? "run needs" : "runs need"} attention, listed
            under Waiting on you.
          </p>
        )}
      </SectionCard>

      <SectionCard
        greyMissing
        title="Agent limits"
        availability={autonomy.availability}
        note={autonomy.note}
        emptyLine="no rule is degraded and no change freeze is in force"
        summary={`${autonomy.degraded_rules.length} degraded rule(s), ${autonomy.active_freezes.length} change freeze(s)`}
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
            ariaLabel="Active change freezes"
            columns={["scope", "actor", "frozen", "decision"]}
            rows={autonomy.active_freezes.map((freeze) => [
              freeze.scope,
              freeze.principal,
              instant(freeze.frozen_at, now),
              <CustodyLink key={freeze.plan_id} subject={freeze.plan_id} />,
            ])}
          />
        </div>
      </SectionCard>

      <FreezeControls
        freezes={autonomy.availability === "available" ? autonomy.active_freezes : []}
        jobs={jobs}
        onChanged={onChanged}
      />

      <SectionCard
        greyMissing
        title="Cost"
        availability={cost.availability}
        note={cost.note}
        summary={`${cost.run_count} run(s), ${formatDuration(cost.total_duration_ms)}`}
      >
        <div className="space-y-3">
          <div className="grid gap-3 sm:grid-cols-3">
            <StatusCard label="total cost" value={usd(cost.total_cost_usd)} sub={cost.adapter_type ? cost.adapter_type : <Missing reason="no warehouse adapter was recorded for these runs" />} />
            <StatusCard label="bytes scanned" value={bytes(cost.total_bytes_scanned)} />
            <StatusCard
              label="budget"
              value={
                cost.budget
                  ? `${cost.budget.runs_over_budget} over $${cost.budget.max_usd_per_run.toFixed(2)}/run`
                  : <Missing reason="no cost budget is configured" />
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
              duration(run.duration_ms),
              bytes(run.bytes_scanned),
              usd(run.cost_usd),
            ])}
          />
        </div>
      </SectionCard>

      <div className="grid gap-5 lg:grid-cols-2">
      <SectionCard greyMissing title="Drift" availability={drift.availability} note={drift.note} summary={`${drift.events.length} event(s)`}>
        <Rows
          ariaLabel="Drift events"
          columns={["when", "change", "graph"]}
          rows={drift.events.map((event) => [instant(event.timestamp, now), event.change, event.graph_hash])}
        />
      </SectionCard>

      <SectionCard greyMissing title="Freshness" availability={freshness.availability} note={freshness.note} summary={`${freshness.models.length} model(s)`}>
        <Rows
          ariaLabel="Freshness"
          columns={["model", "lag", "observed", "run"]}
          rows={freshness.models.map((entry) => [
            entry.model_name,
            duration(entry.freshness_lag_seconds * 1000),
            instant(entry.observed_at, now),
            entry.run_id,
          ])}
        />
      </SectionCard>

      <SectionCard greyMissing title="Quality" availability={quality.availability} note={quality.note} summary={`${quality.models.length} model(s)`}>
        <Rows
          ariaLabel="Quality"
          columns={["model", "rows", "max null rate", "observed", "run"]}
          rows={quality.models.map((entry) => [
            entry.model_name,
            entry.row_count,
            percent(entry.max_null_rate),
            instant(entry.observed_at, now),
            entry.run_id,
          ])}
        />
      </SectionCard>

      </div>

      <SectionCard
        greyMissing
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

/** Why a section is a grey "-": the engine's own note when it gave one. */
function unreadReason(note: string | null | undefined): string {
  return note ? `${NOT_RECORDED}: ${note}` : `${NOT_RECORDED}: the engine could not read this section`;
}

function plural(count: number, one: string, many = `${one}s`): string {
  return `${count} ${count === 1 ? one : many}`;
}

function capitalize(text: string): string {
  return text.length === 0 ? text : text[0].toUpperCase() + text.slice(1);
}

/**
 * The screen's first line, as one sentence: what waits on a person first,
 * then what is fine. A section that could not be read is named as such on
 * its own line and never counted as zero, so "Nothing needs you" is said
 * only when every section behind it was read.
 */
function StatusLine({ brief }: { brief: BriefOutput }) {
  const { escalations, runs, autonomy } = brief;
  const act: string[] = [];
  const fine: string[] = [];
  const unread: { label: string; reason: string }[] = [];

  if (escalations.availability === "available" && escalations.pending.length > 0) {
    const plans = new Set(escalations.pending.map((entry) => entry.plan_id)).size;
    act.push(
      `${plural(plans, "change")} ${plans === 1 ? "waits" : "wait"} on you (${plural(escalations.total, "decision")})`,
    );
  } else if (escalations.availability === "unavailable") {
    unread.push({ label: "approval requests", reason: unreadReason(escalations.note) });
  }

  if (runs.availability === "available") {
    if (runs.attention.length > 0) {
      act.push(`${plural(runs.attention.length, "run")} ${runs.attention.length === 1 ? "needs" : "need"} attention`);
    } else {
      fine.push(`${runs.succeeded} of ${runs.total} runs succeeded`);
    }
  } else if (runs.availability === "no_data") {
    fine.push("no runs in the window");
  } else {
    unread.push({ label: "runs", reason: unreadReason(runs.note) });
  }

  if (autonomy.availability === "available") {
    const freezes = autonomy.active_freezes.length;
    const degraded = autonomy.degraded_rules.length;
    if (freezes + degraded > 0) {
      act.push(
        [
          freezes > 0 ? `${plural(freezes, "change freeze")} in force` : null,
          degraded > 0 ? plural(degraded, "degraded rule") : null,
        ]
          .filter((part) => part !== null)
          .join(", "),
      );
    } else {
      fine.push("no change freeze, no degraded rule");
    }
  } else if (autonomy.availability === "no_data") {
    fine.push("no change freeze, no degraded rule");
  } else {
    unread.push({ label: "agent limits", reason: unreadReason(autonomy.note) });
  }

  const lead =
    act.length > 0 ? `${capitalize(act.join(", "))}.` : unread.length === 0 ? "Nothing needs you." : null;
  return (
    <section aria-label="Summary" className="space-y-1">
      <p className="text-base">
        {lead !== null && (
          <span className={act.length > 0 ? "font-semibold text-ink" : "font-semibold text-ok-ink"}>{lead}</span>
        )}
        {fine.length > 0 && <span className="text-muted"> {capitalize(fine.join(", "))}.</span>}
      </p>
      {unread.map((entry) => (
        <p key={entry.label} className="text-sm text-muted">
          {capitalize(entry.label)}: <Missing reason={entry.reason} />
        </p>
      ))}
    </section>
  );
}

type PlanGroup = { plan: string; entries: BriefDecisionEntry[] };

/** Pending decisions, one group per plan, in the order the engine ranked them. */
function groupByPlan(pending: BriefDecisionEntry[]): PlanGroup[] {
  const groups = new Map<string, BriefDecisionEntry[]>();
  for (const entry of pending) {
    const list = groups.get(entry.plan_id);
    if (list) list.push(entry);
    else groups.set(entry.plan_id, [entry]);
  }
  return [...groups].map(([plan, entries]) => ({ plan, entries }));
}

function unique<T>(values: T[]): T[] {
  return [...new Set(values)];
}

/** A run status in words, through the same reading as its colour (`runStatusTone`). */
function runStatusWord(status: string): string {
  switch (runStatusTone(status)) {
    case "risk":
      return "failed";
    case "warn":
      return "partly failed";
    default:
      return status;
  }
}

/**
 * Everything a person has to act on, in one card: each plan that waits on a
 * review, then each run that failed. When every pending decision gives the
 * same reason, the reason is said once, above them, as their shared cause,
 * so a reader sees one problem and not three.
 */
function WaitingCard({ brief, now }: { brief: BriefOutput; now?: number }) {
  const { escalations, runs, autonomy } = brief;
  const pending = escalations.availability === "available" ? escalations.pending : [];
  const attention: BriefRunEntry[] = runs.availability === "available" ? runs.attention : [];
  const groups = groupByPlan(pending);
  const reasons = unique(pending.map((entry) => entry.reason));
  const shared = pending.length > 1 && reasons.length === 1 ? reasons[0] : null;
  const items = groups.length + attention.length;
  // The summary line puts a change freeze and a degraded rule under what needs
  // a person; the card must not say "caught up" while either holds.
  const freezes = autonomy.availability === "available" ? autonomy.active_freezes.length : 0;
  const degraded = autonomy.availability === "available" ? autonomy.degraded_rules.length : 0;
  const unread = [
    escalations.availability === "unavailable" && { label: "Approval requests", note: escalations.note },
    runs.availability === "unavailable" && { label: "Runs", note: runs.note },
    autonomy.availability === "unavailable" && { label: "Agent limits", note: autonomy.note },
  ].filter((entry) => entry !== false);

  return (
    <section aria-label="Waiting on you" className="r-card overflow-hidden">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-5 py-4">
        <h3 className="text-base font-semibold text-ink">Waiting on you</h3>
        {items > 0 && (
          <span className="text-sm text-muted">
            {plural(items, "item")}
            {shared !== null ? " · 1 shared cause" : ""}
          </span>
        )}
      </div>

      {unread.map((entry) => (
        <p key={entry.label} className="r-row text-sm text-muted">
          {entry.label}: <Missing reason={unreadReason(entry.note)} />
        </p>
      ))}

      {shared !== null && (
        <p className="flex items-start gap-3 border-t border-warn/40 bg-warn-soft px-5 py-3 text-sm text-warn-ink">
          <ExclamationTriangleIcon aria-hidden="true" className="mt-0.5 size-4 shrink-0" />
          <span>
            <span className="font-semibold">Shared cause of all {pending.length} decisions:</span> {shared}
          </span>
        </p>
      )}

      {items > 0 && (
        <ul aria-label="Waiting items">
          {groups.map((group) => {
            const latest = group.entries.map((entry) => entry.timestamp).sort().at(-1);
            const own = unique(group.entries.map((entry) => entry.reason));
            return (
              <li key={group.plan} className="r-row">
                <span aria-hidden="true" className="size-2.5 shrink-0 rounded-full bg-warn" />
                <div className="min-w-0 flex-[1_1_22rem] space-y-1">
                  <ul aria-label="Decisions in this change" className="space-y-1">
                    {group.entries.map((entry) => (
                      <li
                        key={entry.decision_ref}
                        className="flex flex-wrap items-center gap-x-2 gap-y-1 text-[15px] text-ink"
                      >
                        {/* The raw capability stays in the title for anyone who needs the CLI's word. */}
                        <span className="font-semibold" title={entry.capability}>
                          {capabilityLabel(entry.capability)}
                        </span>
                        <span aria-hidden="true">·</span>
                        <span className="font-mono break-all">{entry.model}</span>
                        <span aria-hidden="true">·</span>
                        <span className={entry.principal === "agent" ? "r-tag-agent" : "r-tag-human"}>
                          {entry.principal}
                        </span>
                      </li>
                    ))}
                  </ul>
                  {shared === null && <p className="text-sm text-ink-2">{own.join(" · ")}</p>}
                  <p className="text-xs text-muted">
                    change <CustodyLink subject={group.plan} clip /> · {plural(group.entries.length, "decision")} ·{" "}
                    {instant(latest, now)} ·{" "}
                    {group.entries.map((entry, index) => (
                      <span key={entry.decision_ref}>
                        {index > 0 ? ", " : ""}
                        <Clip value={entry.decision_ref} keepEnds />
                      </span>
                    ))}
                  </p>
                </div>
                <a
                  href={reviewPath(group.plan)}
                  onClick={(event) => {
                    event.preventDefault();
                    navigateTo(reviewPath(group.plan));
                  }}
                  // A navigation, so the read look: solid orange is kept for
                  // the writes that change something.
                  className={`${READ_BUTTON} shrink-0`}
                >
                  Review change
                  <ArrowRightIcon aria-hidden="true" className="size-4" />
                </a>
              </li>
            );
          })}
          {attention.map((run) => (
            <li key={run.run_id} className="r-row">
              <span
                aria-hidden="true"
                className={`size-2.5 shrink-0 rounded-full ${runStatusTone(run.status) === "risk" ? "bg-fail" : "bg-warn"}`}
              />
              <div className="min-w-0 flex-[1_1_22rem] space-y-1">
                <p className="flex flex-wrap items-baseline gap-x-2 text-[15px] text-ink">
                  <span className="font-semibold">Run {runStatusWord(run.status)}</span>
                  {run.failed_models.length > 0 && (
                    <span className="font-mono break-all">
                      {run.failed_models.map((model) => `${model.model_name} (${model.status})`).join(", ")}
                    </span>
                  )}
                </p>
                <p className="text-xs text-muted">
                  run <CustodyLink subject={run.run_id} /> · {run.trigger} · started{" "}
                  {formatInstant(run.started_at, now)}
                </p>
              </div>
            </li>
          ))}
        </ul>
      )}

      {items === 0 && unread.length === 0 && freezes + degraded > 0 && (
        <p className="r-row text-sm text-ink-2">
          <ToneDot tone="warn" />
          Nothing to approve, but{" "}
          {[
            freezes > 0 ? `${plural(freezes, "change freeze")} in force` : null,
            degraded > 0 ? plural(degraded, "degraded rule") : null,
          ]
            .filter((part) => part !== null)
            .join(", ")}
          . See Agent limits.
        </p>
      )}

      {items === 0 && unread.length === 0 && freezes + degraded === 0 && (
        <p className="r-row text-sm text-muted">
          <ToneDot tone="ok" />
          You're all caught up
          {escalations.availability === "no_data" && escalations.note ? ` (${escalations.note})` : ""}
        </p>
      )}
    </section>
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
    { label: "allowed", count: allow, color: "bg-ok" },
    { label: "needed approval", count: review, color: "bg-warn" },
    // Blue, not red: a block is a policy working, not an error.
    { label: "blocked", count: deny, color: "bg-block" },
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
            // `min-w-1`: a share of 1 in 1000 stays visible; the label has the exact counts.
            <span key={part.label} className={`min-w-1 ${part.color}`} style={{ flexGrow: part.count }} />
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
