import { useCallback, useState } from "react";
import type { AuditScorecardOutput, ScorecardDimension } from "@rocky-types/audit_scorecard";
import { apiGet } from "../api";
import { StatusCard } from "../components";
import { useResource } from "../estate/useResource";
import { NOT_RECORDED, formatInstant, orNotRecorded } from "../format";
import { Rows, SectionCard } from "./SectionCard";

export type ScorecardLoader = (by: ScorecardDimension, window: string) => Promise<AuditScorecardOutput>;

export const defaultScorecardLoader: ScorecardLoader = (by, window) =>
  apiGet<AuditScorecardOutput>(`audit/scorecard?by=${by}&window=${encodeURIComponent(window)}`);

const DIMENSIONS: { id: ScorecardDimension; label: string }[] = [
  { id: "principal", label: "principal (agent / human)" },
  { id: "rule", label: "rule (the winning [[policy.rules]] entry)" },
  { id: "scope", label: "scope (the model decided on)" },
];

const WINDOWS = ["all", "24h", "7d", "30d", "90d"];

function percent(rate: number): string {
  return `${(rate * 100).toFixed(1)}%`;
}

/**
 * The trust scorecard, `GET /api/v1/audit/scorecard`: acceptance, denial
 * and require-review rates per group over the policy ledger. A judgment
 * aid wired to no automatic change. Metrics the ledger cannot support are
 * listed as "not recorded, because …", never computed.
 */
export function ScorecardScreen({ load = defaultScorecardLoader, now }: { load?: ScorecardLoader; now?: number }) {
  const [by, setBy] = useState<ScorecardDimension>("principal");
  const [window, setWindow] = useState("all");
  const loader = useCallback(() => load(by, window), [load, by, window]);
  const scorecard = useResource(loader, [loader]);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-xs text-zinc-600 dark:text-zinc-300" htmlFor="scorecard-by">
          Group by
        </label>
        <select
          id="scorecard-by"
          value={by}
          onChange={(event) => setBy(event.target.value as ScorecardDimension)}
          className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900"
        >
          {DIMENSIONS.map((dimension) => (
            <option key={dimension.id} value={dimension.id}>
              {dimension.label}
            </option>
          ))}
        </select>
        <label className="text-xs text-zinc-600 dark:text-zinc-300" htmlFor="scorecard-window">
          Window
        </label>
        <select
          id="scorecard-window"
          value={window}
          onChange={(event) => setWindow(event.target.value)}
          className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900"
        >
          {WINDOWS.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
        <button
          type="button"
          onClick={scorecard.reload}
          className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
        >
          Refresh
        </button>
        <code className="text-[11px] text-zinc-500 dark:text-zinc-400">
          GET /api/v1/audit/scorecard?by={by}&window={window}
        </code>
      </div>
      {scorecard.kind === "loading" && <p className="text-sm text-zinc-500">Loading the scorecard…</p>}
      {scorecard.kind === "refused" && (
        <StatusCard
          label={`refused (${scorecard.error.status})`}
          value={scorecard.error.envelope.code}
          tone="risk"
          sub={scorecard.error.envelope.remediation_hint ?? scorecard.error.envelope.message}
        />
      )}
      {scorecard.kind === "unreachable" && (
        <StatusCard label="engine" value="unreachable" tone="risk" sub={scorecard.message} />
      )}
      {scorecard.kind === "ready" && <ScorecardBody scorecard={scorecard.value} now={now} />}
    </div>
  );
}

function ScorecardBody({ scorecard, now }: { scorecard: AuditScorecardOutput; now?: number }) {
  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        Window <code>{scorecard.window}</code>
        {scorecard.window_start ? ` from ${formatInstant(scorecard.window_start, now)}` : ", all of recorded history"},
        grouped by <code>{scorecard.by}</code>. This informs a person; nothing here changes a policy.
      </p>
      <SectionCard
        title="Decisions by group"
        availability={scorecard.availability}
        note={scorecard.note}
        emptyLine="no decision falls in the window"
        summary={`${scorecard.total_decisions} decision(s)`}
      >
        <Rows
          ariaLabel="Scorecard groups"
          columns={["group", "total", "allow", "require review", "deny", "acceptance", "review rate", "denial"]}
          rows={scorecard.groups.map((group) => [
            group.key,
            group.total,
            group.allow,
            group.require_review,
            group.deny,
            percent(group.acceptance_rate),
            percent(group.review_rate),
            percent(group.denial_rate),
          ])}
        />
      </SectionCard>
      <div className="grid gap-3 sm:grid-cols-2">
        <StatusCard
          label="verify-after pass rate"
          value={scorecard.verify_after ? percent(scorecard.verify_after.pass_rate) : NOT_RECORDED}
          tone={
            scorecard.verify_after
              ? scorecard.verify_after.failed > 0
                ? "warn"
                : "ok"
              : "muted"
          }
          sub={
            scorecard.verify_after
              ? `${scorecard.verify_after.passed} passed, ${scorecard.verify_after.failed} failed of ${scorecard.verify_after.total}`
              : "no verification row falls in the window"
          }
        />
        <StatusCard
          label="metrics the ledger cannot support"
          value={scorecard.unavailable_metrics.length}
          tone={scorecard.unavailable_metrics.length > 0 ? "pending" : "ok"}
          sub={orNotRecorded(scorecard.note)}
        />
      </div>
      {scorecard.unavailable_metrics.length > 0 && (
        <ul className="space-y-1 text-xs text-zinc-700 dark:text-zinc-300" aria-label="Unavailable metrics">
          {scorecard.unavailable_metrics.map((metric) => (
            <li key={metric.metric}>
              <span className="font-medium">{metric.metric}</span>: {NOT_RECORDED}, because {metric.note}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
