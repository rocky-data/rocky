import { useCallback, useState } from "react";
import type { AuditDecisionEntry, AuditOutput, PolicyEffect, PrincipalIdSource } from "@rocky-types/audit";
import { apiGet } from "../api";
import { EmptyState, ScreenHeader, StatusCard, TABLE } from "../components";
import { useResource } from "../estate/useResource";
import { formatInstant } from "../format";
import { auditPath, navigateTo, pathForLane } from "../router";
import { CustodyLink } from "./links";

export type AgentsLoader = (days: number) => Promise<AuditOutput>;

export const defaultAgentsLoader: AgentsLoader = (days) => apiGet<AuditOutput>(`audit?since=${days}d`);

const WINDOWS = [
  { days: 1, label: "1 day" },
  { days: 7, label: "7 days" },
  { days: 30, label: "30 days" },
];

/** A principal id's source in words. No default arm: a new variant is a type error. */
function sourceWords(source: PrincipalIdSource): string {
  switch (source) {
    case "flag":
      return "--principal-id flag";
    case "env":
      return "environment variable";
    case "mcp_profile":
      return "MCP profile";
    case "default":
      return "default";
  }
}

const DETAIL_ID = "agent-detail";
const NO_SOURCE = "No source recorded";
const NO_CHANNEL = "The audit log does not record an agent's channel yet";

const EFFECT_PILL: Record<PolicyEffect, string> = {
  allow: "bg-ok-soft text-ok-ink",
  require_review: "bg-warn-soft text-warn-ink",
  deny: "bg-block-soft text-block-ink",
};

interface Group {
  /** `null` is the group of rows written before ids existed. */
  id: string | null;
  key: string;
  rows: AuditDecisionEntry[]; // newest first
  /** Distinct change ids; a row with no plan id, and each draft row, counts as one request. */
  requests: number;
  allow: number;
  review: number;
  deny: number;
}

/** A drift change that `rocky run` applied on its own files its decision under `autoapply:<run id>`. */
const AUTO_APPLY_PREFIX = "autoapply:";

/** Past tense: the row records what the policy decided, not what is pending. */
const OUTCOME_WORDS: Record<PolicyEffect, string> = {
  allow: "Allowed",
  require_review: "Needed approval",
  deny: "Blocked",
};

const EFFECT_RANK: Record<PolicyEffect, number> = { allow: 0, require_review: 1, deny: 2 };

/**
 * The MCP draft gates write one plan id per model, such as `draft:<model>` or
 * `draft-check:<model>`. The id names a model, not a change, so each such row is its own request.
 */
const DRAFT_ID = /^draft(-[a-z]+)?:/;

/** The rows of one change: usually one decision per model it touches. */
interface Request {
  planId: string | null;
  rows: AuditDecisionEntry[];
  newest: string;
  worst: PolicyEffect;
}

function requestsOf(rows: AuditDecisionEntry[]): Request[] {
  const byPlan = new Map<string, Request>();
  const out: Request[] = [];
  for (const row of rows) {
    const planId = row.plan_id ? row.plan_id : null;
    const merges = planId !== null && !DRAFT_ID.test(planId);
    let request = merges ? byPlan.get(planId) : undefined;
    if (!request) {
      request = { planId, rows: [], newest: row.timestamp, worst: row.effect };
      out.push(request);
      if (merges) byPlan.set(planId, request);
    }
    request.rows.push(row);
    if (Date.parse(row.timestamp) > Date.parse(request.newest)) request.newest = row.timestamp;
    if (EFFECT_RANK[row.effect] > EFFECT_RANK[request.worst]) request.worst = row.effect;
  }
  return out.sort((a, b) => Date.parse(b.newest) - Date.parse(a.newest));
}

const UNRECORDED = "unrecorded";
const UNNAMED = "unnamed";

function idLabel(group: Group): string {
  return group.id ?? UNRECORDED;
}

function isNameless(group: Group): boolean {
  return group.id === null || group.id === UNNAMED;
}

function buildGroups(rows: AuditDecisionEntry[]): Group[] {
  const byId = new Map<string | null, AuditDecisionEntry[]>();
  for (const row of rows) {
    const id = row.principal_id ?? null;
    const list = byId.get(id);
    if (list) list.push(row);
    else byId.set(id, [row]);
  }
  const groups: Group[] = [];
  for (const [id, list] of byId) {
    const sorted = [...list].sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp));
    groups.push({
      id,
      key: id === null ? "\u0000unrecorded" : `id:${id}`,
      rows: sorted,
      requests: requestsOf(sorted).length,
      allow: sorted.filter((r) => r.effect === "allow").length,
      review: sorted.filter((r) => r.effect === "require_review").length,
      deny: sorted.filter((r) => r.effect === "deny").length,
    });
  }
  return groups.sort(
    (a, b) => b.requests - a.requests || b.rows.length - a.rows.length || idLabel(a).localeCompare(idLabel(b)),
  );
}

function namedBy(group: Group): string | null {
  if (group.id === UNNAMED) return "nobody named it";
  const sources: string[] = [];
  for (const row of group.rows) {
    if (!row.principal_id_source) continue;
    const words = sourceWords(row.principal_id_source);
    if (!sources.includes(words)) sources.push(words);
  }
  return sources.length > 0 ? sources.join(", ") : null;
}

function plural(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`;
}

/**
 * The agents tab, from `GET /api/v1/audit?since=<N>d`: the requests Rocky's policy
 * gated as an agent. A person can appear here: backfill, gc and AI-authored plans
 * are always gated as agents. Only an `evaluation` row of an `agent` principal is
 * a request with a policy verdict. A name is a self-asserted label.
 */
export function AgentsScreen({ load = defaultAgentsLoader, now }: { load?: AgentsLoader; now?: number }) {
  const [days, setDays] = useState(7);
  const loader = useCallback(() => load(days), [load, days]);
  const audit = useResource(loader, [loader]);

  return (
    <div className="space-y-4">
      <ScreenHeader
        title="Agents"
        detail="Requests that Rocky's policy gated as an agent, from the audit log. A person can appear here: backfill, gc and AI-authored plans are always gated as agents."
      >
        <div className="flex items-center gap-2">
          <label className="text-sm text-ink-2" htmlFor="agents-window">
            Window
          </label>
          <select
            id="agents-window"
            value={days}
            onChange={(event) => setDays(Number(event.target.value))}
            className="h-9 rounded-md border border-line bg-surface px-2 text-sm text-ink"
          >
            {WINDOWS.map((option) => (
              <option key={option.days} value={option.days}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
      </ScreenHeader>
      <div className="rounded-md border border-line bg-surface-2 px-4 py-3 text-sm text-ink-2">
        <strong className="font-semibold text-ink">Names are self-asserted.</strong> An agent names itself with{" "}
        <code>ROCKY_PRINCIPAL_ID</code>, a flag or its MCP profile. Rocky records the name but does not verify it
        yet. Treat a name as a label, not a proof. Many agents can share one name: default MCP agents all use{" "}
        <code>mcp-default</code>.
      </div>
      {audit.kind === "loading" && <p className="text-sm text-muted">Loading agents…</p>}
      {audit.kind === "refused" && (
        <StatusCard
          label={`refused (${audit.error.status})`}
          value={audit.error.envelope.code}
          tone="risk"
          sub={audit.error.envelope.remediation_hint ?? audit.error.envelope.message}
        />
      )}
      {audit.kind === "unreachable" && (
        <StatusCard label="engine" value="unreachable" tone="risk" sub={audit.message} />
      )}
      {audit.kind === "ready" && <AgentsBody audit={audit.value} days={days} now={now} />}
    </div>
  );
}

function AgentsBody({ audit, days, now }: { audit: AuditOutput; days: number; now?: number }) {
  const agentRows = audit.decisions.filter((d) => d.principal === "agent" && d.kind === "evaluation");
  const requests = agentRows.filter((d) => !d.plan_id?.startsWith(AUTO_APPLY_PREFIX));
  const autoApplied = agentRows.length - requests.length;
  const groups = buildGroups(requests);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  if (groups.length === 0) {
    return (
      <EmptyState
        title={`No agent requests in the last ${days} ${days === 1 ? "day" : "days"}`}
        detail={
          autoApplied > 0
            ? `${plural(autoApplied, "decision", "decisions")} from rocky run's own drift fixes ${autoApplied === 1 ? "is" : "are"} left out.`
            : undefined
        }
      />
    );
  }
  const selected = groups.find((g) => g.key === selectedKey) ?? groups[0];
  const named = groups.filter((g) => !isNameless(g)).length;
  const allRequests = requestsOf(requests);
  const needed = allRequests.filter((r) => r.worst === "require_review").length;
  const nameless = groups.filter(isNameless).reduce((sum, g) => sum + g.rows.length, 0);

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Stat label="Distinct names" value={named} sub="self-asserted; unnamed and unrecorded not counted" />
        <Stat
          label="Agent requests"
          value={allRequests.length}
          sub={`${plural(requests.length, "gate decision", "gate decisions")}, one per model`}
        />
        <Stat label="Needed approval" value={needed} sub="requests whose worst outcome was needing approval" />
        <Stat label="Rows with no name" value={nameless} sub={'named "unnamed", or written before names'} />
      </div>
      <div className={TABLE.scroller}>
        <table className={TABLE.table} aria-label="Agents">
          <thead className={TABLE.head}>
            <tr>
              {["Agent", "Named by", "Channel", "Requests", "Outcome", "Last seen"].map((c) => (
                <th key={c} scope="col" className={TABLE.th}>
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {groups.map((group) => (
              <GroupRow
                key={group.key}
                group={group}
                selected={group.key === selected.key}
                onSelect={() => setSelectedKey(group.key)}
                now={now}
              />
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-muted">
        One request usually writes one decision per model it touches. Drift decisions <code>rocky run</code> made on
        its own, applied or refused, are recorded as agent decisions too, under a plan id that starts with{" "}
        <code>autoapply:</code>.
        {autoApplied > 0
          ? ` This page leaves ${plural(autoApplied, "such decision", "such decisions")} out.`
          : " This page leaves those out."}
      </p>
      <p className="text-xs text-muted">
        Decisions about the same change, including retries, count as one request; it shows the worst outcome. A
        change whose decisions carry two names counts under each name. Draft plan ids name a model, not a change,
        so each draft decision counts as its own request. An outcome is the policy decision at the time. A later
        approval is not shown here: see Trace.
      </p>
      <AgentDetail key={selected.key} group={selected} days={days} now={now} />
      <p className="text-xs text-muted">
        Name an agent: set <code>ROCKY_PRINCIPAL_ID=&lt;name&gt;</code> in its environment.
      </p>
    </div>
  );
}

function Stat({ label, value, sub }: { label: string; value: number; sub?: string }) {
  return (
    <div className="rounded-card border border-line bg-surface p-4">
      <div className="text-xs text-muted">{label}</div>
      <div className="mt-1 text-2xl font-semibold text-ink">{value}</div>
      {sub && <div className="mt-1 text-xs text-muted">{sub}</div>}
    </div>
  );
}

function GroupRow({
  group,
  selected,
  onSelect,
  now,
}: {
  group: Group;
  selected: boolean;
  onSelect: () => void;
  now?: number;
}) {
  const total = group.rows.length;
  const via = namedBy(group);
  const nameless = isNameless(group);
  return (
    <tr className={`${TABLE.row} ${selected ? "bg-surface-2" : ""}`}>
      <td className={TABLE.td}>
        <button
          type="button"
          onClick={onSelect}
          aria-current={selected ? "true" : undefined}
          aria-controls={DETAIL_ID}
          className={`font-mono text-sm hover:underline ${nameless ? "text-muted" : "text-ink"}`}
        >
          {idLabel(group)}
        </button>
      </td>
      <td className={TABLE.td}>
        {via === null ? (
          <span className="text-muted" title={NO_SOURCE}>
            -
          </span>
        ) : (
          <span className={nameless ? "text-warn-ink" : "text-ink-2"}>{via}</span>
        )}
      </td>
      <td className={TABLE.td}>
        <span className="text-muted" title={NO_CHANNEL}>
          -
        </span>
      </td>
      <td className={`${TABLE.td} tabular-nums`}>
        {group.requests}
        <span className="ml-2 text-xs text-muted">({plural(total, "decision", "decisions")})</span>
      </td>
      <td className={TABLE.td}>
        <div className="flex items-center gap-3">
          <div aria-hidden="true" className="flex h-2 w-24 overflow-hidden rounded-full bg-surface-2">
            <span className="bg-ok" style={{ width: `${(group.allow / total) * 100}%` }} />
            <span className="bg-warn" style={{ width: `${(group.review / total) * 100}%` }} />
            <span className="bg-block" style={{ width: `${(group.deny / total) * 100}%` }} />
          </div>
          <span className="text-xs text-ink-2">
            {group.allow} allowed · {group.review} needed approval · {group.deny} blocked decisions
          </span>
        </div>
      </td>
      <td className={`${TABLE.td} text-ink-2`}>{formatInstant(group.rows[0].timestamp, now)}</td>
    </tr>
  );
}

function tally(values: string[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1);
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

const RECENT_LIMIT = 5;
const LINK = "text-accent-ink underline-offset-2 hover:underline";

function AgentDetail({ group, days, now }: { group: Group; days: number; now?: number }) {
  const [showAll, setShowAll] = useState(false);
  const id = idLabel(group);
  const via = namedBy(group);
  // Verified only when the engine says so for every row. Always false today.
  const verified = group.rows.length > 0 && group.rows.every((r) => r.principal_id_verified === true);
  const naming =
    group.id === UNNAMED
      ? "nobody named it"
      : group.id === null
        ? "written before names were recorded"
        : `named by ${via ?? "no source recorded"}`;
  const capabilities = tally(group.rows.map((r) => r.capability));
  const models = [...new Set(group.rows.map((r) => r.model))].sort();
  const inboxHref = pathForLane("governor");
  const auditHref = auditPath();
  const requests = requestsOf(group.rows);
  const shown = showAll ? requests : requests.slice(0, RECENT_LIMIT);
  return (
    <section id={DETAIL_ID} aria-label={`Agent ${id}`} className="space-y-4 rounded-card border border-line bg-surface p-5">
      <header>
        <h3 className="font-mono text-base font-semibold text-ink">{id}</h3>
        <p className="mt-1 text-sm text-muted">
          {naming} · {verified ? "verified" : "not verified"}
        </p>
        <p className="mt-2 text-sm">
          <a
            href={auditHref}
            onClick={(event) => {
              event.preventDefault();
              navigateTo(auditHref);
            }}
            className={LINK}
          >
            Audit log
          </a>
          <span className="text-muted">
            {" "}
            (on the command line: <code>{`rocky audit --actor ${idLabel(group)} --since ${days}d`}</code>)
          </span>
        </p>
      </header>
      <div className="grid gap-4 md:grid-cols-3">
        <div>
          <h4 className="text-sm font-semibold text-ink">What it asked to do</h4>
          <ul className="mt-1 space-y-0.5 text-sm text-ink-2" aria-label="Capabilities asked">
            {capabilities.map(([capability, count]) => (
              <li key={capability}>
                <span className="font-mono">{capability}</span> · {count}
              </li>
            ))}
          </ul>
          <ul className="mt-2 flex flex-wrap gap-1.5" aria-label="Models">
            {models.map((model) => (
              <li key={model} className="rounded-chip bg-agent-soft px-2 py-0.5 font-mono text-xs text-agent-ink">
                {model}
              </li>
            ))}
          </ul>
        </div>
        <div>
          <h4 className="text-sm font-semibold text-ink">Rules that decide for it</h4>
          <p className="mt-1 text-sm text-ink-2">Rules match agents as a class, not by name.</p>
        </div>
        <div>
          <h4 className="text-sm font-semibold text-ink">Limits</h4>
          <p className="mt-1 text-sm text-ink-2">
            A change freeze stops every agent, not one. Start or lift one from the{" "}
            <a
              href={inboxHref}
              onClick={(event) => {
                event.preventDefault();
                navigateTo(inboxHref);
              }}
              className={LINK}
            >
              Inbox
            </a>
            .
          </p>
        </div>
      </div>
      <div>
        <h4 className="text-sm font-semibold text-ink">Recent requests</h4>
        <ul className="mt-1 divide-y divide-divider" aria-label="Recent requests">
          {shown.map((request, index) => {
            const caps = [...new Set(request.rows.map((r) => r.capability))].sort();
            const models = [...new Set(request.rows.map((r) => r.model))].sort();
            return (
              <li
                key={`${request.planId ?? "none"}-${index}`}
                className="flex flex-wrap items-center gap-x-4 gap-y-1 py-2 text-sm"
              >
                <span className="text-ink-2">{formatInstant(request.newest, now)}</span>
                <span className="text-ink">
                  <span className="font-mono">{caps.join(", ")}</span> on{" "}
                  <span className="font-mono">{models.join(", ")}</span>
                </span>
                <span className={`rounded-chip px-2 py-0.5 text-xs font-medium ${EFFECT_PILL[request.worst]}`}>
                  {OUTCOME_WORDS[request.worst]}
                </span>
                {request.planId !== null && <CustodyLink subject={request.planId} />}
              </li>
            );
          })}
        </ul>
        {requests.length > RECENT_LIMIT && !showAll && (
          <button type="button" onClick={() => setShowAll(true)} className={`mt-2 text-sm ${LINK}`}>
            Show all {requests.length}
          </button>
        )}
      </div>
    </section>
  );
}
