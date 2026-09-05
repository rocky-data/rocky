import { useCallback, useState, type FormEvent } from "react";
import type { AuditForOutput } from "@rocky-types/audit_for";
import { apiGet } from "../api";
import { EmptyState, StatusCard } from "../components";
import { useResource } from "../estate/useResource";
import { formatInstant, orNotRecorded } from "../format";
import { custodyPath, navigateTo } from "../router";
import { CustodyLink } from "./links";
import { Rows, SectionCard } from "./SectionCard";

export type CustodyLoader = (subject: string) => Promise<AuditForOutput>;

export const defaultCustodyLoader: CustodyLoader = (subject) =>
  apiGet<AuditForOutput>(`custody/${encodeURIComponent(subject)}`);

/** The route's own bound on a subject (U1-P3); refused here before a request. */
export const MAX_SUBJECT_BYTES = 512;

/**
 * One subject's custody chain, `GET /api/v1/custody/{subject}`: a model
 * name, a run id, a plan id or a custody id such as `freeze:global`. The
 * five links render by their availability; a subject nothing references
 * says so in one line.
 */
export function CustodyScreen({
  subject,
  load = defaultCustodyLoader,
  now,
}: {
  /** The subject from the address, or `null` for the empty box. */
  subject: string | null;
  load?: CustodyLoader;
  now?: number;
}) {
  const [draft, setDraft] = useState(subject ?? "");
  const tooLong = new TextEncoder().encode(draft).length > MAX_SUBJECT_BYTES;

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const trimmed = draft.trim();
    if (trimmed === "" || tooLong) return;
    navigateTo(custodyPath(trimmed));
  };

  return (
    <div className="space-y-4">
      <form onSubmit={submit} className="flex flex-wrap items-center gap-3">
        <label className="text-xs text-zinc-600 dark:text-zinc-300" htmlFor="custody-subject">
          Subject
        </label>
        <input
          id="custody-subject"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          placeholder="a model name, a run id, a plan id, or freeze:global"
          className="w-96 max-w-full rounded border border-zinc-300 bg-white px-2 py-1 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-900"
        />
        <button
          type="submit"
          disabled={tooLong || draft.trim() === ""}
          className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
        >
          Trace
        </button>
        {tooLong && (
          <span className="text-xs text-red-700 dark:text-red-400">
            a subject is at most {MAX_SUBJECT_BYTES} bytes
          </span>
        )}
        <code className="text-[11px] text-zinc-500 dark:text-zinc-400">GET /api/v1/custody/{"{subject}"}</code>
      </form>
      {subject === null ? (
        <EmptyState
          title="No subject yet"
          detail="Type one above, or follow a plan or run id from the brief or the audit ledger."
        />
      ) : (
        <CustodyChain subject={subject} load={load} now={now} />
      )}
    </div>
  );
}

function CustodyChain({ subject, load, now }: { subject: string; load: CustodyLoader; now?: number }) {
  const loader = useCallback(() => load(subject), [load, subject]);
  const chain = useResource(loader, [loader]);
  switch (chain.kind) {
    case "loading":
      return <p className="text-sm text-zinc-500">Tracing {subject}…</p>;
    case "refused":
      return (
        <StatusCard
          label={`refused (${chain.error.status})`}
          value={chain.error.envelope.code}
          tone="risk"
          sub={chain.error.envelope.remediation_hint ?? chain.error.envelope.message}
        />
      );
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={chain.message} />;
    case "ready":
      return <ChainBody chain={chain.value} now={now} />;
  }
}

function ChainBody({ chain, now }: { chain: AuditForOutput; now?: number }) {
  const { decisions, plan, runs, verify_after: verify, blast_radius: blast } = chain;
  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        <span className="font-mono text-zinc-900 dark:text-zinc-100">{chain.subject}</span> resolved as a{" "}
        <code>{chain.subject_kind}</code>
        {chain.resolved ? "." : ": nothing in the ledger, the run history, the plan files or the graph references it."}
      </p>

      <SectionCard
        title="Decisions"
        availability={decisions.availability}
        note={decisions.note}
        emptyLine="no decision references this subject"
        summary={`${decisions.total} row(s)`}
      >
        <Rows
          ariaLabel="Decisions about the subject"
          columns={["when", "principal", "capability", "model", "effect", "rule", "plan", "reason"]}
          rows={decisions.entries.map((entry) => [
            formatInstant(entry.timestamp, now),
            entry.principal,
            entry.capability,
            entry.model,
            entry.effect,
            entry.rule_id === null || entry.rule_id === undefined ? "default" : `rule ${entry.rule_id}`,
            <CustodyLink key={entry.plan_id} subject={entry.plan_id} />,
            entry.reason,
          ])}
        />
      </SectionCard>

      <SectionCard
        title="Plan"
        availability={plan.availability}
        note={plan.note}
        emptyLine="no plan governs this subject"
        summary={`${plan.changes.length} change(s)`}
      >
        <div className="space-y-2 text-xs text-zinc-900 dark:text-zinc-100">
          <p>
            Plan {plan.plan_id ? <CustodyLink subject={plan.plan_id} /> : orNotRecorded(null)}, kind{" "}
            {orNotRecorded(plan.kind)}, principal {orNotRecorded(plan.principal)}, diff{" "}
            {plan.diff_available ? "available" : "not available"}.
          </p>
          <Rows
            ariaLabel="Plan changes"
            columns={["model", "capability"]}
            rows={plan.changes.map((change) => [change.model, change.capability])}
          />
        </div>
      </SectionCard>

      <SectionCard
        title="Runs"
        availability={runs.availability}
        note={runs.note}
        emptyLine="no run materialized this subject"
        summary={`${runs.total} run(s)`}
      >
        <Rows
          ariaLabel="Runs that materialized the subject"
          columns={["run", "status", "started", "finished", "triggered by"]}
          rows={runs.runs.map((run) => [
            <CustodyLink key={run.run_id} subject={run.run_id} />,
            run.status,
            formatInstant(run.started_at, now),
            formatInstant(run.finished_at, now),
            orNotRecorded(run.triggering_identity),
          ])}
        />
      </SectionCard>

      <SectionCard
        title="Verification after apply"
        availability={verify.availability}
        note={verify.note}
        emptyLine="no verification row is recorded for this subject"
        summary={`${verify.total} row(s)`}
      >
        <Rows
          ariaLabel="Verification rows"
          columns={["when", "plan", "passed", "checks", "reason"]}
          rows={verify.entries.map((entry) => [
            formatInstant(entry.timestamp, now),
            <CustodyLink key={entry.plan_id} subject={entry.plan_id} />,
            entry.passed ? "yes" : "no",
            entry.checks.join(", ") || "none",
            entry.reason,
          ])}
        />
      </SectionCard>

      <SectionCard
        title="Blast radius"
        availability={blast.availability}
        note={blast.note}
        emptyLine="nothing sits downstream"
        summary={`${blast.total} downstream model(s)`}
      >
        <div className="space-y-1 text-xs text-zinc-900 dark:text-zinc-100">
          <p>Model: {orNotRecorded(blast.model)}</p>
          <p>Direct: {blast.direct.length === 0 ? "none" : blast.direct.join(", ")}</p>
          <p>Transitive: {blast.transitive.length === 0 ? "none" : blast.transitive.join(", ")}</p>
        </div>
      </SectionCard>
    </div>
  );
}
