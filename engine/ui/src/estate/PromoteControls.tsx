import { useCallback, useState } from "react";
import type { BranchListOutput } from "@rocky-types/branch_list";
import type { JobStatus } from "@rocky-types/job_status";
import { apiGet } from "../api";
import {
  ConfirmedWrite,
  JobLine,
  WriteButton,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobClient,
} from "../operator";
import { ResourceState } from "../review/ResourceState";
import { useResource } from "./useResource";

const SELECT =
  "h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900";

/** The config key that turns promote on. Said as it is written in `rocky.toml`. */
export const PROMOTE_CONFIG_KEY = "[branch.approval] required = true";

/** Where the branch list comes from. Tests hand in a fake. */
export type LoadBranches = () => Promise<BranchListOutput>;

export const defaultLoadBranches: LoadBranches = () => apiGet<BranchListOutput>("branches");

const PLAN_ID = /^[0-9a-f]{64}$/;

/** A finished promote plan job: the plan to apply and every table it replaces. */
export interface PromotePlanSummary {
  planId: string;
  /** Every production table the plan would replace, as the plan lists them. */
  tables: string[];
}

/**
 * Read a succeeded `rocky plan promote` result. The plan lists one statement
 * per production table; only the table name is read, never the SQL. `null`
 * when the result carries no usable plan id, which the page reports instead of
 * offering an apply for a plan it cannot name.
 */
export function promotePlanSummary(job: JobStatus): PromotePlanSummary | null {
  const result = (job.result ?? {}) as Record<string, unknown>;
  const planId = result.plan_id;
  if (typeof planId !== "string" || !PLAN_ID.test(planId)) return null;
  const statements = Array.isArray(result.statements) ? (result.statements as unknown[]) : [];
  const tables = statements
    .map((entry) =>
      entry !== null && typeof entry === "object" ? (entry as Record<string, unknown>).target : undefined,
    )
    .filter((target): target is string => typeof target === "string" && target !== "");
  return { planId, tables };
}

/** The per-table outcome of a finished apply, read from `result.targets`. */
function appliedTargets(job: JobStatus): { target: string; succeeded: boolean }[] {
  const result = (job.result ?? {}) as Record<string, unknown>;
  const targets = Array.isArray(result.targets) ? (result.targets as unknown[]) : [];
  return targets.flatMap((entry) => {
    if (entry === null || typeof entry !== "object") return [];
    const { target, succeeded } = entry as Record<string, unknown>;
    return typeof target === "string" && target !== "" ? [{ target, succeeded: succeeded === true }] : [];
  });
}

/**
 * Promote a branch to production from the browser, only when the project
 * requires a branch approval.
 *
 * `approvalRequired` is `[branch.approval] required = true`, read by the
 * server per request (`GET /api/v1/project`). When it is `true` the panel
 * offers promote; the server checks the same key again and refuses a promote
 * job, and an apply of a promote plan, when it is off. When it is `false` or
 * unknown the panel says why it is absent and which key turns it on.
 */
export function PromoteControls({
  approvalRequired,
  jobs = defaultJobClient,
  loadBranches = defaultLoadBranches,
  onDone,
}: {
  approvalRequired: boolean | null | undefined;
  jobs?: JobClient;
  loadBranches?: LoadBranches;
  onDone?: () => void;
}) {
  if (approvalRequired === undefined) {
    return <p className="text-sm text-zinc-500">Loading…</p>;
  }
  if (approvalRequired !== true) {
    return (
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        {approvalRequired === false
          ? "Promote is not offered here. It writes production tables, so this page offers it only when rocky.toml sets "
          : "Promote is not offered here: the project config could not be read. It is offered only when rocky.toml sets "}
        <code>{PROMOTE_CONFIG_KEY}</code>
        {approvalRequired === false
          ? ", which makes the engine ask for a signed branch approval first. Set it, or promote in a terminal with rocky branch promote."
          : "."}
      </p>
    );
  }
  return <PromotePanel jobs={jobs} loadBranches={loadBranches} onDone={onDone} />;
}

function PromotePanel({
  jobs,
  loadBranches,
  onDone,
}: {
  jobs: JobClient;
  loadBranches: LoadBranches;
  onDone?: () => void;
}) {
  const branches = useResource(useCallback(() => loadBranches(), [loadBranches]), [loadBranches]);
  const plan = useJob("promote", jobs);
  const apply = useJob("apply", jobs, onDone);
  const [chosen, setChosen] = useState("");
  // The branch the plan job was started for. A plan shown beside a different
  // selection would be applied under the wrong name.
  const [plannedFor, setPlannedFor] = useState<string | null>(null);
  // The plan the apply job was started for, for the same reason.
  const [applyingPlan, setApplyingPlan] = useState<string | null>(null);

  if (branches.kind !== "ready") {
    return <ResourceState resource={branches} loadingLine="reading the branches…" />;
  }
  const names = branches.value.branches.map((branch) => branch.name);
  if (names.length === 0) {
    return (
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        There are no branches to promote. Create one with Preview above, or <code>rocky branch create</code>.
      </p>
    );
  }
  const branch = names.includes(chosen) ? chosen : names[0];
  const planBusy = jobBusy(plan.view);
  const applyBusy = jobBusy(apply.view);
  const shown = plannedFor === branch;
  const summary =
    shown && plan.view.kind === "done" && plan.view.job.state === "succeeded"
      ? promotePlanSummary(plan.view.job)
      : null;
  const applyShown = summary !== null && applyingPlan === summary.planId;
  const applied = applyShown && apply.view.kind === "done" && apply.view.job.state === "succeeded";
  const planFinishedWithoutPlan =
    shown && plan.view.kind === "done" && plan.view.job.state === "succeeded" && summary === null;

  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Promotes a branch&apos;s tables to their production targets in two steps. First a plan is written: it
        checks the branch&apos;s signed approval and its breaking changes and lists every production table it
        would replace. Nothing in production changes yet. Then you apply that exact plan. The approval itself is
        signed in a terminal with <code>rocky branch approve</code>; this page does not sign it.
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Branch
          <select
            value={branch}
            onChange={(event) => setChosen(event.target.value)}
            disabled={planBusy || applyBusy}
            className={`${SELECT} w-64`}
          >
            {names.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <WriteButton
          label="Plan promote"
          busy={planBusy}
          onClick={() => {
            setPlannedFor(branch);
            plan.start({ name: branch });
          }}
        />
      </div>
      {shown && <JobLine label="Plan promote" view={plan.view} cancel={plan.cancel} />}
      {planFinishedWithoutPlan && (
        <p role="alert" className="text-xs text-red-700 dark:text-red-300">
          Plan promote: the job succeeded but returned no plan id, so there is nothing to apply.
        </p>
      )}
      {summary !== null && (
        <div className="space-y-2">
          <p className="text-xs text-emerald-700 dark:text-emerald-300">
            Plan ready for branch {branch}: it replaces {summary.tables.length} production table
            {summary.tables.length === 1 ? "" : "s"}.
          </p>
          <ul aria-label="Production tables this plan replaces" className="list-disc space-y-0.5 pl-5">
            {summary.tables.map((table) => (
              <li key={table} className="break-all font-mono text-xs text-zinc-700 dark:text-zinc-200">
                {table}
              </li>
            ))}
          </ul>
          {summary.tables.length === 0 ? (
            <p className="text-xs text-amber-800 dark:text-amber-300">
              The plan lists no tables, so there is nothing to apply.
            </p>
          ) : applied ? null : (
            <ConfirmedWrite
              label="Promote"
              question={`Replace ${summary.tables.length} production table${
                summary.tables.length === 1 ? "" : "s"
              } with the contents of branch ${branch}: ${summary.tables.join(", ")}? This writes the warehouse with this server's credentials. Rocky cannot undo it.`}
              confirmLabel="Yes, replace these tables"
              busy={applyBusy}
              onConfirm={() => {
                setApplyingPlan(summary.planId);
                apply.start({ plan_id: summary.planId });
              }}
            />
          )}
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            Applies plan <code className="font-mono">{summary.planId}</code> as written; it is not planned again.
          </p>
          {applyShown && <JobLine label="Promote" view={apply.view} cancel={apply.cancel} />}
          {applyShown && apply.view.kind === "done" && appliedTargets(apply.view.job).length > 0 && (
            <ul aria-label="Promote result" className="list-disc space-y-0.5 pl-5">
              {appliedTargets(apply.view.job).map(({ target, succeeded }) => (
                <li
                  key={target}
                  className={`break-all font-mono text-xs ${
                    succeeded ? "text-zinc-700 dark:text-zinc-200" : "text-red-700 dark:text-red-300"
                  }`}
                >
                  {target}: {succeeded ? "replaced" : "not replaced"}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
