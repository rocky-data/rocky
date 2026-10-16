import { useState } from "react";
import type { JobStatus } from "@rocky-types/job_status";
import {
  ConfirmedWrite,
  JobLine,
  WriteButton,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobClient,
} from "../operator";

const INPUT =
  "h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900";

const PLAN_ID = /^[0-9a-f]{64}$/;

/** What a compact plan job wrote, read from its `rocky compact` result. */
export interface CompactPlanSummary {
  /** The plan to apply, or `null` when the plan was not saved. */
  planId: string | null;
  /** Every table the plan compacts. */
  tables: string[];
  statementCount: number;
  targetSizeMb: number | null;
  /** The SQL of each statement, as the plan lists them. */
  statements: string[];
}

/**
 * Read a succeeded `rocky compact` result. A single-table plan names its table
 * in `model`; a catalog plan keys its tables in `tables`. The plan id is
 * absent when the command could not save the plan, and then there is nothing
 * to apply: the page says so rather than offering a button.
 */
export function compactPlanSummary(job: JobStatus): CompactPlanSummary {
  const result = (job.result ?? {}) as Record<string, unknown>;
  const planId = typeof result.plan_id === "string" && PLAN_ID.test(result.plan_id) ? result.plan_id : null;
  const byTable =
    result.tables !== null && typeof result.tables === "object" && !Array.isArray(result.tables)
      ? Object.keys(result.tables as Record<string, unknown>).sort()
      : [];
  const tables = byTable.length > 0 ? byTable : typeof result.model === "string" && result.model !== "" ? [result.model] : [];
  const rawStatements = Array.isArray(result.statements) ? (result.statements as unknown[]) : [];
  const statements = rawStatements.flatMap((entry) => {
    const sql = entry !== null && typeof entry === "object" ? (entry as Record<string, unknown>).sql : undefined;
    return typeof sql === "string" ? [sql] : [];
  });
  return {
    planId,
    tables,
    statementCount: statements.length,
    targetSizeMb: typeof result.target_size_mb === "number" ? result.target_size_mb : null,
    statements,
  };
}

type Scope = "table" | "catalog";

function plural(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`;
}

/**
 * Compact tables from the browser, in two steps: write a plan, then apply
 * that exact plan.
 *
 * **Plan compact** runs `rocky compact <table>` (or `--catalog <name>`). It
 * reads the config, writes a plan file and runs no statement. The page lists
 * the tables and the statements the plan holds. **Compact** then confirms by
 * naming every table, and applies the plan by its id through the existing
 * apply; it is not planned again. Apply runs `OPTIMIZE` and `VACUUM` on the
 * warehouse with this server's credentials (Databricks only).
 *
 * The command's `--dry-run` is not offered. It prints "no statements
 * executed" but still writes a plan that apply can run.
 */
export function CompactControls({
  jobs = defaultJobClient,
  onDone,
}: {
  jobs?: JobClient;
  onDone?: () => void;
}) {
  const plan = useJob("compact", jobs);
  const apply = useJob("apply", jobs, onDone);
  const [scope, setScope] = useState<Scope>("table");
  const [table, setTable] = useState("");
  const [catalog, setCatalog] = useState("");
  // What the plan job was started for, so a plan shown beside a different
  // input is dropped rather than applied under the wrong name.
  const [plannedFor, setPlannedFor] = useState<string | null>(null);
  const [applyingPlan, setApplyingPlan] = useState<string | null>(null);

  const target = (scope === "table" ? table : catalog).trim();
  const key = `${scope}:${target}`;
  const planBusy = jobBusy(plan.view);
  const applyBusy = jobBusy(apply.view);
  const shown = plannedFor === key;
  const summary =
    shown && plan.view.kind === "done" && plan.view.job.state === "succeeded"
      ? compactPlanSummary(plan.view.job)
      : null;
  const applyShown = summary !== null && summary.planId !== null && applyingPlan === summary.planId;
  const applied = applyShown && apply.view.kind === "done" && apply.view.job.state === "succeeded";
  const blocked = target === "" ? (scope === "table" ? "Type the full table name." : "Type a catalog name.") : undefined;

  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Compacts tables in two steps. First a plan is written: it lists each table and the statements it would
        run, and runs none. Then you apply that exact plan, which runs <code>OPTIMIZE</code> (rewrites small
        files) and <code>VACUUM</code> (removes stale data files) on the warehouse. This works on Databricks
        only. Archiving is not offered here: it deletes rows, so it stays a terminal step.
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <fieldset className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          <legend className="sr-only">What to compact</legend>
          <label className="flex items-center gap-1.5">
            <input
              type="radio"
              name="compact-scope"
              checked={scope === "table"}
              onChange={() => setScope("table")}
              disabled={planBusy || applyBusy}
            />
            One table
          </label>
          <label className="flex items-center gap-1.5">
            <input
              type="radio"
              name="compact-scope"
              checked={scope === "catalog"}
              onChange={() => setScope("catalog")}
              disabled={planBusy || applyBusy}
            />
            Every managed table in a catalog
          </label>
        </fieldset>
        {scope === "table" ? (
          <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
            Table
            <input
              value={table}
              onChange={(event) => setTable(event.target.value)}
              placeholder="catalog.schema.table"
              spellCheck={false}
              autoComplete="off"
              className={`${INPUT} w-72 font-mono`}
            />
          </label>
        ) : (
          <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
            Catalog
            <input
              value={catalog}
              onChange={(event) => setCatalog(event.target.value)}
              placeholder="catalog"
              spellCheck={false}
              autoComplete="off"
              className={`${INPUT} w-72 font-mono`}
            />
          </label>
        )}
        <WriteButton
          label="Plan compact"
          busy={planBusy}
          disabledReason={blocked}
          onClick={() => {
            setPlannedFor(key);
            plan.start(scope === "table" ? { table: target } : { catalog: target });
          }}
        />
      </div>
      {shown && <JobLine label="Plan compact" view={plan.view} cancel={plan.cancel} />}
      {summary !== null && (
        <div className="space-y-2">
          <p className="text-xs text-emerald-700 dark:text-emerald-300">
            Plan ready: {plural(summary.statementCount, "statement", "statements")} on{" "}
            {plural(summary.tables.length, "table", "tables")}
            {summary.targetSizeMb === null ? "" : `, target file size ${summary.targetSizeMb} MB`}. Nothing has run.
          </p>
          <ul aria-label="Tables this plan compacts" className="list-disc space-y-0.5 pl-5">
            {summary.tables.map((name) => (
              <li key={name} className="break-all font-mono text-xs text-zinc-700 dark:text-zinc-200">
                {name}
              </li>
            ))}
          </ul>
          {summary.statements.length > 0 && (
            <details className="text-xs text-zinc-600 dark:text-zinc-300">
              <summary className="cursor-pointer">The statements ({summary.statements.length})</summary>
              <pre className="mt-1 max-h-64 overflow-auto rounded-md bg-zinc-50 p-2 font-mono text-xs whitespace-pre-wrap dark:bg-zinc-950">
                {summary.statements.join(";\n")}
              </pre>
            </details>
          )}
          {summary.planId === null ? (
            <p role="alert" className="text-xs text-amber-800 dark:text-amber-300">
              The command could not save this plan, so it cannot be applied from here.
            </p>
          ) : summary.tables.length === 0 ? (
            <p className="text-xs text-amber-800 dark:text-amber-300">
              The plan lists no tables, so there is nothing to apply.
            </p>
          ) : applied ? null : (
            <ConfirmedWrite
              label="Compact"
              question={`Run OPTIMIZE and VACUUM on ${plural(summary.tables.length, "table", "tables")}: ${summary.tables.join(", ")}? This rewrites small files and removes stale data files in the warehouse, with this server's credentials. Rocky cannot undo it.`}
              confirmLabel="Yes, compact these tables"
              busy={applyBusy}
              onConfirm={() => {
                setApplyingPlan(summary.planId);
                apply.start({ plan_id: summary.planId });
              }}
            />
          )}
          {summary.planId !== null && (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              Applies plan <code className="font-mono">{summary.planId}</code> as written; it is not planned again.
            </p>
          )}
          {applyShown && <JobLine label="Compact" view={apply.view} cancel={apply.cancel} />}
        </div>
      )}
    </div>
  );
}
