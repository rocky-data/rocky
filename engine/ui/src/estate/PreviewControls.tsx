import { useState } from "react";
import type { JobStatus } from "@rocky-types/job_status";
import {
  ConfirmedWrite,
  JobLine,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobCancel,
  type JobClient,
  type JobView,
} from "../operator";

const INPUT =
  "h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900";

/** The most model names a preview result lists, and the most characters of each. */
const NAMES_MAX = 10;
const NAME_MAX_CHARS = 80;

/** What a succeeded `preview create` did, read from `result`. */
export interface PreviewSummary {
  branchSchema: string;
  changed: string[];
  copied: number;
  /** Copies that failed, by model name. */
  failed: string[];
  /** Models the command only planned, not copied. The server log says why. */
  planned: string[];
}

function names(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return (value as unknown[])
    .map((entry) =>
      entry !== null && typeof entry === "object" ? (entry as Record<string, unknown>).model_name : undefined,
    )
    .filter((name): name is string => typeof name === "string" && name !== "");
}

/**
 * Read the fields the page says something about. The result is the child's
 * own JSON: only the fixed `copy_strategy` words, model names and the branch
 * schema are used. The result carries no message text, and none is read.
 */
export function previewSummary(job: JobStatus): PreviewSummary {
  const result = (job.result ?? {}) as Record<string, unknown>;
  const copySet = Array.isArray(result.copy_set) ? (result.copy_set as unknown[]) : [];
  const withStrategy = (strategy: string) =>
    names(
      copySet.filter(
        (entry) =>
          entry !== null &&
          typeof entry === "object" &&
          (entry as Record<string, unknown>).copy_strategy === strategy,
      ),
    );
  return {
    branchSchema: typeof result.branch_schema === "string" ? result.branch_schema : "",
    changed: names(result.prune_set),
    copied: withStrategy("ctas").length,
    failed: withStrategy("failed"),
    planned: withStrategy("planning_only"),
  };
}

function nameList(all: string[]): string {
  const shown = all
    .slice(0, NAMES_MAX)
    .map((n) => (n.length <= NAME_MAX_CHARS ? n : `${n.slice(0, NAME_MAX_CHARS)}…`));
  return all.length > NAMES_MAX ? `${shown.join(", ")} and ${all.length - NAMES_MAX} more` : shown.join(", ");
}

/**
 * The job line for a preview. A succeeded job can still have copied nothing:
 * a failed copy is recorded per model and the command still exits 0, a model
 * can be planned but not copied, and a preview with no changed models copies
 * nothing. Each is shown as a warning, never as a bare "done". The changed models are said to be planned, not run.
 */
function PreviewJobLine({ view, cancel }: { view: JobView; cancel: JobCancel }) {
  if (view.kind !== "done" || view.job.state !== "succeeded") {
    return <JobLine label="Preview" view={view} cancel={cancel} />;
  }
  const s = previewSummary(view.job);
  const nothing = s.copied === 0 && s.changed.length === 0 && s.failed.length === 0 && s.planned.length === 0;
  const warn = nothing || s.failed.length > 0 || s.planned.length > 0;
  const headline = nothing
    ? "Preview: branch registered, but nothing was copied."
    : warn
      ? "Preview: branch registered, but not every model was copied."
      : "Preview: done.";
  const lines: string[] = [];
  if (s.branchSchema !== "") lines.push(`Warehouse schema: ${s.branchSchema}`);
  lines.push(`Copied from production: ${s.copied} model${s.copied === 1 ? "" : "s"}.`);
  if (s.failed.length > 0) lines.push(`Copy failed for: ${nameList(s.failed)}. See the server log.`);
  if (s.planned.length > 0) {
    lines.push(`Planned but not copied (the server log says why): ${nameList(s.planned)}.`);
  }
  lines.push(
    s.changed.length > 0
      ? `Changed and downstream, planned but not re-run: ${nameList(s.changed)}.`
      : nothing
        ? "No changed models, so nothing was copied."
        : "No changed models.",
  );
  return (
    <div className="space-y-1">
      <p
        role={warn ? "alert" : undefined}
        className={
          warn
            ? "text-xs font-medium text-amber-800 dark:text-amber-300"
            : "text-xs text-emerald-700 dark:text-emerald-300"
        }
      >
        {headline}
      </p>
      <ul aria-label="Preview result" className="list-disc space-y-0.5 pl-5">
        {lines.map((line, index) => (
          <li
            key={index}
            className={`break-words text-xs ${
              warn ? "text-amber-800 dark:text-amber-300" : "text-zinc-600 dark:text-zinc-300"
            }`}
          >
            {line}
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * Create a preview branch from the browser: `rocky preview create` on this
 * server. It registers the branch in the state store and copies each unchanged
 * model from its production schema into the warehouse schema
 * `branch__<name>`. It reads production tables and writes only that schema.
 * It does not re-run the changed models. It is refused (409) while a run,
 * apply, approve, freeze, unfreeze or product approval is in progress. A name already in use
 * fails the job; pick another.
 */
export function PreviewControls({
  jobs = defaultJobClient,
  onDone,
}: {
  jobs?: JobClient;
  onDone?: () => void;
}) {
  const preview = useJob("preview", jobs, onDone);
  const [name, setName] = useState("");
  const [base, setBase] = useState("");
  const nameValue = name.trim();
  const baseValue = base.trim();
  const body: Record<string, unknown> = {};
  if (nameValue !== "") body.name = nameValue;
  if (baseValue !== "") body.base = baseValue;
  const schema = nameValue === "" ? "branch__<derived from the git branch>" : `branch__${nameValue}`;
  return (
    <div className="space-y-3">
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Registers a branch and copies each unchanged model from production into its own warehouse schema, using
        this server&apos;s warehouse credentials. Production tables are only read. The changed models are
        planned, not re-run. It runs <code>rocky preview create</code> on this server.
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Branch name
          <input
            value={name}
            onChange={(event) => setName(event.target.value)}
            placeholder="optional: from the git branch"
            className={`${INPUT} w-64`}
          />
        </label>
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Base git ref
          <input
            value={base}
            onChange={(event) => setBase(event.target.value)}
            placeholder="main"
            className={`${INPUT} w-48`}
          />
        </label>
        <ConfirmedWrite
          label="Create preview"
          question={`Create a preview against ${baseValue === "" ? "main" : baseValue}? This writes warehouse schema ${schema}: each unchanged model is copied into it. Depending on the warehouse, each copy is a metadata-only clone or a full copy of the table, which costs compute and storage. Production tables are only read.`}
          confirmLabel="Confirm create preview"
          busy={jobBusy(preview.view)}
          onConfirm={() => preview.start(body)}
        />
      </div>
      <PreviewJobLine view={preview.view} cancel={preview.cancel} />
    </div>
  );
}
