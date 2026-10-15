import { useState } from "react";
import type { BriefActiveFreeze } from "@rocky-types/brief";
import type { JobStatus } from "@rocky-types/job_status";
import { READ_BUTTON } from "../components";
import {
  JobLine,
  WriteButton,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobCancel,
  type JobClient,
  type JobView,
} from "../operator";

type FreezePrincipal = "both" | "agent" | "human";

const INPUT =
  "h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm dark:border-zinc-700 dark:bg-zinc-900";

/** The principals a freeze names, said in words. */
function principalWords(principal: FreezePrincipal | string): string {
  return principal === "both" ? "agent and human" : principal;
}

/** The scope a freeze names, said in words (`any` is every model). */
function scopeWords(scope: string): string {
  return scope === "any" ? "every model" : scope;
}

/** The most notes a freeze result shows, and the most characters of each. */
const NOTES_MAX = 5;
const NOTE_MAX_CHARS = 300;

/**
 * What a succeeded freeze or unfreeze says in `result.notes`. The engine
 * writes them as fixed sentences (and the server cleans file paths out of
 * them), so the page shows the strings as they are. Anything that is not a
 * string is dropped, and the count and length are capped.
 */
function resultNotes(job: JobStatus): { notes: string[]; notEnforced: boolean } {
  const result = job.result as { notes?: unknown } | null | undefined;
  const raw = result !== null && typeof result === "object" && Array.isArray(result.notes) ? result.notes : [];
  const all = raw
    .filter((note): note is string => typeof note === "string")
    .map((note) => note.trim())
    .filter((note) => note !== "");
  return {
    notes: all
      .slice(0, NOTES_MAX)
      .map((note) => (note.length <= NOTE_MAX_CHARS ? note : `${note.slice(0, NOTE_MAX_CHARS)}…`)),
    // Judged on every note, not only the ones shown.
    notEnforced: all.some((note) => note.includes("NOT enforced")),
  };
}

/**
 * The job line for a freeze or unfreeze. A succeeded job can still have done
 * nothing useful: with no `[policy]` block the freeze is recorded but binds
 * nothing. The engine says so in `result.notes`, so the page shows them and
 * never a bare "done" over them. Only "NOT enforced" is a warning; any other
 * note is shown as the engine wrote it.
 */
function FreezeJobLine({ label, view, cancel }: { label: string; view: JobView; cancel: JobCancel }) {
  if (view.kind !== "done" || view.job.state !== "succeeded") {
    return <JobLine label={label} view={view} cancel={cancel} />;
  }
  const { notes, notEnforced } = resultNotes(view.job);
  if (notes.length === 0) return <JobLine label={label} view={view} cancel={cancel} />;
  const warn = notEnforced;
  const headline = notEnforced ? `${label}: recorded, but NOT enforced.` : `${label}: done.`;
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
      <ul aria-label={`${label} notes`} className="list-disc space-y-0.5 pl-5">
        {notes.map((note, index) => (
          <li
            key={index}
            className={`break-words text-xs ${
              warn ? "text-amber-800 dark:text-amber-300" : "text-zinc-600 dark:text-zinc-300"
            }`}
          >
            {note}
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * A two-step control: the first press asks, the second runs. Freeze and
 * unfreeze change what every agent may do, so neither runs on one click.
 */
function ConfirmedWrite({
  label,
  question,
  confirmLabel,
  busy,
  onConfirm,
}: {
  label: string;
  question: string;
  confirmLabel: string;
  busy: boolean;
  onConfirm: () => void;
}) {
  const [asking, setAsking] = useState(false);
  if (!asking || busy) {
    return <WriteButton label={label} busy={busy} onClick={() => setAsking(true)} />;
  }
  return (
    <div
      role="group"
      aria-label={question}
      className="space-y-2 rounded-md border border-amber-300 p-3 dark:border-amber-700"
    >
      <p className="text-sm text-zinc-800 dark:text-zinc-100">{question}</p>
      <div className="flex flex-wrap items-center gap-2">
        <WriteButton
          label={confirmLabel}
          primary
          onClick={() => {
            setAsking(false);
            onConfirm();
          }}
        />
        <button type="button" className={READ_BUTTON} onClick={() => setAsking(false)}>
          Back
        </button>
      </div>
    </div>
  );
}

/**
 * The body that lifts one freeze row. A row normally names its principal and
 * scope, and the unfreeze names the same. A freeze marker whose body could not
 * be read is in force for both principals on `any`, and `rocky policy
 * unfreeze` lifts it only with no principal and scope `any`, so that row sends
 * no principal.
 */
function unfreezeBody(freeze: BriefActiveFreeze): Record<string, unknown> {
  return freeze.unreadable_marker === true
    ? { scope: "any" }
    : { principal: freeze.principal, scope: freeze.scope };
}

/**
 * The freezes to list. An unreadable marker arrives as one row per principal
 * and one Unfreeze lifts it for both, so it is listed once.
 */
function listedFreezes(freezes: BriefActiveFreeze[]): BriefActiveFreeze[] {
  const seen = new Set<string>();
  return freezes.filter((entry) => {
    if (entry.unreadable_marker !== true) return true;
    if (seen.has(entry.plan_id)) return false;
    seen.add(entry.plan_id);
    return true;
  });
}

/** Lift one freeze in force: the same principal and scope it names. */
function UnfreezeRow({
  freeze,
  jobs,
  onChanged,
}: {
  freeze: BriefActiveFreeze;
  jobs: JobClient;
  onChanged: () => void;
}) {
  const unfreeze = useJob("unfreeze", jobs, onChanged);
  const unreadable = freeze.unreadable_marker === true;
  const name = unreadable
    ? "unreadable marker (agent and human) on every model"
    : `${freeze.principal} on ${scopeWords(freeze.scope)}`;
  return (
    <li className="space-y-1">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm text-zinc-700 dark:text-zinc-200">{name}</span>
        <ConfirmedWrite
          label={`Unfreeze ${name}`}
          question={
            unreadable
              ? "Lift every unreadable marker and every freeze on scope any, for both agent and human? Those actions can run again."
              : `Lift the freeze on ${name}? Those actions can run again.`
          }
          confirmLabel="Confirm unfreeze"
          busy={jobBusy(unfreeze.view)}
          onConfirm={() => unfreeze.start(unfreezeBody(freeze))}
        />
      </div>
      <FreezeJobLine label={`Unfreeze ${name}`} view={unfreeze.view} cancel={unfreeze.cancel} />
    </li>
  );
}

/**
 * The kill switch from the browser: freeze actions to `deny` for a principal
 * and a scope, and lift a freeze in force. Each runs `rocky policy freeze` or
 * `rocky policy unfreeze` on the server. Both are refused (409) while a run,
 * apply or approve is in progress, so retry once it finishes; to stop a
 * running run, use Cancel on that job. The ledger row names the server's principal id, not the
 * person at the page, and does not say the freeze came from the browser.
 */
export function FreezeControls({
  freezes,
  jobs = defaultJobClient,
  onChanged,
}: {
  freezes: BriefActiveFreeze[];
  jobs?: JobClient;
  onChanged: () => void;
}) {
  const freeze = useJob("freeze", jobs, onChanged);
  const [principal, setPrincipal] = useState<FreezePrincipal>("both");
  const [scope, setScope] = useState("");
  const [reason, setReason] = useState("");
  const scopeValue = scope.trim();
  const reasonValue = reason.trim();
  const body: Record<string, unknown> = {};
  if (principal !== "both") body.principal = principal;
  if (scopeValue !== "") body.scope = scopeValue;
  if (reasonValue !== "") body.reason = reasonValue;
  const target = `${principalWords(principal)} on ${scopeWords(scopeValue === "" ? "any" : scopeValue)}`;
  return (
    <section
      aria-label="Freeze controls"
      className="space-y-3 rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900"
    >
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">Freeze</h3>
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        A freeze denies the matching actions until you lift it. It runs{" "}
        <code>rocky policy freeze</code> on this server and records the server&apos;s principal id. The
        ledger does not record that the freeze came from the browser.
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Principal
          <select
            value={principal}
            onChange={(event) => setPrincipal(event.target.value as FreezePrincipal)}
            className={INPUT}
          >
            <option value="both">both</option>
            <option value="agent">agent</option>
            <option value="human">human</option>
          </select>
        </label>
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Scope
          <input
            value={scope}
            onChange={(event) => setScope(event.target.value)}
            placeholder="any, model=orders_*, layer=gold"
            className={`${INPUT} w-64`}
          />
        </label>
        <label className="flex flex-col gap-1 text-xs text-zinc-600 dark:text-zinc-300">
          Reason
          <input
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            placeholder="optional"
            className={`${INPUT} w-64`}
          />
        </label>
        <ConfirmedWrite
          label="Freeze"
          question={`Freeze ${target}? Matching actions are denied until you lift it.`}
          confirmLabel="Confirm freeze"
          busy={jobBusy(freeze.view)}
          onConfirm={() => freeze.start(body)}
        />
      </div>
      <FreezeJobLine label="Freeze" view={freeze.view} cancel={freeze.cancel} />
      {freezes.length > 0 && (
        <ul aria-label="Lift a freeze" className="space-y-2 border-t border-zinc-200 pt-3 dark:border-zinc-800">
          {listedFreezes(freezes).map((entry) => (
            <UnfreezeRow
              key={`${entry.principal}|${entry.scope}|${entry.plan_id}`}
              freeze={entry}
              jobs={jobs}
              onChanged={onChanged}
            />
          ))}
        </ul>
      )}
    </section>
  );
}
