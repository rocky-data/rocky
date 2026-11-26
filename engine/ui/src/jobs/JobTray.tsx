/**
 * The job tray: one card per job this tab started, stacked bottom-right,
 * kept across page changes (design: "Job progress pattern").
 *
 * What the engine can say decides what a card says. `GET /api/v1/jobs/{id}`
 * has the state, the times and the final output, so a card shows how long a
 * job has run, and how it ended. It has no progress and no log, so a card
 * shows neither. The engine queues nothing: a write while another holds the
 * project is refused at once, and its card says which job holds it.
 *
 * No card submits a write. A failed apply, approve or freeze is started
 * again from its own page, behind that page's confirm step and checks.
 */

import { CheckIcon, ExclamationTriangleIcon, XMarkIcon } from "@heroicons/react/20/solid";
import { useContext, useEffect, useState, useSyncExternalStore, type ReactNode } from "react";
import { ApiError } from "../api";
import { failureSummary, useWriteAccess } from "../operator";
import { navigateTo, usePathname } from "../router";
import { JobStoreContext } from "./context";
import { JobStore, type JobRecord } from "./store";

/** The shell's job store, for the whole tab. Tests hand one in. */
export function JobTrayProvider({ store, children }: { store?: JobStore; children: ReactNode }) {
  const [own] = useState(() => store ?? new JobStore());
  useEffect(() => {
    own.revive();
    return () => own.dispose();
  }, [own]);
  return <JobStoreContext.Provider value={own}>{children}</JobStoreContext.Provider>;
}

function useRecords(store: JobStore): JobRecord[] {
  return useSyncExternalStore(store.subscribe, store.all);
}

function inFlight(record: JobRecord): boolean {
  return record.view.kind === "submitting" || record.view.kind === "running";
}

/** Seconds as a short duration: "8 s", "2 min 5 s". */
export function shortDuration(ms: number): string {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) return `${seconds} s`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return rest === 0 ? `${minutes} min` : `${minutes} min ${rest} s`;
}

/** Now, again every second while `active`. */
function useNow(active: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, [active]);
  return now;
}

/** The engine's own run time for a finished job, when it recorded both ends. */
function ranFor(record: JobRecord): string | null {
  if (record.view.kind !== "done") return null;
  const { started_at, finished_at } = record.view.job;
  if (started_at == null || finished_at == null) return null;
  const ms = Date.parse(finished_at) - Date.parse(started_at);
  return Number.isFinite(ms) ? shortDuration(ms) : null;
}

/**
 * "N jobs running", in the top bar while any job this tab started is in
 * flight. Only this tab's: the engine has no list of jobs to read.
 */
export function RunningJobs() {
  const store = useContext(JobStoreContext);
  if (store === null) return null;
  return <RunningJobsCount store={store} />;
}

function RunningJobsCount({ store }: { store: JobStore }) {
  const count = useRecords(store).filter(inFlight).length;
  if (count === 0) return null;
  return (
    <span
      role="status"
      title="Jobs started in this tab. A job started elsewhere is not counted."
      className="inline-flex items-center gap-2 rounded-full border border-line bg-surface px-3 py-1 text-xs font-medium text-ink"
    >
      <span
        aria-hidden="true"
        className="size-2.5 animate-spin rounded-full border-2 border-run border-r-transparent"
      />
      {count === 1 ? "1 job running" : `${count} jobs running`}
    </span>
  );
}

/** The cards, bottom-right. Nothing while no job is shown. */
export function JobTray() {
  const store = useContext(JobStoreContext);
  if (store === null) return null;
  return <Cards store={store} />;
}

function Cards({ store }: { store: JobStore }) {
  const shown = useRecords(store).filter((record) => !record.closed);
  const now = useNow(shown.some(inFlight));
  if (shown.length === 0) return null;
  return (
    <section
      aria-label="Jobs"
      className="pointer-events-none fixed right-4 bottom-4 z-50 flex w-[min(380px,calc(100vw-2rem))] flex-col gap-3"
    >
      {shown.map((record) => (
        <Card key={record.key} record={record} now={now} store={store} />
      ))}
    </section>
  );
}

type CardTone = "pending" | "running" | "ok" | "fail" | "warn";

const BADGE: Record<CardTone, string> = {
  pending: "bg-surface-2 text-muted",
  running: "bg-run-soft text-run-ink",
  ok: "bg-ok-soft text-ok-ink",
  fail: "bg-fail-soft text-fail-ink",
  warn: "bg-warn-soft text-warn-ink",
};

function Badge({ tone }: { tone: CardTone }) {
  return (
    <span aria-hidden="true" className={`inline-flex size-6 shrink-0 items-center justify-center rounded-full ${BADGE[tone]}`}>
      {tone === "ok" ? (
        <CheckIcon className="size-4" />
      ) : tone === "fail" ? (
        <XMarkIcon className="size-4" />
      ) : tone === "warn" ? (
        <ExclamationTriangleIcon className="size-4" />
      ) : (
        <span className={`size-2 rounded-full ${tone === "running" ? "bg-run" : "bg-muted"}`} />
      )}
    </span>
  );
}

const CARD_BUTTON =
  "inline-flex h-8 items-center rounded-md border border-line bg-surface px-3 text-xs font-medium text-ink hover:bg-hover focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-500 disabled:cursor-not-allowed disabled:text-muted";

/** What a card says: its tone, its line under the label, and any detail. */
function describe(record: JobRecord, now: number): { tone: CardTone; line: string; detail?: string } {
  const view = record.view;
  switch (view.kind) {
    case "idle":
    case "submitting":
      return { tone: "pending", line: "Sending…" };
    case "running":
      return {
        tone: "running",
        line: record.stopping
          ? "Stopping…"
          : `Running · ${shortDuration(now - record.submittedAt)}`,
      };
    case "refused": {
      const error = view.error;
      const holder = error instanceof ApiError ? error.envelope.running_job_id : undefined;
      return {
        tone: "warn",
        line:
          typeof holder === "string"
            ? `Not started. Job ${holder} holds the project. Try again when it ends.`
            : "Not started. Another job holds the project. Try again when it ends.",
      };
    }
    case "lost":
      return {
        tone: "warn",
        line: "Outcome unknown. The page lost track of this job, which may have finished.",
        detail: `Job ${view.jobId}. Check Runs or the audit log before you try again.`,
      };
    case "done": {
      const took = ranFor(record);
      switch (view.job.state) {
        case "succeeded":
          return { tone: "ok", line: took === null ? "Done" : `Done · ${took}` };
        case "cancelled":
          return { tone: "warn", line: "Cancelled" };
        default: {
          const summary = failureSummary(view.job);
          const [first, ...rest] = summary.split("\n");
          return { tone: "fail", line: first, detail: rest.length > 0 ? summary : undefined };
        }
      }
    }
  }
}

function Card({
  record,
  now,
  store,
}: {
  record: JobRecord;
  now: number;
  store: JobStore;
}) {
  const access = useWriteAccess();
  const pathname = usePathname();
  const { tone, line, detail } = describe(record, now);
  const failed = tone === "fail";
  const running = record.view.kind === "running";
  const sendingCancel = record.cancel.kind === "sending";
  const elsewhere = record.from !== "" && record.from !== pathname;
  return (
    <article
      aria-label={record.label}
      className={`pointer-events-auto rounded-xl border bg-surface shadow-lg ${failed ? "border-fail/40" : "border-line"}`}
    >
      <div className="flex items-start gap-3 px-4 pt-3.5 pb-3">
        <Badge tone={tone} />
        <div className="min-w-0 flex-1 space-y-0.5">
          <p className="text-sm font-semibold text-ink">{record.label}</p>
          <p role={failed ? "alert" : "status"} className="text-[13px] break-words text-muted">
            {line}
          </p>
        </div>
        {!inFlight(record) && (
          <button
            type="button"
            aria-label={`Close: ${record.label}`}
            onClick={() => store.close(record.key)}
            className="-m-1 rounded p-1 text-muted hover:text-ink"
          >
            <XMarkIcon aria-hidden="true" className="size-4" />
          </button>
        )}
      </div>
      {detail !== undefined && (
        <details className="px-4 pb-3 pl-13">
          <summary className="cursor-pointer text-xs text-muted">Details</summary>
          <pre className="mt-1 max-h-40 overflow-auto rounded-md border border-divider bg-surface-2 p-2 font-mono text-[11px] whitespace-pre-wrap break-words text-ink-2">
            {detail}
          </pre>
        </details>
      )}
      {(running && access.kind === "operator") || (failed && elsewhere) ? (
        <div className="flex flex-wrap gap-2 px-4 pb-3.5 pl-13">
          {running && access.kind === "operator" && (
            <button
              type="button"
              className={CARD_BUTTON}
              disabled={sendingCancel}
              onClick={() => store.cancel(record.key)}
            >
              {sendingCancel ? "Stopping…" : record.stopping ? "Force stop" : "Cancel"}
            </button>
          )}
          {failed && elsewhere && (
            <button type="button" className={CARD_BUTTON} onClick={() => navigateTo(record.from)}>
              Open the page it ran from
            </button>
          )}
        </div>
      ) : null}
    </article>
  );
}
