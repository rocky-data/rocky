/**
 * Operator mode: what this page may change, and the controls that change it.
 *
 * The engine says what the session's token may do in `GET /api/v1/meta`
 * (`token_scope`). `full` is operator mode: the page can run, plan, approve
 * and apply, and every change runs as the OS user who started `rocky serve`.
 * `read_only` shows the same controls, disabled, with the reason. `null`
 * means the server has no token at all (a loopback server without `--ui`,
 * the `just ui-dev` loop), which accepts writes too.
 *
 * Reading and writing must LOOK different, so the shell draws an indicator
 * the whole time operator mode is on (`OperatorBanner`).
 */

import { EyeIcon, ShieldExclamationIcon } from "@heroicons/react/20/solid";
import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import type { MetaTokenScope } from "@rocky-types/meta";
import { ApiError, apiGet, apiPost } from "./api";

/** What the page may do. */
export type WriteAccess =
  | { kind: "operator"; scope: MetaTokenScope | null }
  | { kind: "read_only"; reason: string };

/** Why a read-only page cannot write, said the same way everywhere. */
export const READ_ONLY_REASON = "This UI was started read-only (rocky serve --ui --read-only).";

/** Until the engine has said, nothing can be pressed. */
const UNKNOWN: WriteAccess = {
  kind: "read_only",
  reason: "The engine has not said yet what this UI may change.",
};

/** The access the engine's `token_scope` grants. */
export function accessFromScope(scope: MetaTokenScope | null | undefined): WriteAccess {
  switch (scope) {
    case "read_only":
      return { kind: "read_only", reason: READ_ONLY_REASON };
    case "full":
      return { kind: "operator", scope: "full" };
    case null:
    case undefined:
      return { kind: "operator", scope: null };
  }
}

const WriteAccessContext = createContext<WriteAccess>(UNKNOWN);

export const WriteAccessProvider = WriteAccessContext.Provider;

export function useWriteAccess(): WriteAccess {
  return useContext(WriteAccessContext);
}

/** The words of the indicator. Exported so tests pin one string. */
export const OPERATOR_MODE_LABEL = "Operator mode — changes run as this server's user";

/**
 * Always visible while the page can change things, so a write never looks
 * like a read. Read-only says so too, more quietly.
 *
 * Drawn as a pill at the end of a slim bar, not as a full-width amber band:
 * the band read as an alarm on every screen, and an alarm that never goes
 * away stops being read. The pill keeps the same words, the same role and
 * the same place — the sticky top, above every lane — and stays amber.
 */
export function OperatorBanner({ access }: { access: WriteAccess }) {
  if (access.kind === "operator") {
    return (
      <div className="flex justify-end border-b border-zinc-200 bg-white/90 px-4 py-1.5 backdrop-blur-sm sm:px-6 lg:px-8 dark:border-white/10 dark:bg-zinc-950/90">
        <div
          role="status"
          aria-label="Operator mode"
          className="inline-flex items-center gap-1.5 rounded-lg border sm:rounded-full border-amber-400 bg-amber-50 px-2.5 py-0.5 text-xs font-medium text-amber-900 dark:border-amber-700 dark:bg-amber-950/60 dark:text-amber-200"
        >
          <ShieldExclamationIcon aria-hidden="true" className="size-3.5 shrink-0" />
          {OPERATOR_MODE_LABEL}
        </div>
      </div>
    );
  }
  return (
    <div className="flex justify-end border-b border-zinc-200 bg-white/90 px-4 py-1.5 backdrop-blur-sm sm:px-6 lg:px-8 dark:border-white/10 dark:bg-zinc-950/90">
      <div
        role="status"
        aria-label="Read-only"
        className="inline-flex items-center gap-1.5 rounded-lg border sm:rounded-full border-zinc-300 px-2.5 py-0.5 text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-300"
      >
        <EyeIcon aria-hidden="true" className="size-3.5 shrink-0" />
        Read-only. {access.reason}
      </div>
    </div>
  );
}

/** One job as the page follows it. */
export type JobView =
  | { kind: "idle" }
  | { kind: "submitting" }
  | { kind: "refused"; error: ApiError | Error }
  | { kind: "running"; jobId: string }
  | { kind: "done"; job: JobStatus };

/** What `POST /api/v1/jobs/{id}/cancel` asked the job to do. */
export type CancelSignal = "interrupt" | "kill";

/** The routes a job is submitted to, read from and cancelled at. Tests hand in fakes. */
export interface JobClient {
  submit: (kind: JobKind, body: Record<string, unknown>) => Promise<{ job_id: string }>;
  status: (jobId: string) => Promise<JobStatus>;
  cancel: (jobId: string) => Promise<{ job_id: string; signal: CancelSignal }>;
}

export const defaultJobClient: JobClient = {
  submit: (kind, body) => apiPost<{ job_id: string }>(`jobs/${kind}`, body),
  status: (jobId) => apiGet<JobStatus>(`jobs/${encodeURIComponent(jobId)}`),
  cancel: (jobId) =>
    apiPost<{ job_id: string; signal: CancelSignal }>(`jobs/${encodeURIComponent(jobId)}/cancel`, {}),
};

/**
 * Where a cancel of the running job stands. The engine interrupts the job
 * on the first request (as Ctrl-C does) and kills it on the next, so after
 * an interrupt the button offers a forced stop.
 */
export type CancelView =
  | { kind: "idle" }
  | { kind: "sending" }
  | { kind: "sent"; signal: CancelSignal }
  | { kind: "refused"; error: ApiError | Error };

/** The cancel control `useJob` hands to `JobLine`. */
export interface JobCancel {
  view: CancelView;
  request: () => void;
}

/** How often a running job is read again. */
export const JOB_POLL_MS = 1_000;

/**
 * Submit a job and follow it to the end by polling `GET /api/v1/jobs/{id}`.
 * `onDone` runs once with the terminal record, so the caller can refresh
 * what the job changed.
 */
export function useJob(
  kind: JobKind,
  client: JobClient = defaultJobClient,
  onDone?: (job: JobStatus) => void,
  pollMs: number = JOB_POLL_MS,
): { view: JobView; start: (body?: Record<string, unknown>) => void; cancel: JobCancel } {
  const [view, setView] = useState<JobView>({ kind: "idle" });
  const [cancelView, setCancelView] = useState<CancelView>({ kind: "idle" });
  // A second cancel click before the first answers is ignored, like `start`.
  const cancelInFlight = useRef(false);
  const done = useRef(onDone);
  useEffect(() => {
    done.current = onDone;
  }, [onDone]);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    return () => {
      alive.current = false;
    };
  }, []);

  // A second start while one job is in flight is ignored, even when two
  // clicks land before React re-renders the button disabled.
  const inFlight = useRef(false);
  const settle = useCallback((next: JobView) => {
    inFlight.current = false;
    setView(next);
  }, []);

  const follow = useCallback(
    (jobId: string) => {
      const tick = () => {
        client
          .status(jobId)
          .then((job) => {
            if (!alive.current) return;
            if (job.state === "running" || job.state === "queued") {
              setTimeout(tick, pollMs);
              return;
            }
            settle({ kind: "done", job });
            done.current?.(job);
          })
          .catch((error: unknown) => {
            if (!alive.current) return;
            settle({ kind: "refused", error: error instanceof Error ? error : new Error(String(error)) });
          });
      };
      tick();
    },
    [client, pollMs, settle],
  );

  const start = useCallback(
    (body: Record<string, unknown> = {}) => {
      if (inFlight.current) return;
      inFlight.current = true;
      setCancelView({ kind: "idle" });
      setView({ kind: "submitting" });
      client
        .submit(kind, body)
        .then(({ job_id }) => {
          if (!alive.current) return;
          setView({ kind: "running", jobId: job_id });
          follow(job_id);
        })
        .catch((error: unknown) => {
          if (!alive.current) return;
          settle({ kind: "refused", error: error instanceof Error ? error : new Error(String(error)) });
        });
    },
    [client, kind, follow, settle],
  );

  // Cancel the job this hook is following. The job keeps being polled: its
  // final state (`cancelled`, or `succeeded` if it finished first) is what
  // the line shows in the end.
  const jobId = view.kind === "running" ? view.jobId : null;
  const requestCancel = useCallback(() => {
    if (jobId === null || cancelInFlight.current) return;
    cancelInFlight.current = true;
    setCancelView({ kind: "sending" });
    client
      .cancel(jobId)
      .then(({ signal }) => {
        cancelInFlight.current = false;
        if (!alive.current) return;
        setCancelView({ kind: "sent", signal });
      })
      .catch((error: unknown) => {
        cancelInFlight.current = false;
        if (!alive.current) return;
        setCancelView({ kind: "refused", error: error instanceof Error ? error : new Error(String(error)) });
      });
  }, [client, jobId]);

  return { view, start, cancel: { view: cancelView, request: requestCancel } };
}

/** Whether a job is in flight, so its button stays pressed. */
export function jobBusy(view: JobView): boolean {
  return view.kind === "submitting" || view.kind === "running";
}

/** What a button says while its job is in flight. */
export const RUNNING_LABEL = "running…";

/**
 * A control that changes something. In read-only mode it is drawn disabled —
 * never hidden, so the reader knows the action exists. The read-only reason
 * is the button's tooltip only: the shell banner already says it once, so
 * it is not repeated under every button. A reason of this button's own
 * (`disabledReason`, such as "Approve the plan first.") is shown beside it.
 * While its job is in flight the button is disabled and says "running…".
 */
export function WriteButton({
  label,
  busy = false,
  busyLabel = RUNNING_LABEL,
  disabledReason,
  onClick,
  primary = false,
}: {
  label: string;
  busy?: boolean;
  /** What the button says after its label while busy. */
  busyLabel?: string;
  /** A reason this action cannot run now, beyond read-only mode. */
  disabledReason?: string;
  onClick: () => void;
  /**
   * The one action the screen exists for (Run, Approve, Apply): solid
   * orange. Every other write keeps the amber outline. Both stay apart from
   * the neutral grey of a read, so a write never looks like a read.
   */
  primary?: boolean;
}) {
  const access = useWriteAccess();
  const readOnlyReason = access.kind === "read_only" ? access.reason : undefined;
  const ownReason = readOnlyReason === undefined ? disabledReason : undefined;
  const disabled = readOnlyReason !== undefined || disabledReason !== undefined || busy;
  return (
    <span className="inline-flex flex-col items-start gap-0.5">
      <button
        type="button"
        onClick={onClick}
        disabled={disabled}
        aria-busy={busy}
        title={readOnlyReason ?? disabledReason}
        className={`inline-flex h-9 items-center rounded-md border px-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-500 disabled:cursor-not-allowed disabled:border-zinc-300 disabled:bg-zinc-100 disabled:text-zinc-400 dark:disabled:border-zinc-700 dark:disabled:bg-zinc-900 dark:disabled:text-zinc-500 ${
          primary
            ? "border-orange-500 bg-orange-500 text-zinc-950 hover:bg-orange-400"
            : "border-amber-400 bg-amber-50 text-amber-900 hover:bg-amber-100 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100 dark:hover:bg-amber-900"
        }`}
      >
        {busy ? `${label}: ${busyLabel}` : label}
      </button>
      {ownReason !== undefined && (
        <span className="max-w-xs text-xs text-zinc-500 dark:text-zinc-400">{ownReason}</span>
      )}
    </span>
  );
}

/** The most of a failure the page shows, in characters. */
export const FAILURE_MAX_CHARS = 1_500;

/** A JSON tracing event: an object with a `level`. Other JSON is kept. */
function isJsonTracingEvent(line: string): boolean {
  try {
    const value: unknown = JSON.parse(line);
    return value !== null && typeof value === "object" && "level" in value;
  } catch {
    return false;
  }
}

/** A line a `tracing` subscriber wrote: a JSON event, ANSI-coloured, or `fmt`. */
function isTracingLine(line: string): boolean {
  const trimmed = line.trimStart();
  if (trimmed.startsWith("{")) return isJsonTracingEvent(trimmed);
  return (
    trimmed.startsWith("\u001b") ||
    /^\d{4}-\d{2}-\d{2}T/.test(trimmed) ||
    /^(TRACE|DEBUG|INFO|WARN|ERROR) /.test(trimmed)
  );
}

function cap(text: string): string {
  return text.length <= FAILURE_MAX_CHARS ? text : `${text.slice(0, FAILURE_MAX_CHARS)}…`;
}

/**
 * What a failed job says, concisely.
 *
 * First the structured errors of the child's own output (`result.errors`,
 * e.g. `model 'dim_customer' failed: ...`). Failing that, the final
 * `Error:` / `Caused by:` block of `error`, with any tracing lines dropped:
 * a server run with `RUST_LOG` can carry SQL and local paths there. Never
 * the raw stderr. Capped at [`FAILURE_MAX_CHARS`].
 */
export function failureSummary(job: JobStatus): string {
  const result = job.result as { errors?: unknown } | null | undefined;
  if (result !== null && typeof result === "object" && Array.isArray(result.errors)) {
    const lines = result.errors
      .map((entry: unknown) => {
        if (entry === null || typeof entry !== "object") return null;
        const { asset_key, error } = entry as { asset_key?: unknown; error?: unknown };
        if (typeof error !== "string" || error.trim() === "") return null;
        const where = Array.isArray(asset_key) ? asset_key.join(".") : "";
        return where === "" ? error.trim() : `${where}: ${error.trim()}`;
      })
      .filter((line): line is string => line !== null);
    if (lines.length > 0) return cap(lines.join("\n"));
  }
  const lines = (job.error ?? "").split("\n").filter((line) => !isTracingLine(line));
  let start = -1;
  lines.forEach((line, index) => {
    if (line.startsWith("Error:")) start = index;
  });
  const text = (start >= 0 ? lines.slice(start) : lines.slice(-10)).join("\n").trim();
  return text === "" ? "The job ended without a message." : cap(text);
}

/** What the Cancel button says once the job was interrupted. */
export const FORCE_STOP_LABEL = "Force stop";

/**
 * Cancel for a running job, in operator mode only (a read-only page did not
 * start the job and cannot stop it, so it shows nothing). The first press
 * interrupts the job, as Ctrl-C does: a replication run finishes the copies
 * in flight and saves its state before it stops. If it does not stop, the
 * button then offers a forced stop, which kills it at once.
 */
function CancelControl({ cancel }: { cancel: JobCancel }) {
  const access = useWriteAccess();
  if (access.kind !== "operator") return null;
  const view = cancel.view;
  const interrupted = view.kind === "sent";
  const label = interrupted ? FORCE_STOP_LABEL : "Cancel";
  const busy = view.kind === "sending" || (view.kind === "sent" && view.signal === "kill");
  return (
    <div className="space-y-0.5">
      <WriteButton label={label} busy={busy} busyLabel="stopping…" onClick={cancel.request} />
      {view.kind === "sent" && view.signal === "interrupt" && (
        <p className="text-[11px] text-zinc-500 dark:text-zinc-400">
          Asked the job to stop, as Ctrl-C does. A run finishes the copies in flight first.
        </p>
      )}
      {view.kind === "refused" && (
        <p role="alert" className="text-[11px] text-red-700 dark:text-red-300">
          Cancel refused.{" "}
          {view.error instanceof ApiError
            ? `${view.error.envelope.code}: ${view.error.envelope.message}`
            : view.error.message}
        </p>
      )}
    </div>
  );
}

/**
 * One line saying where a job stands. A `409 mutation_in_progress` says so
 * plainly: another run, apply or approve is going, wait for it. Given a
 * `cancel`, a running job also shows the Cancel button (operator mode only).
 */
export function JobLine({ label, view, cancel }: { label: string; view: JobView; cancel?: JobCancel }) {
  switch (view.kind) {
    case "idle":
      return null;
    case "submitting":
      return <p className="text-xs text-zinc-600 dark:text-zinc-300">{label}: submitting…</p>;
    case "running":
      return (
        <div className="space-y-1">
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            {label}: running (job <code>{view.jobId}</code>)…
          </p>
          {cancel !== undefined && <CancelControl cancel={cancel} />}
        </div>
      );
    case "refused": {
      const error = view.error;
      if (error instanceof ApiError && error.envelope.code === "mutation_in_progress") {
        return (
          <p role="alert" className="text-xs text-amber-800 dark:text-amber-300">
            {label}: not started. Another run, apply or approve is already in progress on this
            project. Wait for it to finish, then try again.
          </p>
        );
      }
      const text =
        error instanceof ApiError
          ? `${error.envelope.code}: ${error.envelope.remediation_hint ?? error.envelope.message}`
          : error.message;
      return (
        <p role="alert" className="text-xs text-red-700 dark:text-red-300">
          {label}: refused. {text}
        </p>
      );
    }
    case "done":
      if (view.job.state === "cancelled") {
        return (
          <p role="status" className="text-xs text-amber-800 dark:text-amber-300">
            {label}: cancelled.
          </p>
        );
      }
      return view.job.state === "succeeded" ? (
        <p className="text-xs text-emerald-700 dark:text-emerald-300">{label}: done.</p>
      ) : (
        <p role="alert" className="whitespace-pre-wrap break-words text-xs text-red-700 dark:text-red-300">
          {label}: failed. {failureSummary(view.job)}
        </p>
      );
  }
}
