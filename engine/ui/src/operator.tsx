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
 */
export function OperatorBanner({ access }: { access: WriteAccess }) {
  if (access.kind === "operator") {
    return (
      <div
        role="status"
        aria-label="Operator mode"
        className="border-b border-amber-300 bg-amber-100 px-4 py-1.5 text-center text-xs font-semibold text-amber-900 dark:border-amber-700 dark:bg-amber-900/60 dark:text-amber-100"
      >
        {OPERATOR_MODE_LABEL}
      </div>
    );
  }
  return (
    <div
      role="status"
      aria-label="Read-only"
      className="border-b border-zinc-200 bg-zinc-100 px-4 py-1 text-center text-xs text-zinc-600 dark:border-white/10 dark:bg-zinc-900 dark:text-zinc-300"
    >
      Read-only. {access.reason}
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

/** The routes a job is submitted to and read from. Tests hand in fakes. */
export interface JobClient {
  submit: (kind: JobKind, body: Record<string, unknown>) => Promise<{ job_id: string }>;
  status: (jobId: string) => Promise<JobStatus>;
}

export const defaultJobClient: JobClient = {
  submit: (kind, body) => apiPost<{ job_id: string }>(`jobs/${kind}`, body),
  status: (jobId) => apiGet<JobStatus>(`jobs/${encodeURIComponent(jobId)}`),
};

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
): { view: JobView; start: (body?: Record<string, unknown>) => void } {
  const [view, setView] = useState<JobView>({ kind: "idle" });
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
            setView({ kind: "done", job });
            done.current?.(job);
          })
          .catch((error: unknown) => {
            if (!alive.current) return;
            setView({ kind: "refused", error: error instanceof Error ? error : new Error(String(error)) });
          });
      };
      tick();
    },
    [client, pollMs],
  );

  const start = useCallback(
    (body: Record<string, unknown> = {}) => {
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
          setView({ kind: "refused", error: error instanceof Error ? error : new Error(String(error)) });
        });
    },
    [client, kind, follow],
  );

  return { view, start };
}

/** Whether a job is in flight, so its button stays pressed. */
export function jobBusy(view: JobView): boolean {
  return view.kind === "submitting" || view.kind === "running";
}

/**
 * A control that changes something. In read-only mode it is drawn disabled,
 * with the reason beside it — never hidden, so the reader knows the action
 * exists and why it is not theirs.
 */
export function WriteButton({
  label,
  busyLabel,
  busy = false,
  disabledReason,
  onClick,
}: {
  label: string;
  busyLabel?: string;
  busy?: boolean;
  /** A reason this action cannot run now, beyond read-only mode. */
  disabledReason?: string;
  onClick: () => void;
}) {
  const access = useWriteAccess();
  const reason = access.kind === "read_only" ? access.reason : disabledReason;
  const disabled = reason !== undefined || busy;
  return (
    <span className="inline-flex flex-col items-start gap-0.5">
      <button
        type="button"
        onClick={onClick}
        disabled={disabled}
        title={reason}
        className="rounded border border-amber-400 bg-amber-50 px-2 py-1 text-xs font-semibold text-amber-900 hover:bg-amber-100 disabled:cursor-not-allowed disabled:border-zinc-300 disabled:bg-zinc-100 disabled:text-zinc-400 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100 dark:disabled:border-zinc-700 dark:disabled:bg-zinc-900 dark:disabled:text-zinc-500"
      >
        {busy ? (busyLabel ?? `${label}…`) : label}
      </button>
      {reason !== undefined && (
        <span className="text-[11px] text-zinc-500 dark:text-zinc-400">{reason}</span>
      )}
    </span>
  );
}

/**
 * One line saying where a job stands. A `409 mutation_in_progress` says so
 * plainly: another run, apply or approve is going, wait for it.
 */
export function JobLine({ label, view }: { label: string; view: JobView }) {
  switch (view.kind) {
    case "idle":
      return null;
    case "submitting":
      return <p className="text-xs text-zinc-600 dark:text-zinc-300">{label}: submitting…</p>;
    case "running":
      return (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">
          {label}: running (job <code>{view.jobId}</code>)…
        </p>
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
      return view.job.state === "succeeded" ? (
        <p className="text-xs text-emerald-700 dark:text-emerald-300">{label}: done.</p>
      ) : (
        <p role="alert" className="text-xs text-red-700 dark:text-red-300">
          {label}: failed. {view.job.error ?? "The job ended without a message."}
        </p>
      );
  }
}
