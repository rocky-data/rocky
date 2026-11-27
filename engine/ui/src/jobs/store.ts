import type { JobKind, JobStatus } from "@rocky-types/job_status";
import { ApiError, apiGet, apiPost } from "../api";

/** One job as the page follows it. */
export type JobView =
  | { kind: "idle" }
  | { kind: "submitting" }
  | { kind: "refused"; error: ApiError | Error }
  | { kind: "running"; jobId: string }
  /**
   * The page can no longer read the job (the session ended, or the engine
   * no longer knows the id). The job may have finished either way, so its
   * outcome is unknown: never shown as failed, never offered again.
   */
  | { kind: "lost"; jobId: string; error: ApiError | Error }
  | { kind: "done"; job: JobStatus };

/** What `POST /api/v1/jobs/{id}/cancel` asked the job to do. */
export type CancelSignal = "terminate" | "kill";

/** The routes a job is submitted to, read from and cancelled at. Tests hand in fakes. */
export interface JobClient {
  submit: (kind: JobKind, body: Record<string, unknown>) => Promise<{ job_id: string }>;
  status: (jobId: string) => Promise<JobStatus>;
  cancel: (jobId: string) => Promise<{ job_id: string; signal: CancelSignal }>;
}

/** How long a status read may take before it counts as a failed read. */
export const JOB_STATUS_TIMEOUT_MS = 15_000;

/** How long a cancel request may take before it counts as refused. */
export const JOB_CANCEL_TIMEOUT_MS = 15_000;

/**
 * `fetch` with a deadline, for the job client only: other callers of the api
 * module keep a request with no deadline. A request that passes it rejects.
 */
function fetchWithin(ms: number): typeof fetch {
  return (input, init) => fetch(input, { ...init, signal: AbortSignal.timeout(ms) });
}

function isTimeout(error: unknown): boolean {
  return error instanceof DOMException && (error.name === "TimeoutError" || error.name === "AbortError");
}

export const defaultJobClient: JobClient = {
  submit: (kind, body) => apiPost<{ job_id: string }>(`jobs/${kind}`, body),
  status: (jobId) =>
    apiGet<JobStatus>(`jobs/${encodeURIComponent(jobId)}`, { fetch: fetchWithin(JOB_STATUS_TIMEOUT_MS) }),
  cancel: (jobId) =>
    apiPost<{ job_id: string; signal: CancelSignal }>(
      `jobs/${encodeURIComponent(jobId)}/cancel`,
      {},
      { fetch: fetchWithin(JOB_CANCEL_TIMEOUT_MS) },
    ).catch((error: unknown) => {
      if (isTimeout(error)) throw new Error("The engine did not answer the cancel request in time.");
      throw error;
    }),
};

/**
 * Where the latest cancel request of the running job stands. The engine
 * sends the job SIGTERM on the first request (which `rocky run` handles like
 * Ctrl-C) and SIGTERM again on the second: the run's forced exit, which
 * cancels its warehouse statements and exits. If the job is still running
 * 15 s after that, the engine kills it. A third request kills it at once.
 */
export type CancelView =
  | { kind: "idle" }
  | { kind: "sending" }
  | { kind: "sent"; signal: CancelSignal }
  | { kind: "refused"; error: ApiError | Error };

/** How often a running job is read again. */
export const JOB_POLL_MS = 1_000;

/** The longest wait between reads while the engine cannot be reached. */
export const JOB_POLL_MAX_MS = 30_000;

/** How long a succeeded job's card (or a not-started one) stays in the tray. */
export const DONE_CARD_MS = 6_000;

/** Failed reads in a row before a card says the engine cannot be reached. */
export const UNREACHABLE_AFTER = 4;

/** One job slot: a kind of job about one subject (a change, a product, a model). */
export interface JobRecord {
  key: string;
  kind: JobKind;
  /** What the job is, in words: "Apply change", "Run the project". */
  label: string;
  /** The page the job was started from, so its card can lead back. */
  from: string;
  view: JobView;
  cancel: CancelView;
  /** A stop was delivered to this job, so Cancel now offers a forced stop. */
  stopping: boolean;
  /**
   * How many terminates the engine delivered: 1 is the stop, 2 the forced
   * stop. After the second the engine kills the job if it has not exited
   * 15 s later, and one more press kills it at once.
   */
  terminates: number;
  /** The client the job was submitted with: its reads and cancel go there too. */
  client: JobClient | null;
  /** What the job was submitted with, so a page that comes back can show what it was for. */
  body: Record<string, unknown> | null;
  /** When this page submitted it, in ms since the epoch. */
  submittedAt: number;
  /** Closed in the tray (by hand, or a done card's timer). The page still reads it. */
  closed: boolean;
  /**
   * The job ended and every page that showed it has left. A page that opens
   * now starts clean: an old result is not read as the current state.
   */
  forgotten: boolean;
  /** Status reads that failed in a row because the engine could not be reached. */
  failedReads: number;
}

/** The record a key has before any job was started for it. */
export function idleRecord(key: string, kind: JobKind, label: string): JobRecord {
  return {
    key,
    kind,
    label,
    from: "",
    view: { kind: "idle" },
    cancel: { kind: "idle" },
    stopping: false,
    terminates: 0,
    client: null,
    body: null,
    submittedAt: 0,
    closed: true,
    forgotten: false,
    failedReads: 0,
  };
}

/** Whether a record's job has ended, one way or another. */
export function settled(record: JobRecord): boolean {
  const kind = record.view.kind;
  return kind !== "submitting" && kind !== "running";
}

/** The slot a job of `kind` about `subject` lives in. */
export function jobKey(kind: JobKind, subject = ""): string {
  return subject === "" ? kind : `${kind}:${subject}`;
}

/** Whether a read failure means the job cannot be read again. */
function unreadable(error: unknown): boolean {
  return error instanceof ApiError && (error.status === 401 || error.status === 403 || error.status === 404);
}

function asError(error: unknown): ApiError | Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Every job this tab started, by key. It submits, reads each running job
 * until it ends, and cancels. A page reads its slot through `useJob`; the
 * tray reads them all. The shell keeps one store for the whole tab, so a job
 * outlives the page that started it; a page with no shell around it (a unit
 * test) gets a private store that ends with it.
 *
 * Guarantees, per key:
 * - One job in flight: a second start while one is submitting or running
 *   is ignored, even when two clicks land before React re-renders.
 * - `onDone` listeners run once, with the terminal record, and only while
 *   they are registered (a page that left is not called).
 * - A late cancel answer for an earlier job never marks a newer one.
 * - A read that fails because the engine is unreachable is tried again,
 *   with a growing wait; one that cannot succeed (401, 403, 404) marks the
 *   job lost. A read failure is never shown as a failed job.
 *
 * How long a page sees a job: a page that is open when the job ends, or
 * that opens before anyone saw the end, shows it. Once the job has ended
 * and the last page showing it leaves, it is forgotten for pages: the next
 * visit starts clean. The tray card keeps its own life (closed by hand, or
 * by its timer).
 */
export class JobStore {
  private records = new Map<string, JobRecord>();
  private listeners = new Set<() => void>();
  private doneListeners = new Map<string, Set<(job: JobStatus) => void>>();
  private closeTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private mounts = new Map<string, number>();
  private heldBy = new Map<string, string>();
  private disposed = false;
  private list: JobRecord[] = [];

  constructor(
    private readonly pollMs: number = JOB_POLL_MS,
    private readonly doneCardMs: number = DONE_CARD_MS,
  ) {}

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  get(key: string): JobRecord | undefined {
    return this.records.get(key);
  }

  /** Every record, oldest first. The same array until something changes. */
  all = (): JobRecord[] => this.list;

  /**
   * A page shows `key`. Returns the detach: when the last page leaves and
   * the job has ended, the job is forgotten for pages.
   */
  attach(key: string, heldBy?: string): () => void {
    this.mounts.set(key, (this.mounts.get(key) ?? 0) + 1);
    if (heldBy !== undefined) this.heldBy.set(key, heldBy);
    return () => {
      const left = (this.mounts.get(key) ?? 1) - 1;
      if (left > 0) {
        this.mounts.set(key, left);
        return;
      }
      this.mounts.delete(key);
      // Decide after the commit: a page that remounts at once (a keyed
      // refresh, StrictMode in development) attaches again before this runs.
      queueMicrotask(() => this.forgetIfLeft(key));
    };
  }

  /**
   * Forget `key` for pages if no page shows it and its job has ended. A key
   * held by another (an optimize plan, by its apply) waits for that one:
   * the plan is what the page draws the apply under.
   */
  private forgetIfLeft(key: string): void {
    if (this.disposed || (this.mounts.get(key) ?? 0) > 0) return;
    const record = this.records.get(key);
    if (record === undefined || !settled(record) || record.view.kind === "idle" || record.forgotten) return;
    const holder = this.heldBy.get(key);
    const held = holder === undefined ? undefined : this.records.get(holder);
    if (held !== undefined && held.view.kind !== "idle" && !held.forgotten) return;
    this.put({ ...record, forgotten: true });
    // Anything this key was holding can go now too.
    for (const [other, by] of this.heldBy) {
      if (by === key) this.forgetIfLeft(other);
    }
  }

  /** Run `listener` once when the job in `key` ends. Returns the unsubscribe. */
  onDone(key: string, listener: (job: JobStatus) => void): () => void {
    let set = this.doneListeners.get(key);
    if (set === undefined) {
      set = new Set();
      this.doneListeners.set(key, set);
    }
    set.add(listener);
    return () => {
      set.delete(listener);
    };
  }

  /** Undo `dispose` (StrictMode unmounts and mounts again in development). */
  revive(): void {
    this.disposed = false;
  }

  /** Stop every read and timer. A private store ends with its page. */
  dispose(): void {
    this.disposed = true;
    for (const timer of this.closeTimers.values()) clearTimeout(timer);
    this.closeTimers.clear();
  }

  private put(record: JobRecord): void {
    this.records.set(record.key, record);
    this.list = [...this.records.values()];
    for (const listener of [...this.listeners]) listener();
  }

  private update(key: string, change: (record: JobRecord) => JobRecord): void {
    const record = this.records.get(key);
    if (record === undefined) return;
    this.put(change(record));
  }

  /** Close the card after its time, unless the person closed it first. */
  private closeLater(key: string): void {
    this.closeTimers.set(
      key,
      setTimeout(() => this.close(key), this.doneCardMs),
    );
  }

  /** Hide a card from the tray. The page keeps reading the record. */
  close(key: string): void {
    const timer = this.closeTimers.get(key);
    if (timer !== undefined) clearTimeout(timer);
    this.closeTimers.delete(key);
    this.update(key, (record) => ({ ...record, closed: true }));
  }

  start(
    key: string,
    kind: JobKind,
    label: string,
    client: JobClient,
    body: Record<string, unknown> = {},
  ): void {
    if (this.disposed) return;
    const current = this.records.get(key)?.view.kind;
    if (current === "submitting" || current === "running") return;
    const timer = this.closeTimers.get(key);
    if (timer !== undefined) clearTimeout(timer);
    this.closeTimers.delete(key);
    this.put({
      key,
      kind,
      label,
      from: typeof window === "undefined" ? "" : window.location.pathname,
      view: { kind: "submitting" },
      cancel: { kind: "idle" },
      stopping: false,
      terminates: 0,
      client,
      body,
      submittedAt: Date.now(),
      closed: false,
      forgotten: false,
      failedReads: 0,
    });
    client
      .submit(kind, body)
      .then(({ job_id }) => {
        if (this.disposed) return;
        this.update(key, (record) => ({ ...record, view: { kind: "running", jobId: job_id } }));
        this.follow(key, job_id, client);
      })
      .catch((error: unknown) => {
        if (this.disposed) return;
        const refused = asError(error);
        // Only a busy project is worth a card: it names the job that holds
        // it. Any other refusal is said beside the button that was pressed.
        const busy = refused instanceof ApiError && refused.envelope.code === "mutation_in_progress";
        this.update(key, (record) => ({ ...record, view: { kind: "refused", error: refused }, closed: !busy }));
        if (busy) this.closeLater(key);
      });
  }

  private follow(key: string, jobId: string, client: JobClient): void {
    let wait = this.pollMs;
    const tick = () => {
      if (this.disposed) return;
      // Not the job in this slot any more (a newer start replaced it).
      const shown = this.records.get(key)?.view;
      if (shown?.kind !== "running" || shown.jobId !== jobId) return;
      client
        .status(jobId)
        .then((job) => {
          if (this.disposed) return;
          wait = this.pollMs;
          if (this.records.get(key)?.failedReads !== 0) {
            this.update(key, (record) => ({ ...record, failedReads: 0 }));
          }
          if (job.state === "running" || job.state === "queued") {
            setTimeout(tick, wait);
            return;
          }
          this.update(key, (record) => ({ ...record, view: { kind: "done", job }, cancel: { kind: "idle" } }));
          if (job.state === "succeeded") this.closeLater(key);
          for (const listener of [...(this.doneListeners.get(key) ?? [])]) listener(job);
        })
        .catch((error: unknown) => {
          if (this.disposed) return;
          if (unreadable(error)) {
            this.update(key, (record) => ({
              ...record,
              view: { kind: "lost", jobId, error: asError(error) },
              cancel: { kind: "idle" },
            }));
            return;
          }
          this.update(key, (record) => ({ ...record, failedReads: record.failedReads + 1 }));
          wait = Math.min(wait * 2, JOB_POLL_MAX_MS);
          setTimeout(tick, wait);
        });
    };
    tick();
  }

  /**
   * Cancel the job in `key`. The job keeps being read: its final state
   * (`cancelled`, or `succeeded` if it finished first) is what shows in the end.
   */
  cancel(key: string): void {
    const record = this.records.get(key);
    if (record === undefined || record.view.kind !== "running" || record.cancel.kind === "sending") return;
    const client = record.client;
    if (client === null) return;
    const jobId = record.view.jobId;
    const current = () => {
      const view = this.records.get(key)?.view;
      return !this.disposed && view?.kind === "running" && view.jobId === jobId;
    };
    this.update(key, (r) => ({ ...r, cancel: { kind: "sending" } }));
    client
      .cancel(jobId)
      .then(({ signal }) => {
        if (!current()) return;
        // This counts the terminates whose answers arrived, not the engine's
        // own count. A lost answer (a timeout, a dropped connection) can leave
        // the engine one step ahead of the card. Not reconciled yet.
        this.update(key, (r) => ({
          ...r,
          stopping: r.stopping || signal === "terminate",
          terminates: r.terminates + (signal === "terminate" ? 1 : 0),
          cancel: { kind: "sent", signal },
        }));
      })
      .catch((error: unknown) => {
        if (!current()) return;
        this.update(key, (r) => ({ ...r, cancel: { kind: "refused", error: asError(error) } }));
      });
  }
}
