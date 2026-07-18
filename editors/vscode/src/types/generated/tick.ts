/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/tick.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky tick` — one demand-reconciliation pass.
 *
 * A tick evaluates every scheduled pipeline's standing demand once, runs what is due (sequentially, via child `rocky run` processes), and records the outcome. This payload is the machine surface: an orchestration wrapper (a systemd timer, cron, CI) reads it to see what ran, what was suppressed, and why.
 *
 * **Honesty note:** exit code `0` does NOT mean the estate is healthy. A pipeline sitting in `failure_backoff`/`partial_backoff` produces quiet exit-`0` ticks after the tick that first observed its failure. Exit codes alert on *this tick's* work; ongoing sickness is carried by the `skipped` reasons + `counts` here and the `consecutive_failures` metric. Alert on those, not on exit codes alone.
 *
 * The feature is **experimental** while the native reconciler soaks.
 */
export interface TickOutput {
  command: string;
  /**
   * Roll-up counts across `evaluated`/`executed`/`skipped`.
   */
  counts: TickCounts;
  /**
   * True when invoked with `--dry-run`: demand was evaluated and reported, but nothing executed and no state was written.
   */
  dry_run: boolean;
  /**
   * Wall-clock duration of the whole tick, including child run time.
   */
  duration_ms: number;
  /**
   * Per-pipeline evaluation: the demand sources considered, whether the pipeline was due, and the per-source reason.
   */
  evaluated: PipelineDemandStatus[];
  /**
   * Demands that ran to a terminal outcome this tick, in execution order.
   */
  executed: ExecutedRunOutput[];
  /**
   * True when this tick proceeded via the wedge override — it found the tick lock held but its heartbeat stale past the takeover window, so it ran without the lock and leaned on the claim state machine for correctness. Surfaced loudly because it signals a wedged prior reconciler.
   */
  lock_overridden: boolean;
  /**
   * The instant the tick evaluated demand against (`--now`, or the wall clock). Echoed for determinism and audit — occurrence math, `after`, and freshness are all resolved as of this instant.
   */
  now: string;
  /**
   * Demands suppressed this tick, each with a stable reason string.
   */
  skipped: SkippedDemandOutput[];
  /**
   * True when the whole tick was skipped because a live reconciler already held the tick lock (a `tick_in_progress` skip is also emitted in `skipped`). The CLI still exits `0`.
   */
  tick_in_progress: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * Roll-up counts for a tick.
 */
export interface TickCounts {
  /**
   * Pipelines found due.
   */
  due: number;
  /**
   * Pipelines evaluated this tick.
   */
  evaluated: number;
  /**
   * Demands executed to a terminal outcome.
   */
  executed: number;
  /**
   * Executed runs that failed (exit 1/other).
   */
  failed: number;
  /**
   * Executed runs that were partial (exit 2).
   */
  partial: number;
  /**
   * Demands suppressed (the length of `skipped`).
   */
  skipped: number;
  /**
   * Executed runs that succeeded (exit 0).
   */
  succeeded: number;
  [k: string]: unknown;
}
/**
 * One pipeline's demand evaluation this tick.
 */
export interface PipelineDemandStatus {
  /**
   * Whether any source made the pipeline due this tick.
   */
  due: boolean;
  /**
   * The source that made it due (`cron` | `after` | `freshness` | `webhook`), when `due` is true.
   */
  due_source?: string | null;
  /**
   * The logical timestamp of the due demand (the cron occurrence, the max upstream-success time, or the staleness-breach detection time), when `due` is true.
   */
  logical_ts?: string | null;
  /**
   * The pipeline name.
   */
  pipeline: string;
  /**
   * Per-source outcome for every source considered (the due source included, with reason `due`).
   */
  sources: SourceEvaluation[];
  [k: string]: unknown;
}
/**
 * One demand source's outcome within a pipeline's evaluation.
 */
export interface SourceEvaluation {
  /**
   * Number of missed cron occurrences — present for `catchup_skipped`.
   */
  missed?: number | null;
  /**
   * The reason string for this source: `due` | `not_due` | `disabled` | `catchup_skipped` | `failure_backoff` | `partial_backoff` | `superseded` | `history_unavailable`.
   */
  reason: string;
  /**
   * When the source becomes eligible again — present for `failure_backoff` and `partial_backoff`.
   */
  resume_at?: string | null;
  /**
   * The demand source: `cron` | `after` | `freshness` | `webhook`.
   */
  source: string;
  [k: string]: unknown;
}
/**
 * One demand that executed to a terminal outcome this tick.
 */
export interface ExecutedRunOutput {
  /**
   * Total submissions made for this demand (the claim's monotonic audit counter): `1` unless in-tick retries fired.
   */
  attempts: number;
  /**
   * The child's exit code: `0` success, `2` partial, `1`/other failure.
   */
  exit_code: number;
  /**
   * The logical timestamp of the demand.
   */
  logical_ts: string;
  /**
   * The mapped terminal outcome: `success` | `partial` | `failure`.
   */
  outcome: string;
  /**
   * The pipeline that ran.
   */
  pipeline: string;
  /**
   * The source that made it due: `cron` | `after` | `freshness` | `webhook`.
   */
  source: string;
  /**
   * The submission id of the recorded attempt — the precise join key between this demand and *its* run record (`rocky history` shows the same id). This is the reconciler's demand↔run identity; there is no separate `run_id` field because a run is joined to its demand by `submission_id`.
   */
  submission_id: string;
  [k: string]: unknown;
}
/**
 * One demand suppressed this tick.
 */
export interface SkippedDemandOutput {
  /**
   * Number of missed cron occurrences — present for `catchup_skipped`.
   */
  missed?: number | null;
  /**
   * The pipeline the skip belongs to. `None` for a whole-tick skip (`tick_in_progress`, `state_busy`), which is not tied to a single pipeline.
   */
  pipeline?: string | null;
  /**
   * Why it was skipped: `not_due` | `disabled` | `in_flight` | `tick_in_progress` | `catchup_skipped` | `failure_backoff` | `partial_backoff` | `dedup` | `history_unavailable` | `state_busy`.
   */
  reason: string;
  /**
   * When the demand becomes eligible again — present for `failure_backoff` and `partial_backoff`.
   */
  resume_at?: string | null;
  /**
   * The demand source, when the skip is source-specific: `cron` | `after` | `freshness` | `webhook`. `None` for a pipeline- or tick-level skip.
   */
  source?: string | null;
  [k: string]: unknown;
}
