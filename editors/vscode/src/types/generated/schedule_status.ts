/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/schedule_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Why a demand is currently suppressed.
 */
export type ScheduleThrottleKind = "failure_backoff" | "partial_backoff";
/**
 * Tick-lock states.
 *
 * **`free` is the normal steady state.** The lock is held only for the duration of a tick, and a tick is short relative to the poll interval, so a perfectly healthy scheduler reports `free` on almost every request. It does not mean "no scheduler is running" — for that, compare `last_evaluated_at` against the schedule's cadence.
 */
export type TickLockState = "never" | "free" | "held" | "wedged";

/**
 * A read-only snapshot of the scheduler: what is scheduled, when each pipeline last evaluated and fired, when it is next expected to, what is claimed in flight, and whether a reconciler holds the tick lock.
 *
 * API-only (`GET /api/v1/schedule`), so it carries no `version`/`command` preamble — it mirrors [`MetaOutput`] and `JobStatus`, not the CLI-backed [`TickOutput`].
 *
 * This is a **status read, not an evaluation**: it reports stored cursors, claims and lock state without deciding what is due. `rocky tick --dry-run` is the evaluation. Keeping the two apart makes this endpoint side-effect free and `O(pipelines)` rather than `O(runs × pipelines)`.
 */
export interface ScheduleStatusOutput {
  /**
   * Roll-up counts across `pipelines`.
   */
  counts: ScheduleStatusCounts;
  /**
   * The instant this snapshot was taken. Every projection below is relative to it.
   */
  now: string;
  /**
   * Every schedulable pipeline declaring a `[schedule]`, sorted by name. Load pipelines are excluded — they are not schedulable (a scheduled load would re-ingest every discovered file), so reporting one as scheduled would promise a fire that can never happen.
   */
  pipelines: SchedulePipelineStatus[];
  /**
   * Whether a reconciler currently holds the tick lock.
   */
  tick_lock: ScheduleLockStatus;
  /**
   * The project timezone cron occurrences resolve in.
   */
  timezone: string;
  [k: string]: unknown;
}
/**
 * Roll-up counts over the reported pipelines.
 */
export interface ScheduleStatusCounts {
  /**
   * How many declare a schedule that cannot be resolved.
   */
  config_errors: number;
  /**
   * Of those, how many are enabled.
   */
  enabled: number;
  /**
   * How many have a `submitted` claim — a run believed to be in flight.
   */
  in_flight: number;
  /**
   * How many have a `next_fire_at` in the past (overdue).
   */
  overdue: number;
  /**
   * Pipelines declaring a schedule (the length of `pipelines`).
   */
  scheduled: number;
  /**
   * How many are currently inside a backoff window.
   */
  throttled: number;
  [k: string]: unknown;
}
/**
 * One scheduled pipeline's status.
 */
export interface SchedulePipelineStatus {
  /**
   * Upstream pipelines driving the `after` trigger.
   */
  after?: string[];
  /**
   * True when no tick has recorded a cron anchor yet. The first tick records the anchor *without* firing, so `next_fire_at` is not yet projectable — which is a different thing from "this will never fire".
   */
  awaiting_first_anchor: boolean;
  /**
   * Catch-up policy: `latest` (fire the newest missed occurrence) or `skip` (jump the anchor forward, firing none of them).
   */
  catchup: string;
  /**
   * Claims recorded for this pipeline, newest occurrence first.
   */
  claims?: ScheduleClaimStatus[];
  /**
   * The config error making this pipeline unschedulable, when its schedule cannot be resolved. The reconciler fails closed and never fires it, so this is why an otherwise-configured pipeline is silent.
   */
  config_error?: string | null;
  /**
   * Consecutive failures driving the backoff ladder.
   */
  consecutive_failures: number;
  /**
   * The cron expression, when one is configured.
   */
  cron?: string | null;
  /**
   * False when the schedule declares `enabled = false`.
   */
  enabled: boolean;
  /**
   * The resolved freshness budget in seconds, when that trigger is active.
   */
  freshness_budget_seconds?: number | null;
  /**
   * When the last attempt started.
   */
  last_attempt_at?: string | null;
  /**
   * The last attempt's terminal outcome (`success` | `partial` | `failure`).
   */
  last_attempt_outcome?: string | null;
  /**
   * When the reconciler last evaluated this pipeline's demand. Staleness here — not the tick lock — is what indicates a dead timer.
   */
  last_evaluated_at?: string | null;
  /**
   * The logical timestamp of the last occurrence that fired.
   */
  last_fire_logical_ts?: string | null;
  /**
   * When the cron is next expected to fire.
   *
   * **A timestamp in the past means overdue.** The projection is anchored on the stored cursor, not on the clock, so a stalled timer reports the slot it missed rather than a healthy-looking future one. Null for `after`/`freshness` schedules, which have no projectable instant.
   */
  next_fire_at?: string | null;
  /**
   * The pipeline name.
   */
  pipeline: string;
  /**
   * The active backoff window, when the pipeline is throttled.
   */
  throttle?: ScheduleThrottleStatus | null;
  [k: string]: unknown;
}
/**
 * One claim recorded against a pipeline.
 */
export interface ScheduleClaimStatus {
  /**
   * Attempts made against this claim.
   */
  attempts: number;
  /**
   * The occurrence this claim belongs to. Null for a webhook demand, whose claim key carries a minted id rather than an instant.
   */
  logical_ts?: string | null;
  /**
   * The terminal outcome, when the claim is `completed`.
   */
  outcome?: string | null;
  /**
   * The demand source (`cron` | `after` | `freshness` | `webhook`).
   */
  source: string;
  /**
   * Claim state (`submitted` | `released` | `completed`).
   */
  state: string;
  /**
   * The submission id — the join key to `GET /api/v1/jobs/{id}` and to run history.
   */
  submission_id: string;
  [k: string]: unknown;
}
/**
 * An active backoff window.
 */
export interface ScheduleThrottleStatus {
  /**
   * Which backoff is suppressing the demand.
   */
  kind: ScheduleThrottleKind;
  /**
   * When the demand becomes eligible again.
   */
  resume_at: string;
  [k: string]: unknown;
}
/**
 * The tick lock's state.
 */
export interface ScheduleLockStatus {
  /**
   * Age of the lock's heartbeat in seconds, when one is readable.
   */
  heartbeat_age_seconds?: number | null;
  /**
   * Lock state. Note `free` is the normal steady state — see [`TickLockState`].
   */
  state: TickLockState;
  [k: string]: unknown;
}
