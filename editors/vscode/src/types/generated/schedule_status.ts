/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/schedule_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * What a duration estimate timed.
 */
export type EstimateBasis = "model_delivery" | "pipeline_run";
/**
 * Whether a duration estimate has enough runs to judge from.
 */
export type EstimateState = "known" | "unknown";
/**
 * How a pipeline's cron compares with its latest start.
 *
 * Checked in this order; the first that applies wins.
 */
export type CronCheck = "disabled" | "overdue" | "not_judged" | "on_time" | "too_late" | "relies_on_after";
/**
 * Why a latest start is `unknown`.
 */
export type LatestStartUnknownReason =
  "missing_estimate" | "no_next_deadline" | "models_unreadable" | "cycle" | "overflow";
/**
 * Whether a latest start could be derived.
 */
export type LatestStartState = "known" | "unknown";
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
   * Delivery state of the most urgent product deadline this pipeline delivers: a product whose spec declares `deliver_by` and whose output model the pipeline builds. For a transformation pipeline, that is a non-ephemeral model its `models` glob selects, with the same root, models directory and glob as `rocky run`. The models are loaded, not compiled, so a compile error is not seen. When its models directory is absent or cannot be located, or a model fails to load, `rocky run` builds nothing, so the pipeline has no `delivery` (see `latest_start`); `rocky product status` still shows that product's last delivery. For any other pipeline type, it is any such model the pipeline's newest production run that built one built, within the newest 500 runs. The estimate times the whole pipeline run. Absent when no source shows the pipeline building a product with a deadline.
   */
  delivery?: DeliveryStatus | null;
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
   * The latest time this pipeline can start and still meet every product deadline it feeds, directly or through pipelines that run `after` it. Present on each pipeline that has a `delivery`; on each transformation pipeline whose models cannot all be read, whenever any product declares `deliver_by` (then `unknown`, reason `models_unreadable`, with a `warning`: neither config nor run history can show that it builds no deadlined output model); and on every pipeline any of those runs `after`, directly or through others. Absent otherwise. A deadline only derives this value: it never adds an `after` edge and never moves a cron.
   */
  latest_start?: LatestStartStatus | null;
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
 * Delivery state of one product deadline: was the latest delivery on time, and is the next one at risk.
 *
 * Reported by `rocky product status` and, per pipeline, by the scheduler status. `on_time` and `late_by_seconds` are facts about the latest delivery. `at_risk` rests on [`DurationEstimateStatus`], and is `null` whenever the estimate is `unknown`.
 */
export interface DeliveryStatus {
  /**
   * True when the p90 duration is longer than the time left before `next_due_at`. `null` when the estimate is `unknown`: no verdict.
   */
  at_risk?: boolean | null;
  /**
   * The product's `deliver_by` cron expression.
   */
  deliver_by: string;
  /**
   * The duration estimate `at_risk` rests on.
   */
  estimate: DurationEstimateStatus;
  /**
   * When the latest production delivery finished.
   */
  last_delivered_at?: string | null;
  /**
   * The deadline the latest delivery served, when its run recorded one.
   */
  last_due_at?: string | null;
  /**
   * How late the latest delivery was, in seconds; `0` when on time. `null` when no delivery recorded a deadline.
   */
  late_by_seconds?: number | null;
  /**
   * The deadline the next delivery must meet. A time in the past means the next delivery is already late.
   */
  next_due_at?: string | null;
  /**
   * Whether the latest delivery met its deadline. `null` when no delivery recorded a deadline.
   */
  on_time?: boolean | null;
  /**
   * The model the product delivers.
   */
  output_model: string;
  /**
   * The product whose deadline this is.
   */
  product: string;
  /**
   * The timezone `deliver_by` is read in.
   */
  timezone: string;
  [k: string]: unknown;
}
/**
 * A p50/p90 duration estimate and the sample behind it.
 */
export interface DurationEstimateStatus {
  /**
   * What was timed: `model_delivery` (run start to the model's finish) or `pipeline_run` (run start to run finish).
   */
  basis: EstimateBasis;
  /**
   * True when the newest 5 runs were all slower than the p90 of the older ones, so `p90_ms` is the slowest of those 5.
   */
  growth: boolean;
  /**
   * How many production runs the estimate read.
   */
  n: number;
  /**
   * Median duration in milliseconds, when `known`.
   */
  p50_ms?: number | null;
  /**
   * 90th-percentile duration in milliseconds, when `known`.
   */
  p90_ms?: number | null;
  /**
   * `known` with at least 5 runs; `unknown` below that, and then no verdict rests on the estimate.
   */
  state: EstimateState;
  /**
   * The most runs it reads (the newest 20).
   */
  window: number;
  [k: string]: unknown;
}
/**
 * The latest start of a scheduled pipeline, derived from the product deadlines it feeds.
 *
 * `latest_start(P) = min(next_due_at of a deadline P serves, latest_start of each pipeline that runs after P and feeds one) - p90(P)`. The p90 is the pipeline's run time over its last 20 successful production runs. Any unknown term on the way makes the answer `unknown`, with `reasons`.
 */
export interface LatestStartStatus {
  /**
   * The latest start, when `known`.
   */
  at?: string | null;
  /**
   * How the pipeline's own cron compares with `at`. See [`CronCheck`].
   */
  cron_check: CronCheck;
  /**
   * True in two cases, both about the pipeline's own cron. `cron_check` is `too_late` and the pipeline has a cron: the next cron fire (`next_fire_at`) is later than `at`. Or `cron_check` is `overdue`, `at` is known, and `now` is already later than `at`. False otherwise: also when `state` is `unknown`, and always for a pipeline with no cron, even when its `cron_check` is `too_late` (it has no cron to be late; its `after` trigger is what may start it late). So alert on `warning`, not on this field alone. A fire exactly at `at` is in time. Rocky warns only; it never changes the cron.
   */
  cron_too_late: boolean;
  /**
   * That product's next deadline, when `known`.
   */
  due_at?: string | null;
  /**
   * The pipelines on the way whose p90 is unknown (fewer than 5 successful production runs). Present only with the reason `missing_estimate`.
   */
  missing_estimate?: string[];
  /**
   * When `cron_check` is `relies_on_after`: the `after` trigger is what starts the pipeline in time. For a pipeline with a cron, the cron alone would start it too late; for one with no cron, the `after` trigger is its only trigger. Also when a pipeline with no cron reads `not_judged` because no `after` upstream is known late but some cannot be judged: the note names those upstreams.
   */
  note?: string | null;
  /**
   * The product whose deadline gives the minimum, when `known`.
   */
  product?: string | null;
  /**
   * Why the latest start is `unknown`: every reason found on the way, sorted, each once. Empty (and omitted) when `known`.
   */
  reasons?: LatestStartUnknownReason[];
  /**
   * `known` when every term on every chain to a deadline is known; `unknown` otherwise, and then `at` is absent and `reasons` says why.
   */
  state: LatestStartState;
  /**
   * Why the schedule may miss the deadline: when `cron_check` is `too_late`, `overdue` or `disabled`, or when the pipeline's models cannot all be read (reason `models_unreadable`). That warning names the products whose output model config or run history shows it building, when there are any.
   */
  warning?: string | null;
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
