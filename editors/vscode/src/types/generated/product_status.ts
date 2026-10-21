/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_status.schema.json
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
 * What an out-of-range row count does to the run.
 */
export type VolumeSeverity = "error" | "warning";
/**
 * The outcome of one volume check.
 */
export type VolumeVerdict = "in_range" | "below_min" | "above_max" | "not_evaluated";

/**
 * JSON output of `rocky product status`.
 */
export interface ProductStatusOutput {
  /**
   * The approval record, when one exists.
   */
  approval?: ProductApprovalOutput | null;
  /**
   * Byte-verification problems against the committed manifest (empty = every committed artifact matches its recorded hash).
   */
  artifact_problems?: string[];
  command: string;
  /**
   * The committed lowering phase, when a manifest exists.
   */
  committed_phase?: string | null;
  /**
   * The committed manifest's spec digest, when one exists.
   */
  committed_spec_digest?: string | null;
  /**
   * Delivery state, when the spec declares `deliver_by`: whether the latest production delivery was on time, how late, and whether the next delivery is at risk. The estimate times run start to the output model's finish.
   */
  delivery?: DeliveryStatus | null;
  /**
   * The persisted fulfillment state tag, when one exists.
   */
  fulfill_state?: string | null;
  /**
   * Number of fulfillment journal rows recorded for this product.
   */
  journal_rows: number;
  /**
   * The resolved output model, when the spec parses.
   */
  output_model?: string | null;
  /**
   * The product name the status was asked for.
   */
  product: string;
  /**
   * `product:<name>`, when the spec parses.
   */
  product_id?: string | null;
  /**
   * Whether the approval snapshot's bytes still digest to the recorded value. `null` without an approval; `false` is tamper.
   */
  snapshot_intact?: boolean | null;
  /**
   * The working spec's digest, when it parses.
   */
  spec_digest?: string | null;
  /**
   * Why the spec failed to parse, when it did not.
   */
  spec_error?: string | null;
  /**
   * Whether the working spec equals the approved revision. `null` when either side is missing.
   */
  spec_matches_approval?: boolean | null;
  /**
   * Whether `products/<name>.toml` exists and parses.
   */
  spec_present: boolean;
  /**
   * Whether an uncommitted staging journal is pending. Status never mutates; the next compile resolves it.
   */
  staging_journal_present: boolean;
  version: string;
  /**
   * Volume state, when the spec declares `[product.output.volume]`: the declared range and the latest production count judged against it.
   */
  volume?: ProductVolumeStatus | null;
  [k: string]: unknown;
}
/**
 * The approval record, as echoed by compile/status output.
 */
export interface ProductApprovalOutput {
  /**
   * RFC3339 approval instant.
   */
  approved_at?: string | null;
  /**
   * Best-effort approver identity.
   */
  approver: string;
  /**
   * Project-root-relative path of the immutable snapshot file.
   */
  snapshot_path: string;
  /**
   * The approved `sha256:<hex>` spec digest.
   */
  spec_digest: string;
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
 * The declared volume range and the latest production count.
 */
export interface ProductVolumeStatus {
  /**
   * When that build finished.
   */
  counted_at?: string | null;
  /**
   * The declared `max_rows` (inclusive), when set.
   */
  max_rows?: number | null;
  /**
   * The declared `min_rows` (inclusive), when set.
   */
  min_rows?: number | null;
  /**
   * The newest production build that recorded a count. Absent when no build has been counted yet.
   */
  row_count?: number | null;
  /**
   * The run that took the count.
   */
  run_id?: string | null;
  /**
   * The declared severity: `warning` (the default) or `error`.
   */
  severity: VolumeSeverity;
  /**
   * That count judged against the range the spec declares now: `in_range`, `below_min`, `above_max`, or `not_evaluated` when no count exists.
   */
  verdict: VolumeVerdict;
  [k: string]: unknown;
}
