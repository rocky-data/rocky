/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_clear_breach.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product clear-breach`.
 */
export interface ProductClearBreachOutput {
  /**
   * The marker that was deleted, when one was open.
   */
  cleared?: ProductVolumeBreachStatus | null;
  command: string;
  /**
   * The output model whose breach marker was cleared.
   */
  model: string;
  /**
   * The product name.
   */
  product: string;
  version: string;
  /**
   * True when a marker was open and was deleted. False when none was open, so a second call changes nothing.
   */
  was_open: boolean;
  [k: string]: unknown;
}
/**
 * An open volume breach: the output model stays failed in every run until it is rebuilt or the breach is cleared (#2581).
 */
export interface ProductVolumeBreachStatus {
  /**
   * The declared `max_rows` at the newest breach.
   */
  max_rows?: number | null;
  /**
   * The declared `min_rows` at the newest breach.
   */
  min_rows?: number | null;
  /**
   * The output model held failed.
   */
  model: string;
  /**
   * When the breach was first recorded.
   */
  opened_at: string;
  /**
   * For a `time_interval` output, the partition keys still breached. Empty means the whole output.
   */
  partitions?: string[];
  /**
   * The product that declares the range.
   */
  product: string;
  /**
   * The newest breach, in words.
   */
  reason?: string;
  /**
   * The row count of the newest breach, when it was counted.
   */
  rows?: number | null;
  /**
   * The run that recorded it, when known.
   */
  run_id?: string | null;
  [k: string]: unknown;
}
