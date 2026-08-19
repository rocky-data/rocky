/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/fulfill.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky fulfill <product>` — the reconciler's stop report: how far the loop advanced, why it stopped, and the exact next command when one exists.
 */
export interface FulfillOutput {
  /**
   * Always `"fulfill"`.
   */
  command: string;
  /**
   * Why the loop stopped, in plain language.
   */
  message: string;
  /**
   * The exact command that unblocks it, when one exists.
   */
  next_command?: string | null;
  /**
   * The pinned plan, while one is in flight.
   */
  plan_id?: string | null;
  /**
   * The product name the loop was asked to drive.
   */
  product: string;
  /**
   * `product:<name>`.
   */
  product_id: string;
  /**
   * The approved spec digest the state was reached under, if any.
   */
  spec_digest?: string | null;
  /**
   * The state the record was left in (its wire tag, e.g. `"needs_input"`, `"proposed"`, `"observing"`, `"blocked"`).
   */
  state: string;
  version: string;
  [k: string]: unknown;
}
