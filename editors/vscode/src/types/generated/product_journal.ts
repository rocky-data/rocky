/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_journal.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product journal <name>`.
 *
 * The product's fulfillment journal, whole and in append order, read through the same store function the fulfillment loop reads through. A known product with no rows is an empty journal; a product the project does not know (no spec file, no store record) is a refusal, never an empty journal.
 */
export interface ProductJournalOutput {
  command: string;
  /**
   * `rows.len()`, repeated so a consumer can assert it received the whole journal.
   */
  count: number;
  /**
   * The product name as given.
   */
  product: string;
  /**
   * `product:<name>`, the store key every record of the product hangs off.
   */
  product_id: string;
  /**
   * Every journal row, in append order.
   */
  rows: ProductJournalEntry[];
  version: string;
  [k: string]: unknown;
}
/**
 * One persisted fulfillment journal row, as `rocky product journal` prints it: the loop's own record of a transition, field for field.
 */
export interface ProductJournalEntry {
  /**
   * RFC 3339 instant of the transition, when recorded.
   */
  at?: string | null;
  /**
   * What happened, in the loop's plain language (`spec approved`, …). A label to render, not an enum to switch on.
   */
  event: string;
  /**
   * The state tag before the transition, when one existed.
   */
  from_state?: string | null;
  /**
   * The idempotency key pinned by this event, when it pinned one.
   */
  idempotency_key?: string | null;
  /**
   * The plan involved, when the event concerns one (propose, apply).
   */
  plan_id?: string | null;
  /**
   * Sequence number, also encoded in the row's store key.
   */
  seq: number;
  /**
   * The spec digest involved, when the event concerns one.
   */
  spec_digest?: string | null;
  /**
   * The state tag after the transition.
   */
  to_state: string;
  [k: string]: unknown;
}
