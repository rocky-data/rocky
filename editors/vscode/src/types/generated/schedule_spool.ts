/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/schedule_spool.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state schedule spool`, mirrored by `GET /api/v1/schedule/spool`.
 *
 * The webhook spool is the queue of demands accepted by the ingress but not yet consumed by a tick. [`ScheduleStatusOutput`] is blind to it: that snapshot reports *claims*, which exist only once a tick has picked a demand up, so a demand sitting here appears nowhere in `GET /api/v1/schedule`. This output is the missing half — what has been accepted and is still waiting.
 *
 * **Pending only.** Consumed demands are excluded: `.done` tombstones are the 24h idempotency window for `kind = id`, not outstanding work. Already quarantined files are excluded from `pending` too and counted in `counts.corrupt`.
 *
 * **Fail-closed on an unreadable spool.** A spool directory that is present but cannot be read is an error, never an empty list. That distinction is the whole bug class behind #1710/#1752/#1731: a present-but-unreadable spool that read as empty let every wrapper see a healthy tick while the webhook demand source was silently not firing. An *absent* spool is different and is fine — it means no webhook has ever been accepted here.
 */
export interface ScheduleSpoolOutput {
  command: string;
  /**
   * Roll-up counts over this listing.
   */
  counts: SpoolCounts;
  /**
   * Outstanding demands, oldest first, tie-broken by `demand_uid` so the order is total. Empty when nothing is queued.
   *
   * This is arrival order, which is **not** the order a tick consumes them in: a spool file is named for the blake3 of its `(pipeline, kind, token)` dedup tuple, so the tick's sorted-by-filename walk is deterministic but unrelated to time. Arrival order is what an operator triaging a stuck queue needs.
   */
  pending: SpoolPendingEntry[];
  /**
   * Entries that could not be reported, with the reason for each. Never silently dropped — a demand this command cannot read is still a demand blocking the queue, and staying quiet about it is the #1731 failure one level up.
   */
  skipped: SpoolSkippedEntry[];
  /**
   * The spool directory this listing was read from (`<project>/.rocky/pending-demands`).
   *
   * Surfaced for the same reason [`ScheduleHoldOutput`] names its state file: a `serve --scheduler` rooted at a different project has a different spool, and reading the wrong one must not look like an empty queue.
   */
  spool_path: string;
  version: string;
  [k: string]: unknown;
}
/**
 * Roll-up counts over a spool listing.
 */
export interface SpoolCounts {
  /**
   * Files already quarantined as corrupt by a previous tick. These are not in `pending` or `skipped` — the queue has set them aside — but a non-zero count means demands were accepted and never ran.
   */
  corrupt: number;
  /**
   * Outstanding demands reported in `pending`.
   */
  pending: number;
  /**
   * Entries in `skipped`.
   */
  skipped: number;
  [k: string]: unknown;
}
/**
 * One demand accepted by the webhook ingress and not yet consumed.
 */
export interface SpoolPendingEntry {
  /**
   * blake3 hex of the raw request body.
   */
  body_hash: string;
  /**
   * The uid minted at acceptance. This is the claim-key discriminator and the join key to a claim in [`ScheduleStatusOutput`] once a tick picks the demand up.
   */
  demand_uid: string;
  /**
   * `id` or `body` — which dedup discipline the demand was accepted under.
   */
  kind: string;
  /**
   * The target pipeline. Validated against the config before acceptance, so this always names a pipeline that existed when the demand arrived.
   */
  pipeline: string;
  /**
   * When the ingress accepted the demand.
   */
  received_at: string;
  /**
   * The raw dedup token.
   *
   * For `kind = id` this is **caller-supplied text**, taken verbatim from the delivery header and never interpreted by the engine; for `kind = body` it is a copy of `body_hash`. Present because it is the dedup key an operator needs when a webhook appears stuck.
   *
   * Because a caller controls it, a consumer must render it as inert text. The browser UI does: React escapes by default and no production component uses `dangerouslySetInnerHTML` (`engine/ui/src/components.tsx`), with the pattern in `SamplePanel.test.tsx` pinning a hostile string as text and never as an element.
   */
  token: string;
  [k: string]: unknown;
}
/**
 * A spool entry this listing could not report, and why.
 */
export interface SpoolSkippedEntry {
  /**
   * The underlying detail, for an operator deciding what to do with the file.
   */
  detail: string;
  /**
   * The file name within the spool directory. Only the name, not the full path, which `spool_path` already carries.
   */
  file: string;
  /**
   * Why it was skipped: `unreadable` (the file could not be read), `malformed` (its JSON did not parse), or `bad_timestamp` (its `received_at` is not a valid RFC3339 instant).
   */
  reason: string;
  [k: string]: unknown;
}
