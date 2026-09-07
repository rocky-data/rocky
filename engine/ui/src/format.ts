/** Rendering helpers. Every function returns text; nothing here builds markup. */

/** What an absent or `null` value renders as (D14ii): a status, never a value. */
export const NOT_RECORDED = "not recorded";

export function orNotRecorded(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return NOT_RECORDED;
  return String(value);
}

/** `45ms`, `1.2s`, `3m 04s`, `2h 05m`. */
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return NOT_RECORDED;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${String(Math.round(seconds % 60)).padStart(2, "0")}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${String(minutes % 60).padStart(2, "0")}m`;
}

/**
 * An RFC 3339 instant as `2026-09-05 08:00:00 UTC`, with how long ago it was
 * when `now` is given. A value that does not parse renders as given: it is
 * the engine's text, and the engine is the authority.
 */
export function formatInstant(iso: string | null | undefined, now?: number): string {
  if (iso === null || iso === undefined || iso === "") return NOT_RECORDED;
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return iso;
  const absolute = new Date(at).toISOString().replace("T", " ").replace(/\.\d+Z$/, " UTC");
  if (now === undefined) return absolute;
  return `${absolute} (${formatAgo(now - at)})`;
}

function formatAgo(deltaMs: number): string {
  if (deltaMs < 0) return "in the future";
  const seconds = Math.round(deltaMs / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} d ago`;
}

/**
 * The first 12 characters of a long id, for a table cell, with an ellipsis so
 * a truncation reads as one. Put the full id in `title` at the call site.
 *
 * Right for an id whose HEAD identifies it — a hex digest, a run id. A
 * compound id whose tail is the distinguishing part wants `elideMiddle`
 * instead: every fulfillment idempotency key for one product begins
 * `product:<name>@`, so twelve leading characters of it distinguish nothing
 * (#1756).
 */
export function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 12)}…` : id;
}

/**
 * Keeps both ends of a compound identifier and elides the middle:
 * `product:revenue_daily@sha256:5b1bf5c@21` renders as `product:re…1bf5c@21`,
 * keeping the digest tail and the sequence number that tell two keys apart.
 *
 * Used where the tail carries the distinguishing part. It reads the string as
 * text and knows nothing about any identifier's grammar, so a key whose shape
 * changes still renders, just with a different slice shown.
 *
 * Returns the input unchanged when eliding would not make it shorter, so a
 * value near the limit never renders LONGER than it is.
 */
export function elideMiddle(value: string, head = 10, tail = 8): string {
  if (head < 0 || tail < 0) return value;
  if (value.length <= head + tail + 1) return value;
  return `${value.slice(0, head)}…${value.slice(value.length - tail)}`;
}
