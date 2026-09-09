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
 * Text cut for display. `clipped` is the fact a marker stands for, kept apart
 * from the text so the marker is rendered from it and never inferred from the
 * characters — a value that happens to contain "…" is not a value that was
 * cut, and before this the two rendered identically (#1815). `<Clip>` in
 * `components.tsx` is the one renderer.
 */
export type Clipped =
  | { clipped: false; text: string }
  | { clipped: true; head: string; tail: string };

/**
 * The characters a reader sees, so a cut never lands inside one. `String`
 * indexes UTF-16 code units, and slicing at one splits a surrogate pair: a
 * slice can end in a lone high surrogate. Grapheme segmentation is used where
 * the runtime has it; code points are the fallback, which still keeps every
 * pair whole.
 */
function characters(value: string): string[] {
  if (typeof Intl !== "undefined" && typeof Intl.Segmenter === "function") {
    const segmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });
    return Array.from(segmenter.segment(value), (part) => part.segment);
  }
  return Array.from(value);
}

/**
 * The first 12 characters of a long id, and whether that cut anything. Put
 * the full id in `title` at the call site — `<Clip>` does.
 *
 * Right for an id whose HEAD identifies it — a hex digest, a run id. A
 * compound id whose tail is the distinguishing part wants `clipMiddle`
 * instead: every fulfillment idempotency key for one product begins
 * `product:<name>@`, so twelve leading characters of it distinguish nothing
 * (#1756).
 */
export function clipHead(id: string, keep = 12): Clipped {
  const chars = characters(id);
  if (chars.length <= keep) return { clipped: false, text: id };
  return { clipped: true, head: chars.slice(0, keep).join(""), tail: "" };
}

/**
 * Keeps both ends of a compound identifier and cuts the middle:
 * `product:revenue_daily@sha256:5b1bf5c@21` renders as `product:re…1bf5c@21`,
 * keeping the digest tail and the sequence number that tell two keys apart.
 *
 * Used where the tail carries the distinguishing part. It reads the string as
 * text and knows nothing about any identifier's grammar, so a key whose shape
 * changes still renders, just with a different slice shown.
 *
 * Returns the input uncut when cutting would not make it shorter, so a value
 * near the limit never renders LONGER than it is.
 */
export function clipMiddle(value: string, head = 10, tail = 8): Clipped {
  if (head < 0 || tail < 0) return { clipped: false, text: value };
  const chars = characters(value);
  if (chars.length <= head + tail + 1) return { clipped: false, text: value };
  return {
    clipped: true,
    head: chars.slice(0, head).join(""),
    tail: chars.slice(chars.length - tail).join(""),
  };
}
