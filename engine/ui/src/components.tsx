import type { ReactNode } from "react";
import { clipHead, clipMiddle } from "./format";

/** Trust-signal tone, the idiom the VS Code Inspector uses. */
export type Tone = "ok" | "warn" | "risk" | "muted" | "pending";

/**
 * The tone's dot. A muted card has none: it states a fact and asks nothing.
 * The tone used to be a 4px left border; that border needed the neutral
 * edge split into three directional utilities so the dark rule could not
 * reset it (#2123). A dot has no such interaction with the card's border.
 */
export const TONE_DOT: Record<Tone, string | null> = {
  ok: "bg-emerald-500",
  warn: "bg-amber-500",
  risk: "bg-red-500",
  muted: null,
  pending: "bg-zinc-400 dark:bg-zinc-500",
};

/** A risk card also carries a red edge, so a refusal reads before its words do. */
const TONE_BORDER: Record<Tone, string> = {
  ok: "border-zinc-200 dark:border-zinc-800",
  warn: "border-zinc-200 dark:border-zinc-800",
  risk: "border-red-300 dark:border-red-900",
  muted: "border-zinc-200 dark:border-zinc-800",
  pending: "border-zinc-200 dark:border-zinc-800",
};

/** The dot alone, for a line of status outside a card. */
export function ToneDot({ tone }: { tone: Tone }) {
  const dot = TONE_DOT[tone];
  if (dot === null) return null;
  return <span aria-hidden="true" data-tone-dot="" className={`inline-block size-2 shrink-0 rounded-full ${dot}`} />;
}

/**
 * A status card: a label, a value, an optional sub-line, and a tone dot.
 * Every value renders as text: React escapes it, and nothing here uses
 * `dangerouslySetInnerHTML`. That is the whole XSS story for the shell.
 */
export function StatusCard({
  label,
  value,
  tone = "muted",
  sub,
}: {
  label: string;
  value: ReactNode;
  tone?: Tone;
  sub?: ReactNode;
}) {
  return (
    <div
      data-tone={tone}
      className={`rounded-lg border bg-white p-4 dark:bg-zinc-900 ${TONE_BORDER[tone]}`}
    >
      <div className="flex items-center gap-2 text-xs font-medium text-zinc-500 dark:text-zinc-400">
        <ToneDot tone={tone} />
        {/* Labels are written lower case for the screen reader's sake; the
            first letter is raised for the eye only. */}
        <span className="inline-block first-letter:uppercase">{label}</span>
      </div>
      <div className="mt-1 break-words text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        {value}
      </div>
      {sub != null && sub !== "" && (
        <div className="mt-1 text-xs text-zinc-600 dark:text-zinc-400">{sub}</div>
      )}
    </div>
  );
}

/**
 * A neutral button: a read, never a write (`WriteButton` draws writes). One
 * class string, so Refresh and the other reads look alike on every screen.
 */
export const READ_BUTTON =
  "inline-flex h-9 items-center gap-1.5 rounded-md border border-zinc-300 bg-white px-3 text-sm font-medium text-zinc-700 hover:bg-zinc-50 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-orange-500 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-200 dark:hover:bg-zinc-800";

/**
 * The data table's look, shared by every table on the page: a bordered,
 * rounded box that scrolls sideways on its own, padded cells, a header row
 * that reads as a header. One place, so the runs table and the digest's
 * tables cannot drift apart again.
 */
export const TABLE = {
  scroller:
    "overflow-x-auto rounded-lg border border-zinc-200 bg-white focus-visible:outline-2 focus-visible:outline-orange-500 dark:border-zinc-800 dark:bg-zinc-900",
  table: "w-full min-w-max text-left text-sm",
  head: "border-b border-zinc-200 bg-zinc-50 text-xs text-zinc-500 dark:border-zinc-800 dark:bg-zinc-950/40 dark:text-zinc-400",
  th: "px-3 py-2 font-medium",
  row: "border-t border-zinc-100 first:border-t-0 dark:border-zinc-800",
  td: "px-3 py-2 align-top",
} as const;

/**
 * A screen's title row: the name, an optional line under it, and the
 * screen's own controls at the end. It wraps on a phone.
 */
export function ScreenHeader({
  title,
  detail,
  children,
}: {
  title: string;
  detail?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
      <div className="min-w-0">
        <h2 className="text-xl font-semibold tracking-tight text-zinc-900 dark:text-white">{title}</h2>
        {detail != null && (
          <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">{detail}</p>
        )}
      </div>
      {children != null && <div className="flex flex-wrap items-start gap-2">{children}</div>}
    </div>
  );
}

/**
 * A long identifier cut for a cell, with the full value in `title`.
 *
 * The marker is its own element, rendered only when something was cut, and
 * dimmed so it reads as the screen's mark and not the value's text. A value
 * that happens to end in "…" renders that character as plain text, so a
 * reader — and a test — can tell the two apart (#1815). `keepEnds` keeps both
 * ends for a compound id whose tail distinguishes it (#1756).
 */
export function Clip({ value, keepEnds = false }: { value: string; keepEnds?: boolean }) {
  const clipped = keepEnds ? clipMiddle(value) : clipHead(value);
  if (!clipped.clipped) {
    return <span title={value}>{clipped.text}</span>;
  }
  return (
    <span title={value}>
      {clipped.head}
      <span data-clipped="" aria-hidden="true" className="text-zinc-400 dark:text-zinc-500">
        …
      </span>
      {clipped.tail}
    </span>
  );
}

/** What a screen shows when the producer has nothing for it yet. */
export function EmptyState({ title, detail }: { title: string; detail?: ReactNode }) {
  return (
    <div className="rounded-md border border-dashed border-zinc-300 p-6 text-center dark:border-zinc-700">
      <p className="font-medium text-zinc-700 dark:text-zinc-200">{title}</p>
      {detail != null && <p className="mt-1 text-sm text-zinc-500 dark:text-zinc-400">{detail}</p>}
    </div>
  );
}
