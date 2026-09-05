import type { ReactNode } from "react";

/** Trust-signal tone, the idiom the VS Code Inspector uses. */
export type Tone = "ok" | "warn" | "risk" | "muted" | "pending";

const TONE_CLASS: Record<Tone, string> = {
  ok: "border-l-emerald-500",
  warn: "border-l-amber-500",
  risk: "border-l-red-500",
  muted: "border-l-zinc-400",
  pending: "border-l-zinc-400",
};

/**
 * A status card: a label, a value, an optional sub-line, and a tone accent.
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
      className={`rounded-md border border-zinc-200 border-l-4 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900 ${TONE_CLASS[tone]}`}
    >
      <div className="text-[11px] uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
        {label}
      </div>
      <div className="mt-1 break-words text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        {value}
      </div>
      {sub != null && sub !== "" && (
        <div className="mt-0.5 text-xs text-zinc-500 dark:text-zinc-400">{sub}</div>
      )}
    </div>
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
