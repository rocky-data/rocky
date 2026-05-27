import type { ReactNode } from "react";
import type { ColumnTestStatus } from "./viewModel";

const STATUS_COLOR: Record<Exclude<ColumnTestStatus, "none">, string> = {
  pass: "var(--vscode-testing-iconPassed)",
  warn: "var(--vscode-testing-iconQueued)",
  fail: "var(--vscode-testing-iconFailed)",
};

/** Trust-signal tone → a theme chart colour, used as the card's accent. */
export type Tone = "ok" | "warn" | "risk" | "muted" | "pending";

const TONE_COLOR: Record<Tone, string> = {
  ok: "var(--vscode-charts-green)",
  warn: "var(--vscode-charts-yellow)",
  risk: "var(--vscode-charts-red)",
  muted: "var(--vscode-descriptionForeground)",
  pending: "var(--vscode-descriptionForeground)",
};

/**
 * A trust-plane status card for the Overview dashboard: a label, a value, an
 * optional sub-line, and a tone accent (left border + label dot). `hero` gives
 * the value extra weight for the headline signals (cost, blast radius). `title`
 * carries an explain-on-hover tooltip. Themed to `--vscode-*`.
 */
export function StatusCard({
  label,
  value,
  tone = "muted",
  sub,
  hero = false,
  title,
}: {
  label: string;
  value: ReactNode;
  tone?: Tone;
  sub?: ReactNode;
  hero?: boolean;
  title?: string;
}) {
  return (
    <div
      title={title}
      className="rounded-md border border-vscode-border bg-vscode-widget-bg p-3"
      style={{ borderLeft: `3px solid ${TONE_COLOR[tone]}` }}
    >
      <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-wide text-vscode-desc">
        <span
          aria-hidden
          className="inline-block h-1.5 w-1.5 rounded-full"
          style={{ backgroundColor: TONE_COLOR[tone] }}
        />
        {label}
      </div>
      <div
        className={
          "mt-1 break-words font-semibold text-vscode-fg " +
          (hero ? "text-2xl leading-tight" : "text-sm")
        }
      >
        {value}
      </div>
      {sub != null && sub !== "" && (
        <div className="mt-0.5 text-xs text-vscode-desc">{sub}</div>
      )}
    </div>
  );
}

/** A small theme-aware test-status pill (or an em dash when untested). */
export function StatusBadge({ status }: { status: ColumnTestStatus }) {
  if (status === "none") return <span className="text-vscode-desc">—</span>;
  return (
    <span
      className="rounded px-1.5 py-0.5 text-[11px] font-medium"
      style={{
        backgroundColor: STATUS_COLOR[status],
        color: "var(--vscode-editor-background)",
      }}
    >
      {status}
    </span>
  );
}
