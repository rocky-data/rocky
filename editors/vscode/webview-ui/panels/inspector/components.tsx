import type { ColumnTestStatus } from "./viewModel";

const STATUS_COLOR: Record<Exclude<ColumnTestStatus, "none">, string> = {
  pass: "var(--vscode-testing-iconPassed)",
  warn: "var(--vscode-testing-iconQueued)",
  fail: "var(--vscode-testing-iconFailed)",
};

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
