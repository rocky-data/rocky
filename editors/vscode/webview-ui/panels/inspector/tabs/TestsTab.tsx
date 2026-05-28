import { useMemo } from "react";
import type { InspectorTestsData } from "../../../../src/webviews/inspector/contract";
import type { DeclarativeTestResult } from "../../../../src/types/generated/test";
import { EmptyState, StatusBadge, TableSkeleton } from "../components";
import { type ColumnTestStatus, statusRank, toStatus } from "../viewModel";

const SUMMARY = [
  { key: "fail", label: "failed", color: "var(--vscode-testing-iconFailed)" },
  { key: "warn", label: "warning", color: "var(--vscode-testing-iconQueued)" },
  { key: "pass", label: "passed", color: "var(--vscode-testing-iconPassed)" },
] as const;

export function TestsTab({ tests }: { tests: InspectorTestsData | null }) {
  const rows = useMemo(
    () =>
      (tests?.results ?? []).map((r) => ({
        r,
        status: toStatus(r.status, r.severity),
      })),
    [tests],
  );
  // Failed first, then by column — surface what needs attention.
  const sorted = useMemo(
    () =>
      [...rows].sort(
        (a, b) =>
          statusRank(b.status) - statusRank(a.status) ||
          (a.r.column ?? "").localeCompare(b.r.column ?? ""),
      ),
    [rows],
  );
  const counts = useMemo(() => {
    const c: Record<string, number> = { pass: 0, warn: 0, fail: 0 };
    for (const { status } of rows) if (status in c) c[status] += 1;
    return c;
  }, [rows]);

  // The model-execution check (does the model compile + materialize via
  // DuckDB) is surfaced as a banner above the declarative assertions, so a
  // model that passes `rocky test` but declares no `[[tests]]` still shows
  // green instead of an empty state.
  const exec = tests?.modelExecution;
  const execStatus = exec ? toStatus(exec.status, "error") : "none";

  if (!tests) {
    return <TableSkeleton rows={4} />;
  }
  if (tests.unavailable) {
    return (
      <EmptyState tone="error" title="Tests unavailable" hint={tests.unavailable} />
    );
  }
  if (tests.results.length === 0 && !exec) {
    return (
      <EmptyState
        title="No tests"
        hint="This model declares no tests, and its execution status is unavailable."
      />
    );
  }

  return (
    <div>
      {exec && (
        <div className="mb-3 flex items-center gap-2 rounded border border-vscode-border px-3 py-2 text-sm">
          <StatusBadge status={execStatus} />
          <span className="text-vscode-fg">Model executes</span>
          <span className="text-vscode-desc">
            {exec.status === "pass"
              ? "compiles and materializes against local DuckDB"
              : (exec.error ?? "execution failed")}
          </span>
        </div>
      )}
      {tests.results.length === 0 ? (
        <EmptyState
          title="No declarative tests"
          hint="This model declares no [[tests]] in its contract."
        />
      ) : (
        <DeclarativeTests sorted={sorted} counts={counts} rowCount={rows.length} />
      )}
    </div>
  );
}

/** The declarative `[[tests]]` assertions table. Split out so the Model
 *  execution banner can render above it without nesting the table logic. */
function DeclarativeTests({
  sorted,
  counts,
  rowCount,
}: {
  sorted: { r: DeclarativeTestResult; status: ColumnTestStatus }[];
  counts: Record<string, number>;
  rowCount: number;
}) {
  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center gap-3 text-xs text-vscode-desc">
        {SUMMARY.filter((s) => counts[s.key] > 0).map((s) => (
          <span key={s.key} className="inline-flex items-center gap-1.5">
            <span
              aria-hidden
              className="inline-block h-2 w-2 rounded-full"
              style={{ backgroundColor: s.color }}
            />
            {counts[s.key]} {s.label}
          </span>
        ))}
        <span className="flex-1" />
        <span>
          {rowCount} test{rowCount === 1 ? "" : "s"}
        </span>
      </div>
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="text-left text-vscode-desc">
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Test
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Column
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Status
            </th>
            <th className="border-b border-vscode-border py-1 font-medium">
              Detail
            </th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(({ r, status }, i) => (
            <tr key={i}>
              <td className="border-b border-vscode-border py-1 pr-4 font-mono text-vscode-fg">
                {r.test_type}
              </td>
              <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
                {r.column ?? "—"}
              </td>
              <td className="border-b border-vscode-border py-1 pr-4">
                <StatusBadge status={status} />
              </td>
              <td className="border-b border-vscode-border py-1 text-vscode-desc">
                {r.detail ?? ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
