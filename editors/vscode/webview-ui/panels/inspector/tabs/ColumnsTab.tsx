import { useMemo, useState } from "react";
import type {
  InspectorModelData,
  InspectorTestsData,
} from "../../../../src/webviews/inspector/contract";
import { StatusBadge } from "../components";
import {
  statusRank,
  testStatusByColumn,
  type ColumnTestStatus,
} from "../viewModel";

type SortMode = "az" | "tests";

function nullability(nullable: boolean | null | undefined): string {
  if (nullable === false) return "not null";
  if (nullable === true) return "nullable";
  return "—";
}

function sortButton(active: boolean, enabled = true): string {
  const base = "rounded px-2 py-0.5 ";
  if (!enabled) return base + "text-vscode-desc opacity-40";
  return (
    base +
    (active
      ? "bg-vscode-button-bg text-vscode-button-fg"
      : "text-vscode-desc hover:text-vscode-fg")
  );
}

export function ColumnsTab({
  data,
  tests,
}: {
  data: InspectorModelData;
  tests: InspectorTestsData | null;
}) {
  const [sort, setSort] = useState<SortMode>("az");

  const statusByColumn = useMemo<Map<string, ColumnTestStatus>>(
    () => (tests ? testStatusByColumn(tests.results) : new Map()),
    [tests],
  );
  const hasTests = statusByColumn.size > 0;

  const columns = useMemo(() => {
    const cols = [...data.columns];
    if (sort === "tests" && hasTests) {
      cols.sort(
        (a, b) =>
          statusRank(statusByColumn.get(b.name)) -
            statusRank(statusByColumn.get(a.name)) ||
          a.name.localeCompare(b.name),
      );
    } else {
      cols.sort((a, b) => a.name.localeCompare(b.name));
    }
    return cols;
  }, [data.columns, sort, hasTests, statusByColumn]);

  return (
    <div>
      <div className="mb-2 flex items-center gap-2 text-xs text-vscode-desc">
        <span>
          {data.columns.length} column{data.columns.length === 1 ? "" : "s"}
        </span>
        <span className="flex-1" />
        <span>Sort</span>
        <button
          type="button"
          onClick={() => setSort("az")}
          className={sortButton(sort === "az")}
        >
          A–Z
        </button>
        <button
          type="button"
          disabled={!hasTests}
          onClick={() => setSort("tests")}
          className={sortButton(sort === "tests", hasTests)}
        >
          Tests
        </button>
      </div>
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="text-left text-vscode-desc">
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Column
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Type
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Nullability
            </th>
            <th className="border-b border-vscode-border py-1 font-medium">
              Tests
            </th>
          </tr>
        </thead>
        <tbody>
          {columns.map((col) => (
            <tr key={col.name}>
              <td className="border-b border-vscode-border py-1 pr-4 font-mono text-vscode-fg">
                {col.name}
              </td>
              <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
                {col.data_type ?? "—"}
              </td>
              <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
                {nullability(col.nullable)}
              </td>
              <td className="border-b border-vscode-border py-1">
                <StatusBadge status={statusByColumn.get(col.name) ?? "none"} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {!tests && (
        <p className="mt-3 text-xs text-vscode-desc">
          Open the Tests tab to annotate columns with their test results.
        </p>
      )}
    </div>
  );
}
