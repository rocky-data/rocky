import { useEffect, useMemo, useState } from "react";
import type {
  InspectorModelData,
  InspectorTestsData,
  ModelParam,
} from "../../../../src/webviews/inspector/contract";
import type {
  ProfileColumnStats,
  ProfileOutput,
} from "../../../../src/types/generated/profile";
import { getRpc } from "../../../runtime/rpcClient";
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
  const base = "rounded-sm px-2 py-0.5 ";
  if (!enabled) return base + "text-vscode-desc opacity-40";
  return (
    base +
    (active
      ? "bg-vscode-button-bg text-vscode-button-fg"
      : "text-vscode-desc hover:text-vscode-fg")
  );
}

/** Column-lineage confidence → a chart colour for the source dot. */
const CONFIDENCE_COLOR: Record<string, string> = {
  High: "var(--vscode-charts-green)",
  Medium: "var(--vscode-charts-yellow)",
  Low: "var(--vscode-charts-red)",
};

interface ColumnSource {
  source: string;
  transform: string;
  confidence: string;
}

export function ColumnsTab({
  data,
  tests,
}: {
  data: InspectorModelData;
  tests: InspectorTestsData | null;
}) {
  const [sort, setSort] = useState<SortMode>("az");
  const [profile, setProfile] = useState<Map<string, ProfileColumnStats> | null>(
    null,
  );

  // Profile this model's columns (distinct / null-rate) inline — the same
  // DuckDB aggregate the Profile tab uses. DuckDB-only; degrades to "—".
  useEffect(() => {
    let active = true;
    void getRpc()
      .request<ProfileOutput>("profile", {
        model: data.modelName,
      } satisfies ModelParam)
      .then((p) => {
        if (!active) return;
        setProfile(
          p.unavailable ? new Map() : new Map(p.columns.map((c) => [c.name, c])),
        );
      })
      .catch(() => active && setProfile(new Map()));
    return () => {
      active = false;
    };
  }, [data.modelName]);

  // Per-column upstream lineage (confidence + transform + source), from the
  // catalog's column edges into this model.
  const lineageByColumn = useMemo(() => {
    const m = new Map<string, ColumnSource[]>();
    for (const e of data.columnEdges) {
      if (e.target_model !== data.modelName) continue;
      const arr = m.get(e.target_column) ?? [];
      arr.push({
        source: `${e.source_model}.${e.source_column}`,
        transform: e.transform,
        confidence: e.confidence,
      });
      m.set(e.target_column, arr);
    }
    return m;
  }, [data.columnEdges, data.modelName]);

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

  const numCell = "border-b border-vscode-border py-1 pr-4 text-right tabular-nums text-vscode-desc";

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
              Null
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 font-medium">
              Lineage
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 text-right font-medium">
              Distinct
            </th>
            <th className="border-b border-vscode-border py-1 pr-4 text-right font-medium">
              Null %
            </th>
            <th className="border-b border-vscode-border py-1 font-medium">
              Tests
            </th>
          </tr>
        </thead>
        <tbody>
          {columns.map((col) => {
            const edges = lineageByColumn.get(col.name) ?? [];
            const primary = edges[0];
            const stats = profile?.get(col.name);
            return (
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
                <td className="border-b border-vscode-border py-1 pr-4">
                  {primary ? (
                    <span
                      className="inline-flex items-center gap-1.5 text-vscode-desc"
                      title={edges
                        .map(
                          (e) =>
                            `← ${e.source} · ${e.transform} · ${e.confidence} confidence`,
                        )
                        .join("\n")}
                    >
                      <span
                        aria-hidden
                        className="inline-block h-1.5 w-1.5 rounded-full"
                        style={{
                          backgroundColor:
                            CONFIDENCE_COLOR[primary.confidence] ??
                            "var(--vscode-descriptionForeground)",
                        }}
                      />
                      {primary.transform}
                    </span>
                  ) : (
                    <span className="text-vscode-desc">—</span>
                  )}
                </td>
                <td className={numCell}>
                  {stats ? stats.distinct : profile ? "—" : "…"}
                </td>
                <td className={numCell}>
                  {stats ? `${Math.round(stats.null_rate * 100)}%` : profile ? "—" : "…"}
                </td>
                <td className="border-b border-vscode-border py-1">
                  <StatusBadge status={statusByColumn.get(col.name) ?? "none"} />
                </td>
              </tr>
            );
          })}
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
