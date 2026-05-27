import type { InspectorPreviewData } from "../../../../src/webviews/inspector/contract";
import { EmptyState, TableSkeleton } from "../components";

/** Coerce a JSON cell value to a display string (mirrors the query-results grid). */
function coerce(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function PreviewTab({
  preview,
}: {
  preview: InspectorPreviewData | null;
}) {
  if (!preview) {
    return <TableSkeleton rows={6} />;
  }
  if (preview.unavailable || !preview.preview) {
    return (
      <EmptyState
        tone="error"
        title="Preview unavailable"
        hint={preview.unavailable ?? "No rows returned."}
      />
    );
  }
  const rows = preview.preview;
  return (
    <div>
      <div className="mb-2 text-xs text-vscode-desc">
        {rows.row_count}
        {rows.truncated ? "+" : ""} row{rows.row_count === 1 ? "" : "s"} ·{" "}
        {rows.adapter_kind}
      </div>
      <div className="overflow-auto">
        <table className="border-collapse text-sm">
          <thead>
            <tr className="text-left text-vscode-desc">
              {rows.columns.map((col) => (
                <th
                  key={col}
                  className="border-b border-vscode-border px-2 py-1 font-mono font-medium"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.rows.map((row, i) => (
              <tr
                key={i}
                style={
                  i % 2 ? { backgroundColor: "rgba(127,127,127,0.05)" } : undefined
                }
              >
                {row.map((cell, j) => {
                  const isNull = cell === null || cell === undefined;
                  return (
                    <td
                      key={j}
                      className={`whitespace-pre border-b border-vscode-border px-2 py-1 text-vscode-fg ${
                        typeof cell === "number" ? "text-right tabular-nums" : ""
                      }`}
                    >
                      {isNull ? (
                        <span className="italic text-vscode-desc opacity-60">
                          NULL
                        </span>
                      ) : (
                        coerce(cell)
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
