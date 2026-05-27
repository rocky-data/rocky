import type { InspectorPreviewData } from "../../../../src/webviews/inspector/contract";

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
    return <p className="text-vscode-desc">Loading preview…</p>;
  }
  if (preview.unavailable || !preview.preview) {
    return (
      <div className="text-sm">
        <p className="font-semibold text-vscode-error">Preview unavailable.</p>
        <p className="mt-1 text-vscode-desc">
          {preview.unavailable ?? "No rows returned."}
        </p>
      </div>
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
              <tr key={i}>
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className="whitespace-pre border-b border-vscode-border px-2 py-1 text-vscode-fg"
                  >
                    {coerce(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
