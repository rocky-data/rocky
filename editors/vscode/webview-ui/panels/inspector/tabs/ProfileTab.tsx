import type { ProfileOutput } from "../../../../src/types/generated/profile";

export function ProfileTab({ profile }: { profile: ProfileOutput | null }) {
  if (!profile) {
    return (
      <p className="text-vscode-desc">Profiling… (one query per column)</p>
    );
  }
  if (profile.unavailable) {
    return (
      <div className="text-sm">
        <p className="font-semibold text-vscode-error">Profile unavailable.</p>
        <p className="mt-1 text-vscode-desc">{profile.unavailable}</p>
      </div>
    );
  }
  if (profile.columns.length === 0) {
    return <p className="text-vscode-desc">No columns to profile.</p>;
  }
  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="text-left text-vscode-desc">
          {["Column", "Type", "Rows", "Nulls", "Distinct", "Min", "Max"].map(
            (h) => (
              <th
                key={h}
                className="border-b border-vscode-border py-1 pr-4 font-medium"
              >
                {h}
              </th>
            ),
          )}
        </tr>
      </thead>
      <tbody>
        {profile.columns.map((col) => (
          <tr key={col.name}>
            <td className="border-b border-vscode-border py-1 pr-4 font-mono text-vscode-fg">
              {col.name}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
              {col.type}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-fg">
              {col.rows.toLocaleString()}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
              {col.nulls} ({(col.null_rate * 100).toFixed(1)}%)
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-fg">
              {col.distinct.toLocaleString()}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
              {col.min ?? "—"}
            </td>
            <td className="border-b border-vscode-border py-1 text-vscode-desc">
              {col.max ?? "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
