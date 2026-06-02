import type { ProfileOutput } from "../../../../src/types/generated/profile";
import { EmptyState, TableSkeleton } from "../components";

/** Null-rate as a colored bar — green clean, amber some, red mostly-null. */
function NullBar({ rate }: { rate: number }) {
  const pct = rate * 100;
  const tone =
    rate === 0
      ? "var(--vscode-charts-green)"
      : rate > 0.5
        ? "var(--vscode-charts-red)"
        : "var(--vscode-charts-yellow)";
  return (
    <span className="inline-flex items-center gap-2">
      <span
        className="relative inline-block h-1.5 w-14 shrink-0 rounded-full"
        style={{ backgroundColor: "rgba(127,127,127,0.25)" }}
      >
        <span
          className="absolute inset-y-0 left-0 rounded-full"
          style={{
            width: `${pct}%`,
            minWidth: rate > 0 ? "0.25rem" : 0,
            backgroundColor: tone,
          }}
        />
      </span>
      <span className="tabular-nums text-vscode-desc">{pct.toFixed(1)}%</span>
    </span>
  );
}

const num = "border-b border-vscode-border py-1 pr-4 text-right tabular-nums";

export function ProfileTab({ profile }: { profile: ProfileOutput | null }) {
  if (!profile) {
    return <TableSkeleton rows={5} />;
  }
  if (profile.unavailable) {
    return (
      <EmptyState
        tone="error"
        title="Profile unavailable"
        hint={profile.unavailable}
      />
    );
  }
  if (profile.columns.length === 0) {
    return <EmptyState title="No columns to profile" />;
  }
  // Source-fallback: the model's target isn't materialized, so these stats
  // come from the upstream source (`profiled_table`), not the model output.
  // Label it so the numbers aren't mistaken for the materialized model.
  const fallback =
    profile.fell_back_from != null ? (
      <p className="mb-3 rounded-sm border border-vscode-border px-3 py-2 text-xs text-vscode-desc">
        Showing source{" "}
        <span className="font-mono text-vscode-fg">{profile.profiled_table}</span> —
        the model target{" "}
        <span className="font-mono text-vscode-fg">{profile.fell_back_from}</span> isn't
        materialized yet. Run <span className="font-mono">rocky run</span> to profile the
        model's own output.
      </p>
    ) : null;
  return (
    <div>
      {fallback}
      <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="text-vscode-desc">
          <th className="border-b border-vscode-border py-1 pr-4 text-left font-medium">
            Column
          </th>
          <th className="border-b border-vscode-border py-1 pr-4 text-left font-medium">
            Type
          </th>
          <th className="border-b border-vscode-border py-1 pr-4 text-right font-medium">
            Rows
          </th>
          <th className="border-b border-vscode-border py-1 pr-4 text-left font-medium">
            Null rate
          </th>
          <th className="border-b border-vscode-border py-1 pr-4 text-right font-medium">
            Distinct
          </th>
          <th className="border-b border-vscode-border py-1 pr-4 text-left font-medium">
            Min
          </th>
          <th className="border-b border-vscode-border py-1 text-left font-medium">
            Max
          </th>
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
            <td className={`${num} text-vscode-fg`}>
              {col.rows.toLocaleString()}
            </td>
            <td
              className="border-b border-vscode-border py-1 pr-4"
              title={`${col.nulls.toLocaleString()} null${col.nulls === 1 ? "" : "s"}`}
            >
              <NullBar rate={col.null_rate} />
            </td>
            <td className={`${num} text-vscode-fg`}>
              {col.distinct.toLocaleString()}
              {col.distinct === col.rows && col.rows > 0 && (
                <span
                  className="ml-1.5 align-middle text-[10px] uppercase tracking-wide"
                  style={{ color: "var(--vscode-charts-blue)" }}
                  title="Every value is distinct — a candidate key"
                >
                  unique
                </span>
              )}
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
    </div>
  );
}
