import type { InspectorTestsData } from "../../../../src/webviews/inspector/contract";
import { StatusBadge } from "../components";
import { toStatus } from "../viewModel";

export function TestsTab({ tests }: { tests: InspectorTestsData | null }) {
  if (!tests) {
    return <p className="text-vscode-desc">Running tests…</p>;
  }
  if (tests.unavailable) {
    return (
      <div className="text-sm">
        <p className="font-semibold text-vscode-error">Tests unavailable.</p>
        <p className="mt-1 text-vscode-desc">{tests.unavailable}</p>
      </div>
    );
  }
  if (tests.results.length === 0) {
    return (
      <p className="text-vscode-desc">No declarative tests for this model.</p>
    );
  }
  return (
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
        {tests.results.map((result, i) => (
          <tr key={i}>
            <td className="border-b border-vscode-border py-1 pr-4 font-mono text-vscode-fg">
              {result.test_type}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4 text-vscode-desc">
              {result.column ?? "—"}
            </td>
            <td className="border-b border-vscode-border py-1 pr-4">
              <StatusBadge status={toStatus(result.status, result.severity)} />
            </td>
            <td className="border-b border-vscode-border py-1 text-vscode-desc">
              {result.detail ?? ""}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
